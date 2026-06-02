package webhooks_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/dummy"
	"go.temporal.io/server/components/webhooks"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
)

type fakeEnv struct {
	node *hsm.Node
}

func (e fakeEnv) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) error {
	return accessor(e.node)
}

func (fakeEnv) Now() time.Time {
	return time.Now().UTC()
}

var _ hsm.Environment = fakeEnv{}

func TestExecuteWebhookTaskCompletes(t *testing.T) {
	reg := newRegistry(t)
	node := newWebhookNode(t, reg, webhooks.Request{URL: "https://example.com/success"}, 3)
	env := fakeEnv{node: node}

	webhook, err := hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	tasks, err := webhook.RegenerateTasks(node)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	err = reg.ExecuteImmediateTask(context.Background(), env, hsm.Ref{}, tasks[0])
	require.NoError(t, err)

	webhook, err = hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	require.Equal(t, webhooks.StateCompleted, webhook.State())
	require.Equal(t, int32(1), webhook.Attempt)
	require.Equal(t, 200, webhook.LastResponseStatusCode)
}

func TestExecuteWebhookTaskRetriesThenFails(t *testing.T) {
	reg := newRegistry(t)
	node := newWebhookNode(t, reg, webhooks.Request{URL: "https://example.com/retry", MaxAttempts: 2}, 2)
	env := fakeEnv{node: node}

	webhook, err := hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	tasks, err := webhook.RegenerateTasks(node)
	require.NoError(t, err)
	firstTask := tasks[0]

	err = reg.ExecuteImmediateTask(context.Background(), env, hsm.Ref{}, firstTask)
	require.NoError(t, err)

	webhook, err = hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	require.Equal(t, webhooks.StateBackingOff, webhook.State())
	require.Equal(t, int32(1), webhook.Attempt)

	tasks, err = webhook.RegenerateTasks(node)
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	require.Equal(t, webhooks.TaskTypeBackoff, tasks[0].Type())

	err = reg.ExecuteTimerTask(env, node, tasks[0])
	require.NoError(t, err)

	webhook, err = hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	require.Equal(t, webhooks.StateScheduled, webhook.State())

	tasks, err = webhook.RegenerateTasks(node)
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	err = reg.ExecuteImmediateTask(context.Background(), env, hsm.Ref{}, tasks[0])
	require.NoError(t, err)

	webhook, err = hsm.MachineData[webhooks.Webhook](node)
	require.NoError(t, err)
	require.Equal(t, webhooks.StateFailed, webhook.State())
	require.Equal(t, int32(2), webhook.Attempt)
	require.Equal(t, 503, webhook.LastResponseStatusCode)
	require.NotNil(t, webhook.LastAttemptFailure)
	require.True(t, webhook.LastAttemptFailure.GetApplicationFailureInfo().NonRetryable)
}

func newRegistry(t *testing.T) *hsm.Registry {
	t.Helper()
	reg := hsm.NewRegistry()
	require.NoError(t, dummy.RegisterStateMachine(reg))
	require.NoError(t, webhooks.RegisterStateMachine(reg))
	require.NoError(t, webhooks.RegisterTaskSerializers(reg))
	require.NoError(t, webhooks.RegisterExecutor(reg, webhooks.TaskExecutorOptions{
		Config: &webhooks.Config{
			RequestTimeout: func() time.Duration { return time.Second },
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Millisecond).WithMaximumInterval(time.Second).WithExpirationInterval(backoff.NoInterval)
			},
			MaxAttempts: func() int { return 2 },
		},
		Logger:           log.NewNoopLogger(),
		MetricsHandler:   metrics.NoopMetricsHandler,
		HTTPDoerProvider: webhooks.HTTPDoerProviderProvider(),
	}))
	return reg
}

func newWebhookNode(t *testing.T, reg *hsm.Registry, request webhooks.Request, defaultMaxAttempts int32) *hsm.Node {
	t.Helper()
	root, err := hsm.NewRoot(reg, dummy.StateMachineType, dummy.NewDummy(), make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	require.NoError(t, err)
	node, err := webhooks.AddChild(root, "webhook-id", request, time.Now().UTC(), defaultMaxAttempts)
	require.NoError(t, err)
	require.NoError(t, webhooks.Schedule(node))
	return node
}
