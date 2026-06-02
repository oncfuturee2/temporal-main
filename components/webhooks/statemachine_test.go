package webhooks_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/components/webhooks"
	"go.temporal.io/server/service/history/hsm"
)

func TestValidTransitions(t *testing.T) {
	currentTime := time.Now().UTC()
	webhook := webhooks.NewWebhook(
		"webhook-id",
		webhooks.Request{URL: "https://example.com/webhook", Method: "POST", MaxAttempts: 3},
		currentTime,
		3,
	)

	out, err := webhooks.TransitionScheduled.Apply(webhook, webhooks.EventScheduled{})
	require.NoError(t, err)
	require.Equal(t, webhooks.StateScheduled, webhook.State())
	require.Len(t, out.Tasks, 1)
	require.Equal(t, webhooks.TaskTypeWebhook, out.Tasks[0].Type())
	require.Equal(t, "https://example.com", out.Tasks[0].(webhooks.WebhookTask).Destination())

	out, err = webhooks.TransitionAttemptFailed.Apply(webhook, webhooks.EventAttemptFailed{
		Time:        currentTime,
		Err:         errors.New("temporary failure"),
		RetryPolicy: backoff.NewExponentialRetryPolicy(time.Second),
		StatusCode:  503,
	})
	require.NoError(t, err)
	require.Equal(t, webhooks.StateBackingOff, webhook.State())
	require.Equal(t, int32(1), webhook.Attempt)
	require.Equal(t, 503, webhook.LastResponseStatusCode)
	require.Equal(t, "temporary failure", webhook.LastAttemptFailure.Message)
	require.NotNil(t, webhook.NextAttemptScheduleTime)
	require.Len(t, out.Tasks, 1)
	require.Equal(t, webhooks.TaskTypeBackoff, out.Tasks[0].Type())

	out, err = webhooks.TransitionRescheduled.Apply(webhook, webhooks.EventRescheduled{})
	require.NoError(t, err)
	require.Equal(t, webhooks.StateScheduled, webhook.State())
	require.Nil(t, webhook.NextAttemptScheduleTime)
	require.Len(t, out.Tasks, 1)
	require.Equal(t, webhooks.TaskTypeWebhook, out.Tasks[0].Type())

	out, err = webhooks.TransitionCompleted.Apply(webhook, webhooks.EventCompleted{Time: currentTime.Add(time.Second), StatusCode: 204})
	require.NoError(t, err)
	require.Equal(t, webhooks.StateCompleted, webhook.State())
	require.Equal(t, int32(2), webhook.Attempt)
	require.Equal(t, 204, webhook.LastResponseStatusCode)
	require.Nil(t, webhook.LastAttemptFailure)
	require.Empty(t, out.Tasks)
}

func TestCompareState(t *testing.T) {
	reg := hsm.NewRegistry()
	require.NoError(t, webhooks.RegisterStateMachine(reg))
	def, ok := reg.Machine(webhooks.StateMachineType)
	require.True(t, ok)

	older := webhooks.NewWebhook("a", webhooks.Request{URL: "https://example.com"}, time.Now(), 3)
	newer := webhooks.NewWebhook("b", webhooks.Request{URL: "https://example.com"}, time.Now(), 3)
	older.SetState(webhooks.StateStandby)
	newer.SetState(webhooks.StateScheduled)

	cmp, err := def.CompareState(older, newer)
	require.NoError(t, err)
	require.Negative(t, cmp)
}
