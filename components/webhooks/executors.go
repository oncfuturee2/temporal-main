package webhooks

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/hsm"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

type HTTPCaller func(*http.Request) (*http.Response, error)

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
) error {
	exec := taskExecutor{executorOptions}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		exec.executeInvocationTask,
	); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(
		registry,
		exec.executeBackoffTask,
	)
}

type TaskExecutorOptions struct {
	fx.In

	Config            *Config
	NamespaceRegistry namespace.Registry
	MetricsHandler    metrics.Handler
	Logger            log.Logger
}

type taskExecutor struct {
	TaskExecutorOptions
}

type invocationResult interface {
	mustImplementInvocationResult()
	error() error
}

type invocationResultOK struct{}

func (invocationResultOK) mustImplementInvocationResult() {}

func (invocationResultOK) error() error {
	return nil
}

type invocationResultFail struct {
	err error
}

func (invocationResultFail) mustImplementInvocationResult() {}

func (r invocationResultFail) error() error {
	return r.err
}

type invocationResultRetry struct {
	err error
}

func (invocationResultRetry) mustImplementInvocationResult() {}

func (r invocationResultRetry) error() error {
	return r.err
}

func (e taskExecutor) executeInvocationTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task InvocationTask,
) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}

	invokable, err := e.loadInvocationArgs(ctx, env, ref)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.Destination()),
	)
	defer cancel()

	result := invokable.Invoke(callCtx, ns, e, task)
	saveErr := e.saveResult(ctx, env, ref, result)
	return invokable.WrapError(result, saveErr)
}

type webhookInvokable interface {
	Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult
	WrapError(result invocationResult, err error) error
}

type mockWebhookInvokable struct {
	webhook *WebhookRequest
	attempt int32
}

func (m mockWebhookInvokable) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	e.Logger.Info("Executing mock webhook invocation",
		tag.String("webhook-url", m.webhook.Url),
		tag.Int32("attempt", m.attempt),
	)

	select {
	case <-ctx.Done():
		return invocationResultRetry{err: ctx.Err()}
	case <-time.After(100 * time.Millisecond):
	}

	r := rand.Float32()
	switch {
	case r < 0.6:
		e.Logger.Info("Mock webhook invocation succeeded")
		return invocationResultOK{}
	case r < 0.85:
		err := fmt.Errorf("mock retryable error")
		e.Logger.Info("Mock webhook invocation failed, will retry", tag.Error(err))
		return invocationResultRetry{err: err}
	default:
		err := fmt.Errorf("mock non-retryable error")
		e.Logger.Info("Mock webhook invocation failed permanently", tag.Error(err))
		return invocationResultFail{err: err}
	}
}

func (m mockWebhookInvokable) WrapError(result invocationResult, err error) error {
	return err
}

func (e taskExecutor) loadInvocationArgs(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
) (invokable webhookInvokable, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		webhook, err := hsm.MachineData[Webhook](node)
		if err != nil {
			return err
		}

		invokable = mockWebhookInvokable{
			webhook: webhook.Webhook,
			attempt: webhook.Attempt,
		}
		return nil
	})
	return
}

func (e taskExecutor) saveResult(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	result invocationResult,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
			switch result.(type) {
			case invocationResultOK:
				return TransitionSucceeded.Apply(webhook, EventSucceeded{
					Time: env.Now(),
				})
			case invocationResultRetry:
				return TransitionAttemptFailed.Apply(webhook, EventAttemptFailed{
					Time:        env.Now(),
					Err:         result.error(),
					RetryPolicy: e.Config.RetryPolicy(),
				})
			case invocationResultFail:
				return TransitionFailed.Apply(webhook, EventFailed{
					Time: env.Now(),
					Err:  result.error(),
				})
			default:
				return hsm.TransitionOutput{}, queueserrors.NewUnprocessableTaskError(fmt.Sprintf("unrecognized webhook result %v", result))
			}
		})
	})
}

func (e taskExecutor) executeBackoffTask(
	env hsm.Environment,
	node *hsm.Node,
	task BackoffTask,
) error {
	return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(webhook, EventRescheduled{})
	})
}
