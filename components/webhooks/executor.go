package webhooks

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/fx"
)

type TaskExecutorOptions struct {
	fx.In

	// Optional: Any injected dependencies can go here, like HTTPCaller, Metrics, Logger
}

type taskExecutor struct {
	TaskExecutorOptions
	RetryPolicy backoff.RetryPolicy
}

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
) error {
	exec := taskExecutor{
		TaskExecutorOptions: executorOptions,
		RetryPolicy:         backoff.NewExponentialRetryPolicy(time.Second),
	}
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

func (e taskExecutor) executeInvocationTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task InvocationTask,
) error {
	// Simulate HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, task.URL, nil)
	if err != nil {
		return e.saveResult(ctx, env, ref, err, false)
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)

	if err != nil {
		// Network error -> retryable
		return e.saveResult(ctx, env, ref, err, true)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 500 {
		// Server error -> retryable
		return e.saveResult(ctx, env, ref, fmt.Errorf("server error: %d", resp.StatusCode), true)
	}
	if resp.StatusCode >= 400 {
		// Client error -> non-retryable
		return e.saveResult(ctx, env, ref, fmt.Errorf("client error: %d", resp.StatusCode), false)
	}

	// Success
	return e.saveResult(ctx, env, ref, nil, false)
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

func (e taskExecutor) saveResult(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	invocationErr error,
	retryable bool,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
			if invocationErr == nil {
				return TransitionSucceeded.Apply(webhook, EventSucceeded{
					Time: env.Now(),
				})
			}
			if retryable {
				return TransitionAttemptFailed.Apply(webhook, EventAttemptFailed{
					Time:        env.Now(),
					Err:         invocationErr,
					RetryPolicy: e.RetryPolicy,
				})
			}
			return TransitionFailed.Apply(webhook, EventFailed{
				Time: env.Now(),
				Err:  invocationErr,
			})
		})
	})
}
