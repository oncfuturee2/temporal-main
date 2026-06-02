package webhooks

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
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

type HTTPCallerProvider func(destination string) HTTPCaller

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

	Config             *Config
	NamespaceRegistry  namespace.Registry
	MetricsHandler     metrics.Handler
	Logger             log.Logger
	HTTPCallerProvider HTTPCallerProvider
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
func (invocationResultOK) error() error                   { return nil }

type invocationResultFail struct {
	err error
}

func (invocationResultFail) mustImplementInvocationResult() {}
func (r invocationResultFail) error() error                 { return r.err }

type invocationResultRetry struct {
	err error
}

func (invocationResultRetry) mustImplementInvocationResult() {}
func (r invocationResultRetry) error() error                 { return r.err }

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

	webhook, err := e.loadWebhook(ctx, env, ref)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(
		ctx,
		e.Config.RequestTimeout(ns.Name().String(), task.Destination()),
	)
	defer cancel()

	result := e.invokeWebhook(callCtx, webhook, task)
	return e.saveResult(ctx, env, ref, result)
}

func (e taskExecutor) loadWebhook(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
) (Webhook, error) {
	var webhook Webhook
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		var err error
		webhook, err = hsm.MachineData[Webhook](node)
		return err
	})
	return webhook, err
}

func (e taskExecutor) invokeWebhook(
	ctx context.Context,
	webhook Webhook,
	task InvocationTask,
) invocationResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook.URL, nil)
	if err != nil {
		return invocationResultFail{err: fmt.Errorf("failed to create HTTP request: %w", err)}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Request-ID", webhook.RequestID)
	req.Header.Set("X-Webhook-Attempt", fmt.Sprintf("%d", webhook.Attempt+1))

	caller := e.HTTPCallerProvider(task.Destination())

	resp, err := caller(req)
	if err != nil {
		e.Logger.Warn("webhook invocation network error",
			tag.String("url", webhook.URL),
			tag.NewInt32("attempt", webhook.Attempt+1),
			tag.Error(err),
		)
		return invocationResultRetry{err: fmt.Errorf("webhook invocation failed: %w", err)}
	}
	defer func() { _ = resp.Body.Close() }()

	statusCategory := resp.StatusCode / 100
	switch {
	case statusCategory == 2:
		return invocationResultOK{}
	case resp.StatusCode == http.StatusTooManyRequests ||
		resp.StatusCode >= 500:
		e.Logger.Warn("webhook invocation received retryable status",
			tag.String("url", webhook.URL),
			tag.NewInt32("attempt", webhook.Attempt+1),
			tag.NewInt("status", resp.StatusCode),
		)
		return invocationResultRetry{
			err: fmt.Errorf("webhook returned retryable status: %d", resp.StatusCode),
		}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		e.Logger.Error("webhook invocation received non-retryable status",
			tag.String("url", webhook.URL),
			tag.NewInt32("attempt", webhook.Attempt+1),
			tag.NewInt("status", resp.StatusCode),
		)
		return invocationResultFail{
			err: fmt.Errorf("webhook returned non-retryable status: %d", resp.StatusCode),
		}
	default:
		return invocationResultRetry{
			err: fmt.Errorf("webhook returned unexpected status: %d", resp.StatusCode),
		}
	}
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
				return hsm.TransitionOutput{}, queueserrors.NewUnprocessableTaskError(
					fmt.Sprintf("unrecognized webhook result %v", result),
				)
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

func MockHTTPCallerProvider() HTTPCallerProvider {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptStr := r.Header.Get("X-Webhook-Attempt")
		attempt := 1
		if attemptStr != "" {
			_, _ = fmt.Sscanf(attemptStr, "%d", &attempt)
		}

		switch {
		case attempt >= 3:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		case r.URL.Query().Get("fail_permanent") == "true":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad request"}`))
		default:
			delay := time.Duration(50+rand.Intn(100)) * time.Millisecond
			time.Sleep(delay)
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"temporary error"}`))
		}
	}))
	server.Start()

	return func(destination string) HTTPCaller {
		return func(r *http.Request) (*http.Response, error) {
			r.URL.Scheme = "http"
			r.URL.Host = server.Listener.Addr().String()
			return server.Client().Do(r)
		}
	}
}
