package webhooks

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"go.temporal.io/server/common/log"
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
		exec.executeWebhookTask,
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
	HTTPClient        *http.Client
}

type taskExecutor struct {
	TaskExecutorOptions
}

func (e taskExecutor) executeWebhookTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task WebhookTask,
) error {
	var webhook *Webhook
	if err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		var err error
		webhook, err = hsm.MachineData[*Webhook](node)
		return err
	}); err != nil {
		return fmt.Errorf("failed to load webhook state: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, webhook.Method, webhook.URL, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	if webhook.Body != nil {
		req.Body = io.NopCloser(newBytesReader(webhook.Body))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(newBytesReader(webhook.Body)), nil
		}
		req.ContentLength = int64(len(webhook.Body))
	}
	for k, v := range webhook.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.HTTPClient.Do(req)
	if err != nil {
		return e.saveResult(ctx, env, ref, invocationResultRetry{err})
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return e.saveResult(ctx, env, ref, invocationResultOK{
			responseBody: body,
			statusCode:   int32(resp.StatusCode),
		})
	}

	if isRetryableHTTPStatus(resp.StatusCode) && int(webhook.Attempt) < e.Config.MaxAttempts()-1 {
		return e.saveResult(ctx, env, ref, invocationResultRetry{
			fmt.Errorf("webhook received retryable status %d", resp.StatusCode),
		})
	}

	return e.saveResult(ctx, env, ref, invocationResultFail{
		fmt.Errorf("webhook received non-retryable status %d", resp.StatusCode),
	})
}

func (e taskExecutor) executeBackoffTask(
	env hsm.Environment,
	node *hsm.Node,
	task BackoffTask,
) error {
	return hsm.MachineTransition(node, func(webhook *Webhook) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(webhook, EventRescheduled{})
	})
}

type invocationResult interface {
	mustImplementInvocationResult()
	error() error
}

type invocationResultOK struct {
	responseBody []byte
	statusCode   int32
}

func (invocationResultOK) mustImplementInvocationResult() {}

func (invocationResultOK) error() error { return nil }

type invocationResultFail struct {
	err error
}

func (invocationResultFail) mustImplementInvocationResult() {}

func (r invocationResultFail) error() error { return r.err }

type invocationResultRetry struct {
	err error
}

func (invocationResultRetry) mustImplementInvocationResult() {}

func (r invocationResultRetry) error() error { return r.err }

func (e taskExecutor) saveResult(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	result invocationResult,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(webhook *Webhook) (hsm.TransitionOutput, error) {
			switch r := result.(type) {
			case invocationResultOK:
				return TransitionCompleted.Apply(webhook, EventCompleted{
					Time:         env.Now(),
					ResponseBody: r.responseBody,
					StatusCode:   r.statusCode,
				})
			case invocationResultRetry:
				return TransitionAttemptFailed.Apply(webhook, EventAttemptFailed{
					Time:        env.Now(),
					Err:         r.error(),
					RetryPolicy: e.Config.RetryPolicy(),
				})
			case invocationResultFail:
				return TransitionFailed.Apply(webhook, EventFailed{
					Time: env.Now(),
					Err:  r.error(),
				})
			default:
				return hsm.TransitionOutput{}, queueserrors.NewUnprocessableTaskError(
					fmt.Sprintf("unrecognized webhook result %v", result),
				)
			}
		})
	})
}

func isRetryableHTTPStatus(statusCode int) bool {
	switch {
	case statusCode == http.StatusRequestTimeout:
		return true
	case statusCode == http.StatusTooManyRequests:
		return true
	case statusCode == http.StatusBadGateway:
		return true
	case statusCode == http.StatusServiceUnavailable:
		return true
	case statusCode == http.StatusGatewayTimeout:
		return true
	case statusCode >= 500:
		return true
	default:
		return false
	}
}

type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return
}
