package webhooks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/fx"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type HTTPDoerProvider func() HTTPDoer

type TaskExecutorOptions struct {
	fx.In

	Config           *Config
	Logger           log.Logger
	MetricsHandler   metrics.Handler
	HTTPDoerProvider HTTPDoerProvider
}

type taskExecutor struct {
	TaskExecutorOptions
}

type invocationResult interface {
	mustImplementInvocationResult()
	error() error
	statusCode() int
}

type invocationResultOK struct {
	code int
}

func (invocationResultOK) mustImplementInvocationResult() {}
func (r invocationResultOK) error() error                 { return nil }
func (r invocationResultOK) statusCode() int              { return r.code }

type invocationResultRetry struct {
	code int
	err  error
}

func (invocationResultRetry) mustImplementInvocationResult() {}
func (r invocationResultRetry) error() error                 { return r.err }
func (r invocationResultRetry) statusCode() int              { return r.code }

type invocationResultFail struct {
	code int
	err  error
}

func (invocationResultFail) mustImplementInvocationResult() {}
func (r invocationResultFail) error() error                 { return r.err }
func (r invocationResultFail) statusCode() int              { return r.code }

func RegisterExecutor(registry *hsm.Registry, options TaskExecutorOptions) error {
	exec := taskExecutor{TaskExecutorOptions: options}
	if err := hsm.RegisterImmediateExecutor(registry, exec.executeWebhookTask); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(registry, exec.executeBackoffTask)
}

func HTTPDoerProviderProvider() HTTPDoerProvider {
	client := mockHTTPDoer{}
	return func() HTTPDoer {
		return client
	}
}

func (e taskExecutor) executeWebhookTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task WebhookTask,
) error {
	webhook, err := e.loadWebhook(ctx, env, ref)
	if err != nil {
		return err
	}

	requestCtx, cancel := context.WithTimeout(ctx, e.requestTimeout())
	defer cancel()

	result := e.invoke(requestCtx, webhook)
	return e.saveResult(ctx, env, ref, result)
}

func (e taskExecutor) loadWebhook(ctx context.Context, env hsm.Environment, ref hsm.Ref) (Webhook, error) {
	var webhook Webhook
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		var err error
		webhook, err = hsm.MachineData[Webhook](node)
		return err
	})
	return webhook, err
}

func (e taskExecutor) invoke(ctx context.Context, webhook Webhook) invocationResult {
	req, err := http.NewRequestWithContext(ctx, webhook.Method, webhook.URL, strings.NewReader(webhook.Body))
	if err != nil {
		return invocationResultFail{err: err}
	}
	for k, v := range webhook.Headers {
		req.Header.Set(k, v)
	}

	doer := e.httpDoerProvider()()
	resp, err := doer.Do(req)
	if err != nil {
		if e.shouldRetry(webhook) {
			return invocationResultRetry{err: err}
		}
		return invocationResultFail{err: err}
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return invocationResultOK{code: resp.StatusCode}
	}

	err = fmt.Errorf("webhook responded with status %d", resp.StatusCode)
	if isRetryableStatus(resp.StatusCode) && e.shouldRetry(webhook) {
		return invocationResultRetry{code: resp.StatusCode, err: err}
	}
	return invocationResultFail{code: resp.StatusCode, err: err}
}

func (e taskExecutor) saveResult(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	result invocationResult,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
			switch result := result.(type) {
			case invocationResultOK:
				return TransitionCompleted.Apply(webhook, EventCompleted{
					Time:       env.Now(),
					StatusCode: result.statusCode(),
				})
			case invocationResultRetry:
				return TransitionAttemptFailed.Apply(webhook, EventAttemptFailed{
					Time:        env.Now(),
					Err:         result.error(),
					RetryPolicy: e.retryPolicy(),
					StatusCode:  result.statusCode(),
				})
			case invocationResultFail:
				return TransitionFailed.Apply(webhook, EventFailed{
					Time:       env.Now(),
					Err:        result.error(),
					StatusCode: result.statusCode(),
				})
			default:
				return hsm.TransitionOutput{}, fmt.Errorf("unrecognized webhook result %T", result)
			}
		})
	})
}

func (e taskExecutor) executeBackoffTask(env hsm.Environment, node *hsm.Node, task BackoffTask) error {
	return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
		return TransitionRescheduled.Apply(webhook, EventRescheduled{})
	})
}

func (e taskExecutor) shouldRetry(webhook Webhook) bool {
	maxAttempts := webhook.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = int32(e.maxAttempts())
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	return webhook.Attempt+1 < maxAttempts
}

func (e taskExecutor) requestTimeout() time.Duration {
	if e.Config == nil || e.Config.RequestTimeout == nil {
		return 10 * time.Second
	}
	return e.Config.RequestTimeout()
}

func (e taskExecutor) retryPolicy() backoff.RetryPolicy {
	if e.Config == nil || e.Config.RetryPolicy == nil {
		return backoff.NewExponentialRetryPolicy(time.Second).WithMaximumInterval(time.Minute).WithExpirationInterval(backoff.NoInterval)
	}
	return e.Config.RetryPolicy()
}

func (e taskExecutor) maxAttempts() int {
	if e.Config == nil || e.Config.MaxAttempts == nil {
		return 3
	}
	return e.Config.MaxAttempts()
}

func (e taskExecutor) httpDoerProvider() HTTPDoerProvider {
	if e.HTTPDoerProvider != nil {
		return e.HTTPDoerProvider
	}
	return HTTPDoerProviderProvider()
}

func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

type mockHTTPDoer struct{}

func (mockHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	if errMessage := req.Header.Get("X-Webhook-Mock-Error"); errMessage != "" {
		return nil, errors.New(errMessage)
	}

	statusCode := http.StatusOK
	if rawStatusCode := req.Header.Get("X-Webhook-Mock-Status"); rawStatusCode != "" {
		parsedStatusCode, err := strconv.Atoi(rawStatusCode)
		if err != nil {
			return nil, err
		}
		statusCode = parsedStatusCode
	} else {
		switch {
		case strings.Contains(req.URL.Path, "retry"):
			statusCode = http.StatusServiceUnavailable
		case strings.Contains(req.URL.Path, "fail"):
			statusCode = http.StatusBadRequest
		default:
		}
	}

	return &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d %s", statusCode, http.StatusText(statusCode)),
		Body:       io.NopCloser(strings.NewReader(http.StatusText(statusCode))),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}
