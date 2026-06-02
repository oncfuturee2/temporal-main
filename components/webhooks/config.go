package webhooks

import (
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

var RequestTimeout = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.request.timeout",
	10*time.Second,
	"RequestTimeout is the timeout for executing a single webhook request.",
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.initialInterval",
	time.Second,
	"RetryPolicyInitialInterval is the initial backoff interval between webhook attempts.",
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.maxInterval",
	time.Minute,
	"RetryPolicyMaximumInterval is the maximum backoff interval between webhook attempts.",
)

var MaxAttempts = dynamicconfig.NewGlobalIntSetting(
	"component.webhooks.maxAttempts",
	3,
	"MaxAttempts is the maximum number of webhook delivery attempts before failing permanently.",
)

type Config struct {
	RequestTimeout func() time.Duration
	RetryPolicy    func() backoff.RetryPolicy
	MaxAttempts    func() int
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RequestTimeout: RequestTimeout.Get(dc),
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
		MaxAttempts: MaxAttempts.Get(dc),
	}
}
