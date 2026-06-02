package webhooks

import (
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every webhook request attempt.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.maxInterval",
	time.Minute*5,
	`The maximum backoff interval between every webhook request attempt.`,
)

var RequestTimeoutSetting = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.request.timeout",
	time.Second*30,
	`Timeout for executing a single webhook HTTP request.`,
)

var MaxAttemptsSetting = dynamicconfig.NewGlobalIntSetting(
	"component.webhooks.maxAttempts",
	5,
	`Maximum number of attempts for a webhook delivery.`,
)

type Config struct {
	RetryPolicy      func() backoff.RetryPolicy
	RequestTimeout   dynamicconfig.DurationPropertyFn
	MaxAttempts      dynamicconfig.IntPropertyFn
}

func ConfigProvider(dc *dynamicconfig.Collection) *Config {
	return &Config{
		RetryPolicy: func() backoff.RetryPolicy {
			return backoff.NewExponentialRetryPolicy(
				RetryPolicyInitialInterval.Get(dc)(),
			).WithMaximumInterval(
				RetryPolicyMaximumInterval.Get(dc)(),
			).WithExpirationInterval(
				backoff.NoInterval,
			)
		},
		RequestTimeout: RequestTimeoutSetting.Get(dc),
		MaxAttempts:    MaxAttemptsSetting.Get(dc),
	}
}
