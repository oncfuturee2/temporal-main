package webhooks

import (
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
)

var RequestTimeout = dynamicconfig.NewDestinationDurationSetting(
	"component.webhooks.request.timeout",
	time.Second*10,
	`RequestTimeout is the timeout for executing a single webhook request.`,
)

var RetryPolicyInitialInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.initialInterval",
	time.Second,
	`The initial backoff interval between every webhook request attempt for a given webhook.`,
)

var RetryPolicyMaximumInterval = dynamicconfig.NewGlobalDurationSetting(
	"component.webhooks.retryPolicy.maxInterval",
	time.Hour,
	`The maximum backoff interval between every webhook request attempt for a given webhook.`,
)

type Config struct {
	RequestTimeout dynamicconfig.DurationPropertyFnWithDestinationFilter
	RetryPolicy    func() backoff.RetryPolicy
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
	}
}
