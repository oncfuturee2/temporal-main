package callbacks

import (
	chasmcallbacks "go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/metrics"
)

var (
	RequestCounter              = chasmcallbacks.RequestCounter
	RequestLatencyHistogram     = chasmcallbacks.RequestLatencyHistogram
	CallbackMaxAttemptsExceeded = metrics.NewCounterDef(
		"callback_max_attempts_exceeded",
		metrics.WithDescription("The number of callbacks forced into terminal failure after exhausting the configured max attempts."),
	)
)
