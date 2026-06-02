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
		metrics.WithDescription("The number of times a callback reached its maximum retry attempts and was transitioned to the failed state."),
	)
)
