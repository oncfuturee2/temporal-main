package callbacks

import (
	chasmcallbacks "go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/metrics"
)

var (
	RequestCounter          = chasmcallbacks.RequestCounter
	RequestLatencyHistogram = chasmcallbacks.RequestLatencyHistogram
	MaxAttemptsExceededCounter = metrics.NewCounterDef(
		"component_callback_max_attempts_exceeded",
		metrics.WithDescription("The number of times a component callback exceeded the maximum number of attempts and was marked as failed."),
	)
)
