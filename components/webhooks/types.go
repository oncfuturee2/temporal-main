package webhooks

import (
	"time"
)

// WebhookState represents the state of a webhook state machine.
type WebhookState int32

const (
	WebhookStateUnspecified WebhookState = 0
	WebhookStateStandby     WebhookState = 1
	WebhookStateScheduled   WebhookState = 2
	WebhookStateBackingOff  WebhookState = 3
	WebhookStateFailed      WebhookState = 4
	WebhookStateCompleted   WebhookState = 5
)

// WebhookInfo represents the mocked protobuf persistence structure for a webhook.
type WebhookInfo struct {
	State                   WebhookState
	Attempt                 int32
	LastAttemptCompleteTime *time.Time
	NextAttemptScheduleTime *time.Time
	LastAttemptFailure      string
	URL                     string
}
