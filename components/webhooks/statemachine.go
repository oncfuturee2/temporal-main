package webhooks

import (
	"encoding/json"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	StateMachineType = "webhooks.Webhook"

	terminalStage = 3
)

type WebhookStatus int32

const (
	WebhookStatusStandby    WebhookStatus = 0
	WebhookStatusScheduled  WebhookStatus = 1
	WebhookStatusBackingOff WebhookStatus = 2
	WebhookStatusCompleted  WebhookStatus = 3
	WebhookStatusFailed     WebhookStatus = 4
)

func (s WebhookStatus) String() string {
	switch s {
	case WebhookStatusStandby:
		return "STANDBY"
	case WebhookStatusScheduled:
		return "SCHEDULED"
	case WebhookStatusBackingOff:
		return "BACKING_OFF"
	case WebhookStatusCompleted:
		return "COMPLETED"
	case WebhookStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

func MachineCollection(tree *hsm.Node) hsm.Collection[Webhook] {
	return hsm.NewCollection[Webhook](tree, StateMachineType)
}

type WebhookState struct {
	Status                  WebhookStatus          `json:"status"`
	Attempt                 int32                  `json:"attempt"`
	LastAttemptCompleteTime *timestamppb.Timestamp `json:"last_attempt_complete_time"`
	NextAttemptScheduleTime *timestamppb.Timestamp `json:"next_attempt_schedule_time"`
	LastAttemptFailure      *failurepb.Failure     `json:"last_attempt_failure"`
	URL                     string                 `json:"url"`
	RequestID               string                 `json:"request_id"`
	RegistrationTime        *timestamppb.Timestamp `json:"registration_time"`
}

type Webhook struct {
	*WebhookState
}

func NewWebhook(requestID string, registrationTime *timestamppb.Timestamp, url string) Webhook {
	return Webhook{
		&WebhookState{
			Status:           WebhookStatusStandby,
			URL:              url,
			RequestID:        requestID,
			RegistrationTime: registrationTime,
		},
	}
}

func (w Webhook) State() WebhookStatus {
	return w.Status
}

func (w Webhook) SetState(state WebhookStatus) {
	w.Status = state //nolint:revive
}

func (w Webhook) recordAttempt(ts time.Time) {
	w.Attempt++                                     //nolint:revive
	w.LastAttemptCompleteTime = timestamppb.New(ts) //nolint:revive
}

func (w Webhook) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch w.Status {
	case WebhookStatusBackingOff:
		return []hsm.Task{BackoffTask{deadline: w.NextAttemptScheduleTime.AsTime()}}, nil
	case WebhookStatusScheduled:
		return []hsm.Task{InvocationTask{destination: w.URL}}, nil
	default:
		return nil, nil
	}
}

func (w Webhook) output() (hsm.TransitionOutput, error) {
	tasks, err := w.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

func (w Webhook) progress() (int, int32, error) {
	switch w.State() {
	case WebhookStatusStandby:
		return 1, 0, nil
	case WebhookStatusBackingOff:
		return 2, w.Attempt * 2, nil
	case WebhookStatusScheduled:
		return 2, w.Attempt*2 + 1, nil
	case WebhookStatusCompleted, WebhookStatusFailed:
		return terminalStage, 0, nil
	default:
		return 0, 0, fmt.Errorf("unknown webhook status: %v", w.State())
	}
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	state := &WebhookState{}
	if err := json.Unmarshal(d, state); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return Webhook{state}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if w, ok := state.(Webhook); ok {
		return json.Marshal(w.WebhookState)
	}
	return nil, fmt.Errorf("invalid webhook provided: %v", state)
}

func (stateMachineDefinition) CompareState(state1, state2 any) (int, error) {
	w1, ok := state1.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	w2, ok := state2.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := w1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := w2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && w1.State() != w2.State() {
		return 0, fmt.Errorf("cannot compare two distinct terminal states: %v, %v", w1.State(), w2.State())
	}
	return int(attempts1 - attempts2), nil
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]WebhookStatus{WebhookStatusStandby},
	WebhookStatusScheduled,
	func(w Webhook, event EventScheduled) (hsm.TransitionOutput, error) {
		return w.output()
	},
)

type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]WebhookStatus{WebhookStatusBackingOff},
	WebhookStatusScheduled,
	func(w Webhook, event EventRescheduled) (hsm.TransitionOutput, error) {
		w.NextAttemptScheduleTime = nil
		return w.output()
	},
)

type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]WebhookStatus{WebhookStatusScheduled},
	WebhookStatusBackingOff,
	func(w Webhook, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(w.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		w.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		w.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		return w.output()
	},
)

type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = hsm.NewTransition(
	[]WebhookStatus{WebhookStatusScheduled},
	WebhookStatusFailed,
	func(w Webhook, event EventFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return w.output()
	},
)

type EventSucceeded struct {
	Time time.Time
}

var TransitionSucceeded = hsm.NewTransition(
	[]WebhookStatus{WebhookStatusScheduled},
	WebhookStatusCompleted,
	func(w Webhook, event EventSucceeded) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.LastAttemptFailure = nil
		return w.output()
	},
)
