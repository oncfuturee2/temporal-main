package webhooks

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
)

const (
	StateMachineType = "webhooks.Webhook"
)

// Webhook state machine.
type Webhook struct {
	*WebhookInfo
}

// NewWebhook creates a new webhook in the STANDBY state from given params.
func NewWebhook(url string) Webhook {
	return Webhook{
		&WebhookInfo{
			State: WebhookStateStandby,
			URL:   url,
		},
	}
}

func (w Webhook) State() WebhookState {
	return w.WebhookInfo.State
}

func (w Webhook) SetState(state WebhookState) {
	w.WebhookInfo.State = state
}

func (w Webhook) recordAttempt(ts time.Time) {
	w.Attempt++
	t := ts
	w.LastAttemptCompleteTime = &t
}

func (w Webhook) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	switch w.State() {
	case WebhookStateBackingOff:
		var deadline time.Time
		if w.NextAttemptScheduleTime != nil {
			deadline = *w.NextAttemptScheduleTime
		} else {
			deadline = time.Now()
		}
		return []hsm.Task{BackoffTask{TaskDeadline: deadline, Attempt: w.Attempt}}, nil
	case WebhookStateScheduled:
		return []hsm.Task{InvocationTask{URL: w.URL, Attempt: w.Attempt}}, nil
	case WebhookStateUnspecified, WebhookStateStandby, WebhookStateFailed, WebhookStateCompleted:
		return nil, nil
	}
	return nil, nil
}

func (w Webhook) output() (hsm.TransitionOutput, error) {
	tasks, err := w.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	info := &WebhookInfo{}
	if err := json.Unmarshal(d, info); err != nil {
		return nil, err
	}
	return Webhook{info}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Webhook); ok {
		return json.Marshal(state.WebhookInfo)
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

	// Simple comparison based on attempt for mock
	return int(w1.Attempt - w2.Attempt), nil
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventScheduled is triggered when the webhook is meant to be scheduled for the first time.
type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]WebhookState{WebhookStateStandby},
	WebhookStateScheduled,
	func(w Webhook, event EventScheduled) (hsm.TransitionOutput, error) {
		return w.output()
	},
)

// EventRescheduled is triggered when the webhook is meant to be rescheduled after backing off.
type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]WebhookState{WebhookStateBackingOff},
	WebhookStateScheduled,
	func(w Webhook, event EventRescheduled) (hsm.TransitionOutput, error) {
		w.NextAttemptScheduleTime = nil
		return w.output()
	},
)

// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateBackingOff,
	func(w Webhook, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(w.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		w.NextAttemptScheduleTime = &nextAttemptScheduleTime
		w.LastAttemptFailure = event.Err.Error()
		return w.output()
	},
)

// EventFailed is triggered when an attempt is failed with a non retryable error.
type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateFailed,
	func(w Webhook, event EventFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.LastAttemptFailure = event.Err.Error()
		return w.output()
	},
)

// EventSucceeded is triggered when an attempt succeeds.
type EventSucceeded struct {
	Time time.Time
}

var TransitionSucceeded = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateCompleted,
	func(w Webhook, event EventSucceeded) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.LastAttemptFailure = ""
		return w.output()
	},
)
