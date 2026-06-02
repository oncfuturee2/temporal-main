package webhooks

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
)

const (
	StateMachineType = "webhooks.Webhook"
)

type WebhookState int

const (
	WebhookStateUnspecified WebhookState = iota
	WebhookStateStandby
	WebhookStateScheduled
	WebhookStateBackingOff
	WebhookStateCompleted
	WebhookStateFailed
)

func MachineCollection(tree *hsm.Node) hsm.Collection[*Webhook] {
	return hsm.NewCollection[*Webhook](tree, StateMachineType)
}

type Webhook struct {
	CurrentState        WebhookState
	URL                 string
	Method              string
	Headers             map[string]string
	Body                []byte
	Attempt             int32
	MaxAttempts         int32
	NextAttemptSchedule time.Time
	LastAttemptFailure  string
	LastAttemptTime     time.Time
	ResponseBody        []byte
	StatusCode          int32
}

var _ hsm.StateMachine[WebhookState] = (*Webhook)(nil)

func NewWebhook(url, method string, headers map[string]string, body []byte, maxAttempts int32) *Webhook {
	return &Webhook{
		CurrentState: WebhookStateStandby,
		URL:          url,
		Method:       method,
		Headers:      headers,
		Body:         body,
		MaxAttempts:  maxAttempts,
	}
}

func (w *Webhook) State() WebhookState {
	return w.CurrentState
}

func (w *Webhook) SetState(state WebhookState) {
	w.CurrentState = state
}

func (w *Webhook) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch w.CurrentState {
	case WebhookStateBackingOff:
		return []hsm.Task{BackoffTask{deadline: w.NextAttemptSchedule}}, nil
	case WebhookStateScheduled:
		return []hsm.Task{WebhookTask{destination: w.URL}}, nil
	}
	return nil, nil
}

func (w *Webhook) output() (hsm.TransitionOutput, error) {
	tasks, err := w.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

func (w *Webhook) recordAttempt(ts time.Time) {
	w.Attempt++
	w.LastAttemptTime = ts
}

type stateMachineDefinition struct{}

var _ hsm.StateMachineDefinition = stateMachineDefinition{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	w := &Webhook{}
	if err := json.Unmarshal(d, w); err != nil {
		return nil, err
	}
	return w, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	w, ok := state.(*Webhook)
	if !ok {
		return nil, fmt.Errorf("invalid webhook provided: %v", state)
	}
	return json.Marshal(w)
}

func (stateMachineDefinition) CompareState(s1, s2 any) (int, error) {
	w1, ok := s1.(*Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected s1 to be *Webhook, got %T", hsm.ErrIncompatibleType, s1)
	}
	w2, ok := s2.(*Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected s2 to be *Webhook, got %T", hsm.ErrIncompatibleType, s2)
	}

	stage1, attempts1, err := w1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for s1: %w", err)
	}
	stage2, attempts2, err := w2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for s2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && w1.State() != w2.State() {
		return 0, serviceerror.NewInvalidArgumentf("cannot compare two distinct terminal states: %v, %v", w1.State(), w2.State())
	}
	return int(attempts1 - attempts2), nil
}

const terminalStage = 3

func (w *Webhook) progress() (int, int32, error) {
	switch w.State() {
	case WebhookStateUnspecified:
		return 0, 0, serviceerror.NewInvalidArgument("uninitialized webhook state")
	case WebhookStateStandby:
		return 1, 0, nil
	case WebhookStateBackingOff:
		return 2, w.Attempt * 2, nil
	case WebhookStateScheduled:
		return 2, w.Attempt*2 + 1, nil
	case WebhookStateCompleted, WebhookStateFailed:
		return terminalStage, 0, nil
	default:
		return 0, 0, serviceerror.NewInvalidArgument("unknown webhook state")
	}
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]WebhookState{WebhookStateStandby},
	WebhookStateScheduled,
	func(w *Webhook, _ EventScheduled) (hsm.TransitionOutput, error) {
		return w.output()
	},
)

type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]WebhookState{WebhookStateBackingOff},
	WebhookStateScheduled,
	func(w *Webhook, _ EventRescheduled) (hsm.TransitionOutput, error) {
		w.NextAttemptSchedule = time.Time{}
		return w.output()
	},
)

type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateBackingOff,
	func(w *Webhook, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(w.Attempt), event.Err)
		w.NextAttemptSchedule = event.Time.Add(nextDelay)
		w.LastAttemptFailure = event.Err.Error()
		return w.output()
	},
)

type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateFailed,
	func(w *Webhook, event EventFailed) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.LastAttemptFailure = event.Err.Error()
		return hsm.TransitionOutput{}, nil
	},
)

type EventCompleted struct {
	Time         time.Time
	ResponseBody []byte
	StatusCode   int32
}

var TransitionCompleted = hsm.NewTransition(
	[]WebhookState{WebhookStateScheduled},
	WebhookStateCompleted,
	func(w *Webhook, event EventCompleted) (hsm.TransitionOutput, error) {
		w.recordAttempt(event.Time)
		w.ResponseBody = event.ResponseBody
		w.StatusCode = event.StatusCode
		w.LastAttemptFailure = ""
		return hsm.TransitionOutput{}, nil
	},
)
