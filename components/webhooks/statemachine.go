package webhooks

import (
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
)

const (
	StateMachineType = "webhooks.Webhook"
	terminalStage    = 3
)

type State string

const (
	StateUnspecified State = "UNSPECIFIED"
	StateStandby     State = "STANDBY"
	StateScheduled   State = "SCHEDULED"
	StateBackingOff  State = "BACKING_OFF"
	StateCompleted   State = "COMPLETED"
	StateFailed      State = "FAILED"
)

type Request struct {
	URL         string            `json:"url"`
	Method      string            `json:"method,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Body        string            `json:"body,omitempty"`
	MaxAttempts int32             `json:"max_attempts,omitempty"`
}

type WebhookInfo struct {
	ID                      string             `json:"id"`
	URL                     string             `json:"url"`
	Method                  string             `json:"method"`
	Headers                 map[string]string  `json:"headers,omitempty"`
	Body                    string             `json:"body,omitempty"`
	State                   State              `json:"state"`
	Attempt                 int32              `json:"attempt"`
	MaxAttempts             int32              `json:"max_attempts"`
	LastResponseStatusCode  int                `json:"last_response_status_code,omitempty"`
	LastAttemptFailure      *failurepb.Failure `json:"-"`
	RegistrationTime        time.Time          `json:"registration_time"`
	LastAttemptCompleteTime *time.Time         `json:"last_attempt_complete_time,omitempty"`
	NextAttemptScheduleTime *time.Time         `json:"next_attempt_schedule_time,omitempty"`
}

type webhookInfoPersist struct {
	ID                      string            `json:"id"`
	URL                     string            `json:"url"`
	Method                  string            `json:"method"`
	Headers                 map[string]string `json:"headers,omitempty"`
	Body                    string            `json:"body,omitempty"`
	State                   State             `json:"state"`
	Attempt                 int32             `json:"attempt"`
	MaxAttempts             int32             `json:"max_attempts"`
	LastResponseStatusCode  int               `json:"last_response_status_code,omitempty"`
	LastAttemptFailure      string            `json:"last_attempt_failure,omitempty"`
	RegistrationTime        time.Time         `json:"registration_time"`
	LastAttemptCompleteTime *time.Time        `json:"last_attempt_complete_time,omitempty"`
	NextAttemptScheduleTime *time.Time        `json:"next_attempt_schedule_time,omitempty"`
}

type Webhook struct {
	*WebhookInfo
}

var _ hsm.StateMachine[State] = Webhook{}

func MachineCollection(tree *hsm.Node) hsm.Collection[Webhook] {
	return hsm.NewCollection[Webhook](tree, StateMachineType)
}

func NewWebhook(id string, request Request, registrationTime time.Time, defaultMaxAttempts int32) Webhook {
	method := request.Method
	if method == "" {
		method = "POST"
	}
	maxAttempts := request.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	return Webhook{
		WebhookInfo: &WebhookInfo{
			ID:               id,
			URL:              request.URL,
			Method:           method,
			Headers:          cloneHeaders(request.Headers),
			Body:             request.Body,
			State:            StateStandby,
			MaxAttempts:      maxAttempts,
			RegistrationTime: registrationTime.UTC(),
		},
	}
}

func AddChild(node *hsm.Node, id string, request Request, registrationTime time.Time, defaultMaxAttempts int32) (*hsm.Node, error) {
	return node.AddChild(hsm.Key{Type: StateMachineType, ID: id}, NewWebhook(id, request, registrationTime, defaultMaxAttempts))
}

func Schedule(node *hsm.Node) error {
	return hsm.MachineTransition(node, func(webhook Webhook) (hsm.TransitionOutput, error) {
		return TransitionScheduled.Apply(webhook, EventScheduled{})
	})
}

func (w Webhook) State() State {
	return w.WebhookInfo.State
}

func (w Webhook) SetState(state State) {
	w.WebhookInfo.State = state
}

func (w Webhook) recordAttempt(ts time.Time) {
	w.Attempt++
	utc := ts.UTC()
	w.LastAttemptCompleteTime = &utc
}

func (w Webhook) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch w.State() {
	case StateBackingOff:
		if w.NextAttemptScheduleTime == nil {
			return nil, serviceerror.NewInvalidArgument("backing off webhook is missing next attempt schedule time")
		}
		return []hsm.Task{BackoffTask{deadline: *w.NextAttemptScheduleTime}}, nil
	case StateScheduled:
		destination, err := destinationFromURL(w.URL)
		if err != nil {
			return nil, err
		}
		return []hsm.Task{WebhookTask{destination: destination}}, nil
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
	case StateUnspecified:
		return 0, 0, serviceerror.NewInvalidArgument("uninitialized webhook state")
	case StateStandby:
		return 1, 0, nil
	case StateBackingOff:
		return 2, w.Attempt * 2, nil
	case StateScheduled:
		return 2, w.Attempt*2 + 1, nil
	case StateCompleted, StateFailed:
		return terminalStage, 0, nil
	default:
		return 0, 0, serviceerror.NewInvalidArgument("unknown webhook state")
	}
}

type stateMachineDefinition struct{}

var _ hsm.StateMachineDefinition = stateMachineDefinition{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	var persisted webhookInfoPersist
	if err := json.Unmarshal(d, &persisted); err != nil {
		return nil, err
	}
	info := &WebhookInfo{
		ID:                      persisted.ID,
		URL:                     persisted.URL,
		Method:                  persisted.Method,
		Headers:                 cloneHeaders(persisted.Headers),
		Body:                    persisted.Body,
		State:                   persisted.State,
		Attempt:                 persisted.Attempt,
		MaxAttempts:             persisted.MaxAttempts,
		LastResponseStatusCode:  persisted.LastResponseStatusCode,
		RegistrationTime:        persisted.RegistrationTime,
		LastAttemptCompleteTime: persisted.LastAttemptCompleteTime,
		NextAttemptScheduleTime: persisted.NextAttemptScheduleTime,
	}
	if persisted.LastAttemptFailure != "" {
		info.LastAttemptFailure = newFailure(persisted.LastAttemptFailure, false)
	}
	return Webhook{WebhookInfo: info}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	webhook, ok := state.(Webhook)
	if !ok {
		return nil, fmt.Errorf("invalid webhook provided: %v", state)
	}
	persisted := webhookInfoPersist{
		ID:                      webhook.ID,
		URL:                     webhook.URL,
		Method:                  webhook.Method,
		Headers:                 cloneHeaders(webhook.Headers),
		Body:                    webhook.Body,
		State:                   webhook.State(),
		Attempt:                 webhook.Attempt,
		MaxAttempts:             webhook.MaxAttempts,
		LastResponseStatusCode:  webhook.LastResponseStatusCode,
		RegistrationTime:        webhook.RegistrationTime,
		LastAttemptCompleteTime: webhook.LastAttemptCompleteTime,
		NextAttemptScheduleTime: webhook.NextAttemptScheduleTime,
	}
	if webhook.LastAttemptFailure != nil {
		persisted.LastAttemptFailure = webhook.LastAttemptFailure.Message
	}
	return json.Marshal(persisted)
}

func (stateMachineDefinition) CompareState(state1, state2 any) (int, error) {
	webhook1, ok := state1.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	webhook2, ok := state2.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := webhook1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := webhook2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && webhook1.State() != webhook2.State() {
		return 0, serviceerror.NewInvalidArgumentf("cannot compare two distinct terminal states: %v, %v", webhook1.State(), webhook2.State())
	}
	return int(attempts1 - attempts2), nil
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]State{StateStandby},
	StateScheduled,
	func(webhook Webhook, event EventScheduled) (hsm.TransitionOutput, error) {
		return webhook.output()
	},
)

type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]State{StateBackingOff},
	StateScheduled,
	func(webhook Webhook, event EventRescheduled) (hsm.TransitionOutput, error) {
		webhook.NextAttemptScheduleTime = nil
		return webhook.output()
	},
)

type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
	StatusCode  int
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]State{StateScheduled},
	StateBackingOff,
	func(webhook Webhook, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		webhook.recordAttempt(event.Time)
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(webhook.Attempt), event.Err)
		nextAttempt := event.Time.UTC().Add(nextDelay)
		webhook.NextAttemptScheduleTime = &nextAttempt
		webhook.LastResponseStatusCode = event.StatusCode
		webhook.LastAttemptFailure = newFailure(event.Err.Error(), false)
		return webhook.output()
	},
)

type EventFailed struct {
	Time       time.Time
	Err        error
	StatusCode int
}

var TransitionFailed = hsm.NewTransition(
	[]State{StateScheduled},
	StateFailed,
	func(webhook Webhook, event EventFailed) (hsm.TransitionOutput, error) {
		webhook.recordAttempt(event.Time)
		webhook.NextAttemptScheduleTime = nil
		webhook.LastResponseStatusCode = event.StatusCode
		webhook.LastAttemptFailure = newFailure(event.Err.Error(), true)
		return webhook.output()
	},
)

type EventCompleted struct {
	Time       time.Time
	StatusCode int
}

var TransitionCompleted = hsm.NewTransition(
	[]State{StateScheduled},
	StateCompleted,
	func(webhook Webhook, event EventCompleted) (hsm.TransitionOutput, error) {
		webhook.recordAttempt(event.Time)
		webhook.NextAttemptScheduleTime = nil
		webhook.LastResponseStatusCode = event.StatusCode
		webhook.LastAttemptFailure = nil
		return webhook.output()
	},
)

func cloneHeaders(headers map[string]string) map[string]string {
	if len(headers) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(headers))
	for k, v := range headers {
		cloned[k] = v
	}
	return cloned
}

func destinationFromURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse webhook URL %q: %w", rawURL, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", serviceerror.NewInvalidArgumentf("invalid webhook URL %q", rawURL)
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

func newFailure(message string, nonRetryable bool) *failurepb.Failure {
	return &failurepb.Failure{
		Message: message,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: nonRetryable,
			},
		},
	}
}
