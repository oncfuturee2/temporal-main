package webhooks

import (
	"fmt"
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	StateMachineType = "webhooks.Webhook"

	terminalStage = 5
)

type MachineData struct {
	*WebhookInfo
}

func MachineCollection(tree *hsm.Node) hsm.Collection[Webhook] {
	return hsm.NewCollection[Webhook](tree, StateMachineType)
}

type Webhook struct {
	*WebhookInfo
}

func NewWebhook(
	requestId string,
	createdTime *timestamppb.Timestamp,
	webhook *WebhookRequest,
) Webhook {
	return Webhook{
		&WebhookInfo{
			Webhook:     webhook,
			Status:      WEBHOOK_STATUS_STANDBY,
			RequestId:   requestId,
			CreatedTime: createdTime,
		},
	}
}

func (w Webhook) State() WebhookStatus {
	return w.WebhookInfo.Status
}

func (w Webhook) SetState(s WebhookStatus) {
	w.WebhookInfo.Status = s
}

func (w Webhook) recordAttempt(ts time.Time) {
	w.WebhookInfo.Attempt++
	w.WebhookInfo.LastAttemptCompleteTime = timestamppb.New(ts)
}

func (w Webhook) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch w.WebhookInfo.Status {
	case WEBHOOK_STATUS_BACKING_OFF:
		return []hsm.Task{BackoffTask{deadline: w.NextAttemptScheduleTime.AsTime()}}, nil
	case WEBHOOK_STATUS_SCHEDULED:
		return []hsm.Task{InvocationTask{destination: w.Webhook.Url}}, nil
	}
	return nil, nil
}

func (w Webhook) output() (hsm.TransitionOutput, error) {
	tasks, err := w.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

func (w Webhook) progress() (int, int32, error) {
	switch w.Status {
	case WEBHOOK_STATUS_UNSPECIFIED:
		return 0, 0, fmt.Errorf("uninitialized webhook state")
	case WEBHOOK_STATUS_STANDBY:
		return 1, 0, nil
	case WEBHOOK_STATUS_BACKING_OFF:
		return 2, w.GetAttempt() * 2, nil
	case WEBHOOK_STATUS_SCHEDULED:
		return 2, w.GetAttempt()*2 + 1, nil
	case WEBHOOK_STATUS_FAILED, WEBHOOK_STATUS_SUCCEEDED:
		return terminalStage, 0, nil
	default:
		return 0, 0, fmt.Errorf("unknown webhook state")
	}
}

func (w Webhook) GetAttempt() int32 {
	if w.WebhookInfo == nil {
		return 0
	}
	return w.WebhookInfo.Attempt
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(data []byte) (any, error) {
	info := &WebhookInfo{}
	return Webhook{info}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if _, ok := state.(Webhook); ok {
		return nil, nil
	}
	return nil, fmt.Errorf("invalid webhook provided: %v", state)
}

func (stateMachineDefinition) CompareState(state1, state2 any) (int, error) {
	wh1, ok := state1.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	wh2, ok := state2.(Webhook)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Webhook instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := wh1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := wh2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && wh1.Status != wh2.Status {
		return 0, fmt.Errorf("cannot compare two distinct terminal states: %v, %v", wh1.Status, wh2.Status)
	}
	return int(attempts1 - attempts2), nil
}

func RegisterStateMachine(reg *hsm.Registry) error {
	return reg.RegisterMachine(stateMachineDefinition{})
}

type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]WebhookStatus{WEBHOOK_STATUS_STANDBY},
	WEBHOOK_STATUS_SCHEDULED,
	func(wh Webhook, event EventScheduled) (hsm.TransitionOutput, error) {
		return wh.output()
	},
)

type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]WebhookStatus{WEBHOOK_STATUS_BACKING_OFF},
	WEBHOOK_STATUS_SCHEDULED,
	func(wh Webhook, event EventRescheduled) (hsm.TransitionOutput, error) {
		wh.WebhookInfo.NextAttemptScheduleTime = nil
		return wh.output()
	},
)

type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]WebhookStatus{WEBHOOK_STATUS_SCHEDULED},
	WEBHOOK_STATUS_BACKING_OFF,
	func(wh Webhook, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		wh.recordAttempt(event.Time)
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(wh.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		wh.WebhookInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		wh.WebhookInfo.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		return wh.output()
	},
)

type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = hsm.NewTransition(
	[]WebhookStatus{WEBHOOK_STATUS_SCHEDULED},
	WEBHOOK_STATUS_FAILED,
	func(wh Webhook, event EventFailed) (hsm.TransitionOutput, error) {
		wh.recordAttempt(event.Time)
		wh.WebhookInfo.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return wh.output()
	},
)

type EventSucceeded struct {
	Time time.Time
}

var TransitionSucceeded = hsm.NewTransition(
	[]WebhookStatus{WEBHOOK_STATUS_SCHEDULED},
	WEBHOOK_STATUS_SUCCEEDED,
	func(wh Webhook, event EventSucceeded) (hsm.TransitionOutput, error) {
		wh.recordAttempt(event.Time)
		wh.WebhookInfo.LastAttemptFailure = nil
		return wh.output()
	},
)
