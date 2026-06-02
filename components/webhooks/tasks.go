package webhooks

import (
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeWebhook = "webhooks.Invocation"
	TaskTypeBackoff = "webhooks.Backoff"
)

type WebhookTask struct {
	destination string
}

var _ hsm.Task = WebhookTask{}

func NewWebhookTask(destination string) WebhookTask {
	return WebhookTask{destination: destination}
}

func (WebhookTask) Type() string {
	return TaskTypeWebhook
}

func (t WebhookTask) Destination() string {
	return t.destination
}

func (WebhookTask) Deadline() time.Time {
	return hsm.Immediate
}

func (WebhookTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[State, Webhook](node, StateScheduled)
}

type WebhookTaskSerializer struct{}

func (WebhookTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return WebhookTask{destination: attrs.Destination}, nil
}

func (WebhookTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	if _, ok := task.(WebhookTask); !ok {
		return nil, fmt.Errorf("incompatible task: %v", task)
	}
	return nil, nil
}

type BackoffTask struct {
	deadline time.Time
}

var _ hsm.Task = BackoffTask{}

func (BackoffTask) Type() string {
	return TaskTypeBackoff
}

func (t BackoffTask) Deadline() time.Time {
	return t.deadline
}

func (BackoffTask) Destination() string {
	return ""
}

func (BackoffTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[State, Webhook](node, StateBackingOff)
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return BackoffTask{deadline: attrs.Deadline}, nil
}

func (BackoffTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	if _, ok := task.(BackoffTask); !ok {
		return nil, fmt.Errorf("incompatible task: %v", task)
	}
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeWebhook, WebhookTaskSerializer{}); err != nil {
		return err
	}
	return reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{})
}
