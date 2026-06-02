package webhooks

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeWebhook = "webhooks.WebhookInvocation"
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
	return hsm.ValidateState[WebhookState, *Webhook](node, WebhookStateScheduled)
}

type WebhookTaskSerializer struct{}

func (WebhookTaskSerializer) Deserialize(_ []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return WebhookTask{destination: attrs.Destination}, nil
}

func (WebhookTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
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
	return hsm.ValidateState[WebhookState, *Webhook](node, WebhookStateBackingOff)
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(_ []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return BackoffTask{deadline: attrs.Deadline}, nil
}

func (BackoffTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeWebhook, WebhookTaskSerializer{}); err != nil {
		return err
	}
	if err := reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{}); err != nil {
		return err
	}
	return nil
}
