package webhooks

import (
	"encoding/json"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeInvocation = "webhooks.Invocation"
	TaskTypeBackoff    = "webhooks.Backoff"
)

type InvocationTask struct {
	URL     string
	Attempt int32
}

var _ hsm.Task = InvocationTask{}

func NewInvocationTask(url string, attempt int32) InvocationTask {
	return InvocationTask{URL: url, Attempt: attempt}
}

func (InvocationTask) Type() string {
	return TaskTypeInvocation
}

func (t InvocationTask) Destination() string {
	return ""
}

func (t InvocationTask) Deadline() time.Time {
	return hsm.Immediate
}

func (InvocationTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[WebhookState, Webhook](node, WebhookStateScheduled)
}

type InvocationTaskSerializer struct{}

func (InvocationTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var t InvocationTask
	if len(data) > 0 {
		_ = json.Unmarshal(data, &t)
	}
	return t, nil
}

func (InvocationTaskSerializer) Serialize(t hsm.Task) ([]byte, error) {
	return json.Marshal(t.(InvocationTask))
}

type BackoffTask struct {
	TaskDeadline time.Time
	Attempt      int32
}

var _ hsm.Task = BackoffTask{}

func (BackoffTask) Type() string {
	return TaskTypeBackoff
}

func (t BackoffTask) Deadline() time.Time {
	return t.TaskDeadline
}

func (BackoffTask) Destination() string {
	return ""
}

func (BackoffTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateState[WebhookState, Webhook](node, WebhookStateBackingOff)
}

type BackoffTaskSerializer struct{}

func (BackoffTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var t BackoffTask
	if len(data) > 0 {
		_ = json.Unmarshal(data, &t)
	}
	t.TaskDeadline = attrs.Deadline
	return t, nil
}

func (BackoffTaskSerializer) Serialize(t hsm.Task) ([]byte, error) {
	return json.Marshal(t.(BackoffTask))
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeInvocation, InvocationTaskSerializer{}); err != nil {
		return err
	}
	return reg.RegisterTaskSerializer(TaskTypeBackoff, BackoffTaskSerializer{})
}
