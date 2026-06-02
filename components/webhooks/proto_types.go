package webhooks

import (
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// 临时的 WebhookStatus 枚举，模拟 protobuf 生成的类型
type WebhookStatus int32

const (
	WEBHOOK_STATUS_UNSPECIFIED    WebhookStatus = 0
	WEBHOOK_STATUS_STANDBY        WebhookStatus = 1
	WEBHOOK_STATUS_SCHEDULED      WebhookStatus = 2
	WEBHOOK_STATUS_BACKING_OFF    WebhookStatus = 3
	WEBHOOK_STATUS_SUCCEEDED      WebhookStatus = 4
	WEBHOOK_STATUS_FAILED         WebhookStatus = 5
)

// WebhookRequest 临时结构，模拟 persistencespb.Webhook
type WebhookRequest struct {
	Url     string
	Method  string
	Headers map[string]string
	Body    []byte
}

// WebhookInfo 临时结构，模拟 persistencespb.WebhookInfo
type WebhookInfo struct {
	Webhook                *WebhookRequest
	Status                 WebhookStatus
	Attempt                int32
	LastAttemptCompleteTime *timestamppb.Timestamp
	NextAttemptScheduleTime *timestamppb.Timestamp
	LastAttemptFailure     *failurepb.Failure
	RequestId              string
	CreatedTime            *timestamppb.Timestamp
}
