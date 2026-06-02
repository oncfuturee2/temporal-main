package events

import (
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
)

const (
	eventsChanSize = 1000
)

type (
	Notifier interface {
		NotifyNewHistoryEvent(event *Notification)
		WatchHistoryEvent(identifier definition.WorkflowKey) (string, chan *Notification, error)
		UnwatchHistoryEvent(identifier definition.WorkflowKey, subscriberID string) error
		Start()
		Stop()
	}

	Notification struct {
		ID                     definition.WorkflowKey
		LastFirstEventID       int64
		LastFirstEventTxnID    int64
		NextEventID            int64
		PreviousStartedEventID int64
		Timestamp              time.Time
		WorkflowState          enumsspb.WorkflowExecutionState
		WorkflowStatus         enumspb.WorkflowExecutionStatus
		VersionHistories       *historyspb.VersionHistories
		TransitionHistory      []*persistencespb.VersionedTransition
	}

	NotifierImpl struct {
		timeSource          clock.TimeSource
		metricsHandler      metrics.Handler
		status              int32
		closeChan           chan bool
		eventsChan          chan *Notification
		workflowIDToShardID func(namespace.ID, string) int32

		eventsPubsubs collection.ConcurrentTxMap[definition.WorkflowKey, map[string]chan *Notification]
	}
)

var _ Notifier = (*NotifierImpl)(nil)

func NewNotification(
	namespaceID string,
	workflowExecution *commonpb.WorkflowExecution,
	lastFirstEventID int64,
	lastFirstEventTxnID int64,
	nextEventID int64,
	previousStartedEventID int64,
	workflowState enumsspb.WorkflowExecutionState,
	workflowStatus enumspb.WorkflowExecutionStatus,
	versionHistories *historyspb.VersionHistories,
	transitionHistory []*persistencespb.VersionedTransition,
) *Notification {

	return &Notification{
		ID: definition.NewWorkflowKey(
			namespaceID,
			workflowExecution.GetWorkflowId(),
			workflowExecution.GetRunId(),
		),
		LastFirstEventID:       lastFirstEventID,
		LastFirstEventTxnID:    lastFirstEventTxnID,
		NextEventID:            nextEventID,
		PreviousStartedEventID: previousStartedEventID,
		WorkflowState:          workflowState,
		WorkflowStatus:         workflowStatus,
		VersionHistories:       versionhistory.CopyVersionHistories(versionHistories),
		TransitionHistory:      transitionhistory.CopyVersionedTransitions(transitionHistory),
	}
}

func NewNotifier(
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
	workflowIDToShardID func(namespace.ID, string) int32,
) *NotifierImpl {

	hashFn := func(key definition.WorkflowKey) uint32 {
		return uint32(workflowIDToShardID(namespace.ID(key.NamespaceID), key.WorkflowID))
	}
	return &NotifierImpl{
		timeSource:     timeSource,
		metricsHandler: metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryEventNotificationScope)),
		status:         common.DaemonStatusInitialized,
		closeChan:      make(chan bool),
		eventsChan:     make(chan *Notification, eventsChanSize),

		workflowIDToShardID: workflowIDToShardID,

		eventsPubsubs: collection.NewShardedConcurrentTxMap[definition.WorkflowKey, map[string]chan *Notification](1024, hashFn),
	}
}

func (notifier *NotifierImpl) WatchHistoryEvent(
	identifier definition.WorkflowKey) (string, chan *Notification, error) {

	channel := make(chan *Notification, 1)
	subscriberID := uuid.NewString()
	subscribers := map[string]chan *Notification{
		subscriberID: channel,
	}

	_, _, err := notifier.eventsPubsubs.PutOrDo(identifier, subscribers, func(key definition.WorkflowKey, value map[string]chan *Notification) error {
		if _, ok := value[subscriberID]; ok {
			return serviceerror.NewUnavailable("Unable to watch on workflow execution.")
		}
		value[subscriberID] = channel
		return nil
	})

	if err != nil {
		return "", nil, err
	}

	return subscriberID, channel, nil
}

func (notifier *NotifierImpl) UnwatchHistoryEvent(
	identifier definition.WorkflowKey, subscriberID string) error {

	success := true
	notifier.eventsPubsubs.RemoveIf(identifier, func(key definition.WorkflowKey, value map[string]chan *Notification) bool {
		if _, ok := value[subscriberID]; !ok {
			success = false
		} else {
			delete(value, subscriberID)
		}

		return len(value) == 0
	})

	if !success {
		return serviceerror.NewInternal("Unable to unwatch on workflow execution.")
	}

	return nil
}

func (notifier *NotifierImpl) dispatchHistoryEventNotification(event *Notification) {
	identifier := event.ID

	startTime := time.Now().UTC()
	defer func() {
		metrics.HistoryEventNotificationFanoutLatency.With(notifier.metricsHandler).Record(time.Since(startTime))
	}()
	_, _, _ = notifier.eventsPubsubs.GetAndDo(identifier, func(key definition.WorkflowKey, value map[string]chan *Notification) error {
		for _, channel := range value {
			select {
			case channel <- event:
			default:
			}
		}
		return nil
	})
}

func (notifier *NotifierImpl) enqueueHistoryEventNotification(event *Notification) {
	event.Timestamp = notifier.timeSource.Now()
	select {
	case notifier.eventsChan <- event:
	default:
		metrics.HistoryEventNotificationFailDeliveryCount.With(notifier.metricsHandler).Record(1)
	}
}

func (notifier *NotifierImpl) dequeueHistoryEventNotifications() {
	for {
		metrics.HistoryEventNotificationInFlightMessageGauge.With(notifier.metricsHandler).Record(float64(len(notifier.eventsChan)))
		select {
		case event := <-notifier.eventsChan:
			timeelapsed := time.Since(event.Timestamp)
			metrics.HistoryEventNotificationQueueingLatency.With(notifier.metricsHandler).Record(timeelapsed)

			notifier.dispatchHistoryEventNotification(event)
		case <-notifier.closeChan:
			return
		}
	}
}

func (notifier *NotifierImpl) Start() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go notifier.dequeueHistoryEventNotifications()
}

func (notifier *NotifierImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(notifier.closeChan)
}

func (notifier *NotifierImpl) NotifyNewHistoryEvent(event *Notification) {
	notifier.enqueueHistoryEventNotification(event)
}
