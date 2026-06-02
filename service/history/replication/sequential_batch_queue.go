package replication

import (
	"sync"

	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	SequentialBatchableTaskQueue struct {
		id definition.WorkflowKey

		sync.Mutex
		taskQueue                    collection.Queue[*batchedTask]
		lastTask                     *batchedTask
		batchedIndividualTaskHandler func(task TrackableExecutableTask)

		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func NewSequentialBatchableTaskQueue(
	task TrackableExecutableTask,
	batchedIndividualTaskHandler func(task TrackableExecutableTask),
	logger log.Logger,
	metricsHandler metrics.Handler,
) ctasks.SequentialTaskQueue[definition.WorkflowKey, TrackableExecutableTask] {
	return &SequentialBatchableTaskQueue{
		id: task.QueueID().(definition.WorkflowKey),

		taskQueue: collection.NewPriorityQueue[*batchedTask](
			sequentialBatchableTaskQueueCompareLess,
		),
		batchedIndividualTaskHandler: batchedIndividualTaskHandler,
		logger:                       logger,
		metricsHandler:               metricsHandler,
	}
}

func (q *SequentialBatchableTaskQueue) ID() definition.WorkflowKey {
	return q.id
}

func (q *SequentialBatchableTaskQueue) Peek() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Peek()
}

func (q *SequentialBatchableTaskQueue) Add(task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()

	if q.lastTask != nil && q.lastTask.AddTask(task) {
		return
	}

	incomingTask := q.createBatchedTask(task)
	q.taskQueue.Add(incomingTask)
	q.updateLastTask(incomingTask)
}

func (q *SequentialBatchableTaskQueue) Remove() (task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	taskToRemove := q.taskQueue.Remove()
	if taskToRemove == q.lastTask {
		q.lastTask = nil
	}
	return taskToRemove
}

func (q *SequentialBatchableTaskQueue) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.IsEmpty()
}

func (q *SequentialBatchableTaskQueue) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Len()
}

func (q *SequentialBatchableTaskQueue) updateLastTask(task *batchedTask) {
	if q.lastTask == nil || sequentialBatchableTaskQueueCompareLess(q.lastTask, task) {
		q.lastTask = task
	}
}

func (q *SequentialBatchableTaskQueue) createBatchedTask(task TrackableExecutableTask) *batchedTask {
	return &batchedTask{
		batchedTask:     task,
		individualTasks: []TrackableExecutableTask{task},
		state:           batchStateOpen,

		individualTaskHandler: func(task TrackableExecutableTask) {
			q.Add(task)
		},
		logger:         q.logger,
		metricsHandler: q.metricsHandler,
	}
}

func sequentialBatchableTaskQueueCompareLess(this *batchedTask, that *batchedTask) bool {
	return SequentialTaskQueueCompareLess(this, that)
}
