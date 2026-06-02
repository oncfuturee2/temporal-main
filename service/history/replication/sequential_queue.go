package replication

import (
	"sync"

	"github.com/dgryski/go-farm"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	SequentialTaskQueue struct {
		id definition.WorkflowKey

		sync.Mutex
		taskQueue collection.Queue[TrackableExecutableTask]
	}
)

func NewSequentialTaskQueue(task TrackableExecutableTask) ctasks.SequentialTaskQueue[definition.WorkflowKey, TrackableExecutableTask] {
	return &SequentialTaskQueue{
		id: task.QueueID().(definition.WorkflowKey),

		taskQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
	}
}

func NewSequentialTaskQueueWithID(id definition.WorkflowKey) ctasks.SequentialTaskQueue[definition.WorkflowKey, TrackableExecutableTask] {
	return &SequentialTaskQueue{
		id: id,

		taskQueue: collection.NewPriorityQueue[TrackableExecutableTask](
			SequentialTaskQueueCompareLess,
		),
	}
}

func (q *SequentialTaskQueue) ID() definition.WorkflowKey {
	return q.id
}

func (q *SequentialTaskQueue) Peek() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Peek()
}

func (q *SequentialTaskQueue) Add(task TrackableExecutableTask) {
	q.Lock()
	defer q.Unlock()
	q.taskQueue.Add(task)
}

func (q *SequentialTaskQueue) Remove() TrackableExecutableTask {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Remove()
}

func (q *SequentialTaskQueue) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.IsEmpty()
}

func (q *SequentialTaskQueue) Len() int {
	q.Lock()
	defer q.Unlock()
	return q.taskQueue.Len()
}

func SequentialTaskQueueCompareLess(this TrackableExecutableTask, that TrackableExecutableTask) bool {
	return this.TaskID() < that.TaskID()
}

func WorkflowKeyHashFn(
	item definition.WorkflowKey,
) uint32 {
	idBytes := []byte(item.NamespaceID + "_" + item.WorkflowID + "_" + item.RunID)
	return farm.Fingerprint32(idBytes)
}
