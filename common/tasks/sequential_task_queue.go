package tasks

type (
	SequentialTaskQueueFactory[K comparable, T Task] func(task T) SequentialTaskQueue[K, T]

	SequentialTaskQueue[K comparable, T Task] interface {
		// ID return the ID of the queue, as well as the tasks inside (same)
		ID() K
		// Add push a task to the task set
		Add(T)
		// Remove pop a task from the task set
		Remove() T
		// IsEmpty indicate if the task set is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}
)
