package tasks

type testSequentialTaskQueue[K comparable, T Task] struct {
	q  chan T
	id K
}

func newTestSequentialTaskQueue[K comparable, T Task](id K, capacity int) SequentialTaskQueue[K, T] {
	return &testSequentialTaskQueue[K, T]{
		q:  make(chan T, capacity),
		id: id,
	}
}

func (s *testSequentialTaskQueue[K, T]) ID() K {
	return s.id
}

func (s *testSequentialTaskQueue[K, T]) Add(task T) {
	s.q <- task
}

func (s *testSequentialTaskQueue[K, T]) Remove() T {
	select {
	case t := <-s.q:
		return t
	default:
		var emptyT T
		return emptyT
	}
}

func (s *testSequentialTaskQueue[K, T]) IsEmpty() bool {
	return len(s.q) == 0
}

func (s *testSequentialTaskQueue[K, T]) Len() int {
	return len(s.q)
}
