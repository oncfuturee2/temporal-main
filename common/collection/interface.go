package collection

type (
	Queue[T any] interface {
		Peek() T
		Add(item T)
		Remove() T
		IsEmpty() bool
		Len() int
	}

	HashFunc[K comparable] func(K) uint32

	ActionFunc[K comparable, V any]    func(key K, value V) error
	PredicateFunc[K comparable, V any] func(key K, value V) bool

	ConcurrentTxMap[K comparable, V any] interface {
		Get(key K) (V, bool)
		Contains(key K) bool
		Put(key K, value V)
		PutIfNotExist(key K, value V) bool
		Remove(key K)
		GetAndDo(key K, fn ActionFunc[K, V]) (V, bool, error)
		PutOrDo(key K, value V, fn ActionFunc[K, V]) (V, bool, error)
		RemoveIf(key K, fn PredicateFunc[K, V]) bool
		Iter() MapIterator[K, V]
		Len() int
	}

	MapIterator[K comparable, V any] interface {
		Close()
		Entries() <-chan *MapEntry[K, V]
	}

	MapEntry[K comparable, V any] struct {
		Key   K
		Value V
	}
)

const (
	UUIDStringLength = 36
)
