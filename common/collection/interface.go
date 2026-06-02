package collection

type (
	// Queue is the interface for queue
	Queue[T any] interface {
		// Peek returns the first item of the queue
		Peek() T
		// Add push an item to the queue
		Add(item T)
		// Remove pop an item from the queue
		Remove() T
		// IsEmpty indicate if the queue is empty
		IsEmpty() bool
		// Len return the size of the queue
		Len() int
	}

	// HashFunc represents a hash function for a comparable key type
	HashFunc[K comparable] func(K) uint32

	// ActionFunc take a key and value, do calculation and return err
	ActionFunc[K comparable, V any] func(key K, value V) error
	// PredicateFunc take a key and value, do calculation and return boolean
	PredicateFunc[K comparable, V any] func(key K, value V) bool

	// ConcurrentTxMap is a generic interface for any implementation of a dictionary
	// or a key value lookup table that is thread safe, and providing functionality
	// to modify key / value pair inside within a transaction
	ConcurrentTxMap[K comparable, V any] interface {
		// Get returns the value for the given key
		Get(key K) (V, bool)
		// Contains returns true if the key exist and false otherwise
		Contains(key K) bool
		// Put records the mapping from given key to value
		Put(key K, value V)
		// PutIfNotExist records the key value mapping only
		// if the mapping does not already exist
		PutIfNotExist(key K, value V) bool
		// Remove deletes the key from the map
		Remove(key K)
		// GetAndDo returns the value corresponding to the key, and apply fn to key value before return value
		// return (value, value exist or not, error when evaluation fn)
		GetAndDo(key K, fn ActionFunc[K, V]) (V, bool, error)
		// PutOrDo put the key value in the map, if key does not exists, otherwise, call fn with existing key and value
		// return (value, fn evaluated or not, error when evaluation fn)
		PutOrDo(key K, value V, fn ActionFunc[K, V]) (V, bool, error)
		// RemoveIf deletes the given key from the map if fn return true
		// return whether the key is removed or not
		RemoveIf(key K, fn PredicateFunc[K, V]) bool
		// Iter returns an iterator to the map
		Iter() MapIterator[K, V]
		// Len returns the number of items in the map
		Len() int
	}

	// MapIterator represents the interface for map iterators
	MapIterator[K comparable, V any] interface {
		// Close closes the iterator
		// and releases any allocated resources
		Close()
		// Entries returns a channel of MapEntry
		// objects that can be used in a range loop
		Entries() <-chan *MapEntry[K, V]
	}

	// MapEntry represents a key-value entry within the map
	MapEntry[K comparable, V any] struct {
		// Key represents the key
		Key K
		// Value represents the value
		Value V
	}
)

const (
	// UUIDStringLength is the length of an UUID represented as a hex string
	UUIDStringLength = 36 // xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
)
