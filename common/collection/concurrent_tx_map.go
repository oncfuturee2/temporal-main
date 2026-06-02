package collection

import (
	"sync"
	"sync/atomic"
)

const (
	// nShards represents the number of shards
	// At any given point of time, there can only
	// be nShards number of concurrent writers to
	// the map at max
	nShards = 32
)

type (

	// ShardedConcurrentTxMap is an implementation of
	// ConcurrentMap that internally uses multiple
	// sharded maps to increase parallelism
	ShardedConcurrentTxMap[K comparable, V any] struct {
		shards     [nShards]mapShard[K, V]
		hashfn     HashFunc[K]
		size       int32
		initialCap int
	}

	// mapIteratorImpl represents an iterator type
	// for the concurrent map.
	mapIteratorImpl[K comparable, V any] struct {
		stopCh chan struct{}
		dataCh chan *MapEntry[K, V]
	}

	// mapShard represents a single instance
	// of thread safe map
	mapShard[K comparable, V any] struct {
		sync.RWMutex
		items map[K]V
	}
)

// NewShardedConcurrentTxMap returns an instance of ShardedConcurrentMap
//
// ShardedConcurrentMap is a thread safe map that maintains upto nShards
// number of maps internally to allow nShards writers to be acive at the
// same time. This map *does not* use re-entrant locks, so access to the
// map during iterator can cause a dead lock.
//
// @param initialSz
//
//	The initial size for the map
//
// @param hashfn
//
//	The hash function to use for sharding
func NewShardedConcurrentTxMap[K comparable, V any](initialCap int, hashfn HashFunc[K]) ConcurrentTxMap[K, V] {
	cmap := new(ShardedConcurrentTxMap[K, V])
	cmap.hashfn = hashfn
	cmap.initialCap = max(nShards, initialCap/nShards)
	return cmap
}

// Get returns the value corresponding to the key, if it exist
func (cmap *ShardedConcurrentTxMap[K, V]) Get(key K) (V, bool) {
	shard := cmap.getShard(key)
	var ok bool
	var value V
	shard.RLock()
	if shard.items != nil {
		value, ok = shard.items[key]
	}
	shard.RUnlock()
	return value, ok
}

// Contains returns true if the key exist and false otherwise
func (cmap *ShardedConcurrentTxMap[K, V]) Contains(key K) bool {
	_, ok := cmap.Get(key)
	return ok
}

// Put records the given key value mapping. Overwrites previous values
func (cmap *ShardedConcurrentTxMap[K, V]) Put(key K, value V) {
	shard := cmap.getShard(key)
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok := shard.items[key]
	if !ok {
		atomic.AddInt32(&cmap.size, 1)
	}
	shard.items[key] = value
	shard.Unlock()
}

// PutIfNotExist records the mapping, if there is no mapping for this key already
// Returns true if the mapping was recorded, false otherwise
func (cmap *ShardedConcurrentTxMap[K, V]) PutIfNotExist(key K, value V) bool {
	shard := cmap.getShard(key)
	var ok bool
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok = shard.items[key]
	if !ok {
		shard.items[key] = value
		atomic.AddInt32(&cmap.size, 1)
	}
	shard.Unlock()
	return !ok
}

// Remove deletes the given key from the map
func (cmap *ShardedConcurrentTxMap[K, V]) Remove(key K) {
	shard := cmap.getShard(key)
	shard.Lock()
	cmap.lazyInitShard(shard)
	_, ok := shard.items[key]
	if ok {
		delete(shard.items, key)
		atomic.AddInt32(&cmap.size, -1)
	}
	shard.Unlock()
}

// GetAndDo returns the value corresponding to the key, and apply fn to key value before return value
// return (value, value exist or not, error when evaluation fn)
func (cmap *ShardedConcurrentTxMap[K, V]) GetAndDo(key K, fn ActionFunc[K, V]) (V, bool, error) {
	shard := cmap.getShard(key)
	var value V
	var ok bool
	var err error
	shard.Lock()
	if shard.items != nil {
		value, ok = shard.items[key]
		if ok {
			err = fn(key, value)
		}
	}
	shard.Unlock()
	return value, ok, err
}

// PutOrDo put the key value in the map, if key does not exists, otherwise, call fn with existing key and value
// return (value, fn evaluated or not, error when evaluation fn)
func (cmap *ShardedConcurrentTxMap[K, V]) PutOrDo(key K, value V, fn ActionFunc[K, V]) (V, bool, error) {
	shard := cmap.getShard(key)
	var err error
	shard.Lock()
	cmap.lazyInitShard(shard)
	v, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
		v = value
		atomic.AddInt32(&cmap.size, 1)
	} else {
		err = fn(key, v)
	}
	shard.Unlock()
	return v, ok, err
}

// RemoveIf deletes the given key from the map if fn return true
func (cmap *ShardedConcurrentTxMap[K, V]) RemoveIf(key K, fn PredicateFunc[K, V]) bool {
	shard := cmap.getShard(key)
	var removed bool
	shard.Lock()
	if shard.items != nil {
		value, ok := shard.items[key]
		if ok && fn(key, value) {
			removed = true
			delete(shard.items, key)
			atomic.AddInt32(&cmap.size, -1)
		}
	}
	shard.Unlock()
	return removed
}

// Close closes the iterator
func (it *mapIteratorImpl[K, V]) Close() {
	close(it.stopCh)
}

// Entries returns a channel of map entries
func (it *mapIteratorImpl[K, V]) Entries() <-chan *MapEntry[K, V] {
	return it.dataCh
}

// Iter returns an iterator to the map. This map
// does not use re-entrant locks, so access or modification
// to the map during iteration can cause a dead lock.
func (cmap *ShardedConcurrentTxMap[K, V]) Iter() MapIterator[K, V] {

	iterator := new(mapIteratorImpl[K, V])
	iterator.dataCh = make(chan *MapEntry[K, V], 8)
	iterator.stopCh = make(chan struct{})

	go func(iterator *mapIteratorImpl[K, V]) {
		for i := range nShards {
			cmap.shards[i].RLock()
			for k, v := range cmap.shards[i].items {
				entry := &MapEntry[K, V]{Key: k, Value: v}
				select {
				case iterator.dataCh <- entry:
				case <-iterator.stopCh:
					cmap.shards[i].RUnlock()
					close(iterator.dataCh)
					return
				}
			}
			cmap.shards[i].RUnlock()
		}
		close(iterator.dataCh)
	}(iterator)

	return iterator
}

// Len returns the number of items in the map
func (cmap *ShardedConcurrentTxMap[K, V]) Len() int {
	return int(atomic.LoadInt32(&cmap.size))
}

func (cmap *ShardedConcurrentTxMap[K, V]) getShard(key K) *mapShard[K, V] {
	shardIdx := cmap.hashfn(key) % nShards
	return &cmap.shards[shardIdx]
}

func (cmap *ShardedConcurrentTxMap[K, V]) lazyInitShard(shard *mapShard[K, V]) {
	if shard.items == nil {
		shard.items = make(map[K]V, cmap.initialCap)
	}
}
