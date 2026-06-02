package collection

import (
	"sync"
	"sync/atomic"
)

const (
	nShards = 32
)

type (
	ShardedConcurrentTxMap[K comparable, V any] struct {
		shards     [nShards]mapShard[K, V]
		hashfn     HashFunc[K]
		size       int32
		initialCap int
	}

	mapIteratorImpl[K comparable, V any] struct {
		stopCh chan struct{}
		dataCh chan *MapEntry[K, V]
	}

	mapShard[K comparable, V any] struct {
		sync.RWMutex
		items map[K]V
	}
)

func NewShardedConcurrentTxMap[K comparable, V any](initialCap int, hashfn HashFunc[K]) ConcurrentTxMap[K, V] {
	cmap := new(ShardedConcurrentTxMap[K, V])
	cmap.hashfn = hashfn
	cmap.initialCap = max(nShards, initialCap/nShards)
	return cmap
}

func (cmap *ShardedConcurrentTxMap[K, V]) Get(key K) (V, bool) {
	shard := cmap.getShard(key)
	var value V
	var ok bool
	shard.RLock()
	if shard.items != nil {
		value, ok = shard.items[key]
	}
	shard.RUnlock()
	return value, ok
}

func (cmap *ShardedConcurrentTxMap[K, V]) Contains(key K) bool {
	_, ok := cmap.Get(key)
	return ok
}

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

func (cmap *ShardedConcurrentTxMap[K, V]) PutOrDo(key K, value V, fn ActionFunc[K, V]) (V, bool, error) {
	shard := cmap.getShard(key)
	var err error
	shard.Lock()
	cmap.lazyInitShard(shard)
	currentValue, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
		currentValue = value
		atomic.AddInt32(&cmap.size, 1)
	} else {
		err = fn(key, currentValue)
	}
	shard.Unlock()
	return currentValue, ok, err
}

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

func (it *mapIteratorImpl[K, V]) Close() {
	close(it.stopCh)
}

func (it *mapIteratorImpl[K, V]) Entries() <-chan *MapEntry[K, V] {
	return it.dataCh
}

func (cmap *ShardedConcurrentTxMap[K, V]) Iter() MapIterator[K, V] {
	iterator := &mapIteratorImpl[K, V]{
		dataCh: make(chan *MapEntry[K, V], 8),
		stopCh: make(chan struct{}),
	}

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
