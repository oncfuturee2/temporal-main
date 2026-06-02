package collection

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type (
	boolType bool
	intType  int
)

func TestConcurrentTxMapLen(t *testing.T) {
	testMap := NewShardedConcurrentTxMap[string, boolType](1, UUIDHashCode)

	key1 := "0001"
	testMap.Put(key1, boolType(true))
	require.Equal(t, 1, testMap.Len())

	testMap.Put(key1, boolType(false))
	require.Equal(t, 1, testMap.Len())

	key2 := "0002"
	testMap.Put(key2, boolType(false))
	require.Equal(t, 2, testMap.Len())

	testMap.PutIfNotExist(key2, boolType(false))
	require.Equal(t, 2, testMap.Len())

	testMap.Remove(key2)
	require.Equal(t, 1, testMap.Len())

	testMap.Remove(key2)
	require.Equal(t, 1, testMap.Len())
}

func TestConcurrentTxMapGetAndDo(t *testing.T) {
	testMap := NewShardedConcurrentTxMap[string, *intType](1, UUIDHashCode)
	key := uuid.NewString()
	fnApplied := false

	value, ok, err := testMap.GetAndDo(key, func(key string, value *intType) error {
		fnApplied = true
		return nil
	})
	require.Nil(t, value)
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, fnApplied)

	storedValue := intType(1)
	testMap.Put(key, &storedValue)
	value, ok, err = testMap.GetAndDo(key, func(seenKey string, value *intType) error {
		fnApplied = true
		require.Equal(t, key, seenKey)
		*value += 1
		return errors.New("some err")
	})

	require.Equal(t, intType(2), *value)
	require.Error(t, err)
	require.True(t, ok)
	require.True(t, fnApplied)
}

func TestConcurrentTxMapPutOrDo(t *testing.T) {
	testMap := NewShardedConcurrentTxMap[string, *intType](1, UUIDHashCode)
	key := uuid.NewString()
	fnApplied := false

	value := intType(1)
	returnedValue, ok, err := testMap.PutOrDo(key, &value, func(key string, value *intType) error {
		fnApplied = true
		return errors.New("some err")
	})
	require.Equal(t, value, *returnedValue)
	require.NoError(t, err)
	require.False(t, ok)
	require.False(t, fnApplied)

	anotherValue := intType(111)
	returnedValue, ok, err = testMap.PutOrDo(key, &anotherValue, func(seenKey string, value *intType) error {
		fnApplied = true
		require.Equal(t, key, seenKey)
		*value += 1
		return errors.New("some err")
	})
	require.Equal(t, intType(2), *returnedValue)
	require.Error(t, err)
	require.True(t, ok)
	require.True(t, fnApplied)
}

func TestConcurrentTxMapRemoveIf(t *testing.T) {
	testMap := NewShardedConcurrentTxMap[string, *intType](1, UUIDHashCode)
	key := uuid.NewString()
	value := intType(1)
	testMap.Put(key, &value)

	removed := testMap.RemoveIf(key, func(seenKey string, value *intType) bool {
		require.Equal(t, key, seenKey)
		return *value == intType(2)
	})
	require.Equal(t, 1, testMap.Len())
	require.False(t, removed)

	removed = testMap.RemoveIf(key, func(seenKey string, value *intType) bool {
		require.Equal(t, key, seenKey)
		return *value == intType(1)
	})
	require.Equal(t, 0, testMap.Len())
	require.True(t, removed)
}

func TestConcurrentTxMapGetAfterPutAndIterate(t *testing.T) {
	countMap := make(map[string]int)
	testMap := NewShardedConcurrentTxMap[string, boolType](1, UUIDHashCode)

	for range 1024 {
		key := uuid.NewString()
		countMap[key] = 0
		testMap.Put(key, boolType(true))
	}

	for key := range countMap {
		value, ok := testMap.Get(key)
		require.True(t, ok)
		require.True(t, bool(value))
	}

	require.Equal(t, len(countMap), testMap.Len())

	iterator := testMap.Iter()
	for entry := range iterator.Entries() {
		countMap[entry.Key]++
		require.True(t, bool(entry.Value))
	}
	iterator.Close()

	for _, count := range countMap {
		require.Equal(t, 1, count)
	}

	for key := range countMap {
		testMap.Remove(key)
	}

	require.Equal(t, 0, testMap.Len())
}

func TestConcurrentTxMapPutIfNotExist(t *testing.T) {
	testMap := NewShardedConcurrentTxMap[string, boolType](1, UUIDHashCode)
	key := uuid.NewString()

	ok := testMap.PutIfNotExist(key, boolType(true))
	require.True(t, ok)

	ok = testMap.PutIfNotExist(key, boolType(true))
	require.False(t, ok)
}

func TestConcurrentTxMapConcurrency(t *testing.T) {
	nKeys := 1024
	keys := make([]string, nKeys)
	for i := range nKeys {
		keys[i] = uuid.NewString()
	}

	var total int32
	var startWG sync.WaitGroup
	var doneWG sync.WaitGroup
	errCh := make(chan error, 10)
	testMap := NewShardedConcurrentTxMap[string, intType](1024, UUIDHashCode)

	startWG.Add(1)

	for range 10 {
		doneWG.Add(1)
		go func() {
			defer doneWG.Done()
			startWG.Wait()
			for n := range nKeys {
				value := intType(rand.Int())
				if testMap.PutIfNotExist(keys[n], value) {
					atomic.AddInt32(&total, int32(value))
					if _, ok := testMap.Get(keys[n]); !ok {
						select {
						case errCh <- fmt.Errorf("missing key %q after PutIfNotExist", keys[n]):
						default:
						}
					}
				}
			}
		}()
	}

	startWG.Done()
	doneWG.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	require.Equal(t, nKeys, testMap.Len())

	var gotTotal int32
	for i := range nKeys {
		value, ok := testMap.Get(keys[i])
		require.True(t, ok)
		gotTotal += int32(value)
	}

	require.Equal(t, total, gotTotal)
}
