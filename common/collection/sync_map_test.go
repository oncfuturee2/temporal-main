package collection_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/collection"
)

func TestSyncMapMultiThreaded(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	var wg sync.WaitGroup
	barrier := make(chan struct{})

	wg.Add(5)
	go func() {
		defer wg.Done()
		<-barrier
		for i := range 1000 {
			m.Set(i, i)
		}
	}()
	go func() {
		defer wg.Done()
		<-barrier
		for i := range 1000 {
			m.Get(i)
		}
	}()
	go func() {
		defer wg.Done()
		<-barrier
		for i := range 1000 {
			m.GetOrSet(i, i)
		}
	}()
	go func() {
		defer wg.Done()
		<-barrier
		for i := range 1000 {
			m.Pop(i)
		}
	}()
	go func() {
		defer wg.Done()
		<-barrier
		for range 1000 {
			m.PopAll()
		}
	}()

	close(barrier)
	wg.Wait()
}

func TestSyncMapGet(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)

	value, ok := m.Get(1)
	require.True(t, ok)
	require.Equal(t, 1, value)
}

func TestSyncMapGetOrSet(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)

	value, ok := m.GetOrSet(1, 2)
	require.True(t, ok)
	require.Equal(t, 1, value)

	value, ok = m.GetOrSet(2, 2)
	require.False(t, ok)
	require.Equal(t, 2, value)

	value, ok = m.Get(2)
	require.True(t, ok)
	require.Equal(t, 2, value)
}

func TestSyncMapDelete(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)
	m.Set(2, 1)
	m.Delete(1)

	_, ok := m.Get(1)
	require.False(t, ok)

	value, ok := m.Get(2)
	require.True(t, ok)
	require.Equal(t, 1, value)
}

func TestSyncMapPopReturnsFalseWhenKeyDoesNotExist(t *testing.T) {
	m := collection.NewSyncMap[int, int]()

	_, ok := m.Pop(1)
	require.False(t, ok)
}

func TestSyncMapPopReturnsTrueWhenKeyExists(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	m.Set(1, 1)

	value, ok := m.Pop(1)
	require.True(t, ok)
	require.Equal(t, 1, value)
}

func TestSyncMapPopAll(t *testing.T) {
	m := collection.NewSyncMap[int, int]()
	values := m.PopAll()
	require.Len(t, values, 0)

	m.Set(1, 1)
	m.Set(2, 2)
	m.Set(3, 3)
	m.Set(4, 4)
	m.Pop(4)

	mCopy := m

	values = m.PopAll()
	require.Len(t, values, 3)

	sum := 0
	for _, value := range values {
		sum += value
	}
	require.Equal(t, 6, sum)

	_, ok := mCopy.Get(3)
	require.False(t, ok)
}
