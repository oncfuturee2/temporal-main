package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockOwnershipBasedQuotaScaler struct {
	scaleFactor float64
	ok          bool
}

func (m *mockOwnershipBasedQuotaScaler) ScaleFactor() (float64, bool) {
	return m.scaleFactor, m.ok
}

type mockMemberCounter struct {
	count int
}

func (m *mockMemberCounter) AvailableMemberCount() int {
	return m.count
}

func TestGetOwnershipScaledQuota(t *testing.T) {
	t.Run("Scaler is nil", func(t *testing.T) {
		quota, ok := getOwnershipScaledQuota(nil, 100)
		require.False(t, ok)
		require.InDelta(t, float64(0), quota, 0.0001)
	})

	t.Run("Global limit is abnormal (<= 0)", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.5, ok: true}
		quota, ok := getOwnershipScaledQuota(scaler, 0)
		require.False(t, ok)
		require.InDelta(t, float64(0), quota, 0.0001)

		quota, ok = getOwnershipScaledQuota(scaler, -10)
		require.False(t, ok)
		require.InDelta(t, float64(0), quota, 0.0001)
	})

	t.Run("ScaleFactor not ok (unavailable or not ready)", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.5, ok: false}
		quota, ok := getOwnershipScaledQuota(scaler, 100)
		require.False(t, ok)
		require.InDelta(t, float64(0), quota, 0.0001)
	})

	t.Run("ScaleFactor valid", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.25, ok: true}
		quota, ok := getOwnershipScaledQuota(scaler, 100)
		require.True(t, ok)
		require.InDelta(t, float64(25), quota, 0.0001)
	})

	t.Run("ScaleFactor zero", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.0, ok: true}
		quota, ok := getOwnershipScaledQuota(scaler, 100)
		require.True(t, ok)
		require.InDelta(t, float64(0), quota, 0.0001)
	})

	t.Run("ScaleFactor greater than 1", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 1.5, ok: true}
		quota, ok := getOwnershipScaledQuota(scaler, 100)
		require.True(t, ok)
		require.InDelta(t, float64(150), quota, 0.0001)
	})
}

func TestOwnershipAwareQuotaCalculator_GetQuota(t *testing.T) {
	t.Run("Uses scaled quota when available", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.2, ok: true}
		counter := &mockMemberCounter{count: 10}
		perInstance := func() int { return 100 }
		global := func() int { return 1000 }

		calc := NewOwnershipAwareQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota()
		require.InDelta(t, float64(200), quota, 0.0001)
	})

	t.Run("Falls back to cluster aware calculator when scaler not available", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0, ok: false}
		counter := &mockMemberCounter{count: 5}
		perInstance := func() int { return 100 }
		global := func() int { return 1000 }

		calc := NewOwnershipAwareQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota()
		require.InDelta(t, float64(200), quota, 0.0001)
	})

	t.Run("Falls back to per instance when global limit is 0", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.2, ok: true}
		counter := &mockMemberCounter{count: 5}
		perInstance := func() int { return 150 }
		global := func() int { return 0 }

		calc := NewOwnershipAwareQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota()
		require.InDelta(t, float64(150), quota, 0.0001)
	})
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota(t *testing.T) {
	t.Run("Uses scaled quota when available", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.3, ok: true}
		counter := &mockMemberCounter{count: 10}
		perInstance := func(ns string) int { return 100 }
		global := func(ns string) int {
			if ns == "test-ns" {
				return 2000
			}
			return 1000
		}

		calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota("test-ns")
		require.InDelta(t, float64(600), quota, 0.0001)
	})

	t.Run("Falls back to cluster aware calculator when scaler not available", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0, ok: false}
		counter := &mockMemberCounter{count: 4}
		perInstance := func(ns string) int { return 100 }
		global := func(ns string) int { return 1200 }

		calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota("test-ns")
		require.InDelta(t, float64(300), quota, 0.0001)
	})

	t.Run("Falls back to per instance when global limit is 0", func(t *testing.T) {
		scaler := &mockOwnershipBasedQuotaScaler{scaleFactor: 0.3, ok: true}
		counter := &mockMemberCounter{count: 4}
		perInstance := func(ns string) int { return 250 }
		global := func(ns string) int { return 0 }

		calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, counter, perInstance, global)

		quota := calc.GetQuota("test-ns")
		require.InDelta(t, float64(250), quota, 0.0001)
	})
}
