package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockQuotaScaler struct {
	scaleFactor float64
	ready       bool
}

func (m *mockQuotaScaler) ScaleFactor() (float64, bool) {
	return m.scaleFactor, m.ready
}

type mockMemberCounter struct {
	count int
}

func (m *mockMemberCounter) AvailableMemberCount() int {
	return m.count
}

func TestGetOwnershipScaledQuota_ScalerNil(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, 100)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_GlobalLimitZero(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.5, ready: true}
	quota, ok := getOwnershipScaledQuota(scaler, 0)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_GlobalLimitNegative(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.5, ready: true}
	quota, ok := getOwnershipScaledQuota(scaler, -1)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScaleFactorNotReady(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.5, ready: false}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScaleFactorZero(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.0, ready: true}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.True(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScaleFactorOne(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 1.0, ready: true}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.True(t, ok)
	require.InDelta(t, float64(100), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScaleFactorFractional(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.3, ready: true}
	quota, ok := getOwnershipScaledQuota(scaler, 200)
	require.True(t, ok)
	require.InDelta(t, float64(60), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScalerNilAndGlobalLimitZero(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, 0)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestGetOwnershipScaledQuota_ScalerNilAndScaleFactorNotReady(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, -5)
	require.False(t, ok)
	require.InDelta(t, float64(0), quota, 0.0001)
}

func TestOwnershipAwareQuotaCalculator_GetQuota_ScaleFactorReady(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.3, ready: true}
	memberCounter := &mockMemberCounter{count: 5}
	perInstanceQuota := func() int { return 10 }
	globalQuota := func() int { return 200 }

	calc := NewOwnershipAwareQuotaCalculator(scaler, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota()
	require.InDelta(t, float64(60), quota, 0.0001)
}

func TestOwnershipAwareQuotaCalculator_GetQuota_ScaleFactorNotReady_Fallback(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.3, ready: false}
	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func() int { return 10 }
	globalQuota := func() int { return 200 }

	calc := NewOwnershipAwareQuotaCalculator(scaler, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota()
	require.InDelta(t, float64(50), quota, 0.0001)
}

func TestOwnershipAwareQuotaCalculator_GetQuota_ScalerNil_Fallback(t *testing.T) {
	t.Parallel()

	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func() int { return 10 }
	globalQuota := func() int { return 200 }

	calc := NewOwnershipAwareQuotaCalculator(nil, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota()
	require.InDelta(t, float64(50), quota, 0.0001)
}

func TestOwnershipAwareQuotaCalculator_GetQuota_ScalerNil_NoMembers_Fallback(t *testing.T) {
	t.Parallel()

	memberCounter := &mockMemberCounter{count: 0}
	perInstanceQuota := func() int { return 10 }
	globalQuota := func() int { return 200 }

	calc := NewOwnershipAwareQuotaCalculator(nil, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota()
	require.InDelta(t, float64(10), quota, 0.0001)
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota_ScaleFactorReady(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.5, ready: true}
	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func(ns string) int { return 10 }
	globalQuota := func(ns string) int {
		if ns == "test-ns" {
			return 300
		}
		return 0
	}

	calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota("test-ns")
	require.InDelta(t, float64(150), quota, 0.0001)
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota_ScaleFactorNotReady_Fallback(t *testing.T) {
	t.Parallel()

	scaler := &mockQuotaScaler{scaleFactor: 0.5, ready: false}
	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func(ns string) int { return 10 }
	globalQuota := func(ns string) int {
		if ns == "test-ns" {
			return 300
		}
		return 0
	}

	calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota("test-ns")
	require.InDelta(t, float64(75), quota, 0.0001)
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota_ScalerNil_Fallback(t *testing.T) {
	t.Parallel()

	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func(ns string) int { return 10 }
	globalQuota := func(ns string) int {
		if ns == "test-ns" {
			return 300
		}
		return 0
	}

	calc := NewOwnershipAwareNamespaceQuotaCalculator(nil, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota("test-ns")
	require.InDelta(t, float64(75), quota, 0.0001)
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota_ScalerNil_NoGlobalLimit_Fallback(t *testing.T) {
	t.Parallel()

	memberCounter := &mockMemberCounter{count: 4}
	perInstanceQuota := func(ns string) int { return 10 }
	globalQuota := func(ns string) int { return 0 }

	calc := NewOwnershipAwareNamespaceQuotaCalculator(nil, memberCounter, perInstanceQuota, globalQuota)

	quota := calc.GetQuota("test-ns")
	require.InDelta(t, float64(10), quota, 0.0001)
}
