package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas/calculator"
)

type mockScaler struct {
	scaleFactor float64
	ok          bool
}

func (m *mockScaler) ScaleFactor() (float64, bool) {
	return m.scaleFactor, m.ok
}

type mockMemberCounter struct {
	count int
}

func (m *mockMemberCounter) AvailableMemberCount() int {
	return m.count
}

func TestGetOwnershipScaledQuota_NilScaler(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, 100)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_NilScaler_ZeroGlobalLimit(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, 0)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_NilScaler_NegativeGlobalLimit(t *testing.T) {
	t.Parallel()

	quota, ok := getOwnershipScaledQuota(nil, -10)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ZeroGlobalLimit(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 0)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_NegativeGlobalLimit(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, -1)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ScaleFactorNotReady(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.False(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ScaleFactorZeroButReady(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.True(t, ok)
	require.InDelta(t, 0.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ScaleFactorHalf(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.True(t, ok)
	require.InDelta(t, 50.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ScaleFactorOne(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 1.0, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 100)
	require.True(t, ok)
	require.InDelta(t, 100.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_ScaleFactorFractional(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.3, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 10)
	require.True(t, ok)
	require.InDelta(t, 3.0, quota, 1e-9)
}

func TestGetOwnershipScaledQuota_GlobalLimitOne(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	quota, ok := getOwnershipScaledQuota(scaler, 1)
	require.True(t, ok)
	require.InDelta(t, 0.5, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_ScaledQuota(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.3, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		mc,
		func() int { return 10 },
		func() int { return 100 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 30.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_ScalerNil(t *testing.T) {
	t.Parallel()

	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareQuotaCalculator(
		nil,
		mc,
		func() int { return 10 },
		func() int { return 100 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 25.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_ScaleFactorNotReady(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		mc,
		func() int { return 10 },
		func() int { return 100 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 25.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_GlobalLimitZero(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		mc,
		func() int { return 10 },
		func() int { return 0 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_GlobalLimitNegative(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		mc,
		func() int { return 10 },
		func() int { return -5 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_NilMemberCounter(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		nil,
		func() int { return 10 },
		func() int { return 100 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareQuotaCalculator_Fallback_ZeroMemberCount(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	mc := &mockMemberCounter{count: 0}
	calc := NewOwnershipAwareQuotaCalculator(
		scaler,
		mc,
		func() int { return 10 },
		func() int { return 100 },
	)

	quota := calc.GetQuota()
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_ScaledQuota(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.3, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 100 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 30.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_ScalerNil(t *testing.T) {
	t.Parallel()

	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		nil,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 100 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 25.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_ScaleFactorNotReady(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 100 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 25.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_GlobalLimitZero(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 0 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_GlobalLimitNegative(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 4}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return -5 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_NilMemberCounter(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		nil,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 100 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_Fallback_ZeroMemberCount(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0, ok: false}
	mc := &mockMemberCounter{count: 0}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return 100 },
	)

	quota := calc.GetQuota("test-namespace")
	require.InDelta(t, 10.0, quota, 1e-9)
}

func TestOwnershipAwareNamespaceQuotaCalculator_NamespaceSpecificQuotas(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 4}
	nsQuotas := map[string]int{"ns-a": 200, "ns-b": 60}
	calc := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		mc,
		func(namespace string) int { return 10 },
		func(namespace string) int { return nsQuotas[namespace] },
	)

	quotaA := calc.GetQuota("ns-a")
	require.InDelta(t, 100.0, quotaA, 1e-9)

	quotaB := calc.GetQuota("ns-b")
	require.InDelta(t, 30.0, quotaB, 1e-9)
}

func TestNewOwnershipAwareQuotaCalculator_Construction(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 3}
	perInstanceFn := func() int { return 5 }
	globalFn := func() int { return 50 }

	calc := NewOwnershipAwareQuotaCalculator(scaler, mc, perInstanceFn, globalFn)

	require.NotNil(t, calc)
	require.Equal(t, calculator.MemberCounter(mc), calc.MemberCounter)
	require.Equal(t, 5, calc.PerInstanceQuota())
	require.Equal(t, 50, calc.GlobalQuota())
}

func TestNewOwnershipAwareNamespaceQuotaCalculator_Construction(t *testing.T) {
	t.Parallel()

	scaler := &mockScaler{scaleFactor: 0.5, ok: true}
	mc := &mockMemberCounter{count: 3}
	perInstanceFn := func(namespace string) int { return 5 }
	globalFn := func(namespace string) int { return 50 }

	calc := NewOwnershipAwareNamespaceQuotaCalculator(scaler, mc, perInstanceFn, globalFn)

	require.NotNil(t, calc)
	require.Equal(t, calculator.MemberCounter(mc), calc.MemberCounter)
	require.Equal(t, 5, calc.PerInstanceQuota("any-ns"))
	require.Equal(t, 50, calc.GlobalQuota("any-ns"))
}
