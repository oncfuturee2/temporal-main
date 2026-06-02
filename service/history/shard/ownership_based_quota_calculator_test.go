package shard

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.temporal.io/server/common/quotas/calculator"
)

type (
	mockScaler struct {
		scaleFactor float64
		ok          bool
	}

	mockMemberCounter struct {
		memberCount int
	}
)

func (m *mockScaler) ScaleFactor() (float64, bool) {
	return m.scaleFactor, m.ok
}

func (m *mockMemberCounter) AvailableMemberCount() int {
	return m.memberCount
}

func TestGetOwnershipScaledQuota(t *testing.T) {
	tests := []struct {
		name           string
		scaler         OwnershipBasedQuotaScaler
		globalLimit    int
		expectedQuota  float64
		expectedOk     bool
	}{
		{
			name:        "scaler nil",
			scaler:      nil,
			globalLimit: 100,
			expectedQuota: 0,
			expectedOk:    false,
		},
		{
			name:        "global limit zero",
			scaler:      &mockScaler{scaleFactor: 0.5, ok: true},
			globalLimit: 0,
			expectedQuota: 0,
			expectedOk:    false,
		},
		{
			name:        "global limit negative",
			scaler:      &mockScaler{scaleFactor: 0.5, ok: true},
			globalLimit: -100,
			expectedQuota: 0,
			expectedOk:    false,
		},
		{
			name:        "scaler returns false",
			scaler:      &mockScaler{scaleFactor: 0.0, ok: false},
			globalLimit: 100,
			expectedQuota: 0,
			expectedOk:    false,
		},
		{
			name:        "valid scale factor 0.5",
			scaler:      &mockScaler{scaleFactor: 0.5, ok: true},
			globalLimit: 100,
			expectedQuota: 50,
			expectedOk:    true,
		},
		{
			name:        "valid scale factor 1.0",
			scaler:      &mockScaler{scaleFactor: 1.0, ok: true},
			globalLimit: 100,
			expectedQuota: 100,
			expectedOk:    true,
		},
		{
			name:        "valid scale factor 0.0",
			scaler:      &mockScaler{scaleFactor: 0.0, ok: true},
			globalLimit: 100,
			expectedQuota: 0,
			expectedOk:    true,
		},
		{
			name:        "valid scale factor 2.0",
			scaler:      &mockScaler{scaleFactor: 2.0, ok: true},
			globalLimit: 100,
			expectedQuota: 200,
			expectedOk:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota, ok := getOwnershipScaledQuota(tt.scaler, tt.globalLimit)
			require.Equal(t, tt.expectedOk, ok)
			require.InDelta(t, tt.expectedQuota, quota, 0.001)
		})
	}
}

func TestOwnershipAwareQuotaCalculator_GetQuota(t *testing.T) {
	tests := []struct {
		name                 string
		scaler               OwnershipBasedQuotaScaler
		memberCount          int
		perInstanceQuota     int
		globalQuota          int
		expectedQuota        float64
	}{
		{
			name:                 "scaler not ready, fallback to cluster aware",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        50,
		},
		{
			name:                 "scaler nil, fallback to cluster aware",
			scaler:               nil,
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        50,
		},
		{
			name:                 "global quota zero, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.5, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          0,
			expectedQuota:        10,
		},
		{
			name:                 "scaler ready, use scaled quota",
			scaler:               &mockScaler{scaleFactor: 0.5, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        50,
		},
		{
			name:                 "scaler ready with full ownership",
			scaler:               &mockScaler{scaleFactor: 1.0, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        100,
		},
		{
			name:                 "member count zero, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          0,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        10,
		},
		{
			name:                 "member counter nil, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          0,
			perInstanceQuota:     10,
			globalQuota:          100,
			expectedQuota:        10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var memberCounter calculator.MemberCounter
			if tt.name != "member counter nil" {
				memberCounter = &mockMemberCounter{memberCount: tt.memberCount}
			}

			calc := NewOwnershipAwareQuotaCalculator(
				tt.scaler,
				memberCounter,
				func() int { return tt.perInstanceQuota },
				func() int { return tt.globalQuota },
			)

			quota := calc.GetQuota()
			require.InDelta(t, tt.expectedQuota, quota, 0.001)
		})
	}
}

func TestOwnershipAwareNamespaceQuotaCalculator_GetQuota(t *testing.T) {
	tests := []struct {
		name                 string
		scaler               OwnershipBasedQuotaScaler
		memberCount          int
		perInstanceQuota     int
		globalQuota          int
		namespace            string
		expectedQuota        float64
	}{
		{
			name:                 "scaler not ready, fallback to cluster aware",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        50,
		},
		{
			name:                 "scaler nil, fallback to cluster aware",
			scaler:               nil,
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        50,
		},
		{
			name:                 "global quota zero, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.5, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          0,
			namespace:            "test-namespace",
			expectedQuota:        10,
		},
		{
			name:                 "scaler ready, use scaled quota",
			scaler:               &mockScaler{scaleFactor: 0.5, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        50,
		},
		{
			name:                 "scaler ready with full ownership",
			scaler:               &mockScaler{scaleFactor: 1.0, ok: true},
			memberCount:          2,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        100,
		},
		{
			name:                 "member count zero, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          0,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        10,
		},
		{
			name:                 "member counter nil, fallback to per instance",
			scaler:               &mockScaler{scaleFactor: 0.0, ok: false},
			memberCount:          0,
			perInstanceQuota:     10,
			globalQuota:          100,
			namespace:            "test-namespace",
			expectedQuota:        10,
		},
		{
			name:                 "different namespace",
			scaler:               &mockScaler{scaleFactor: 0.75, ok: true},
			memberCount:          2,
			perInstanceQuota:     20,
			globalQuota:          200,
			namespace:            "another-namespace",
			expectedQuota:        150,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var memberCounter calculator.MemberCounter
			if tt.name != "member counter nil" {
				memberCounter = &mockMemberCounter{memberCount: tt.memberCount}
			}

			calc := NewOwnershipAwareNamespaceQuotaCalculator(
				tt.scaler,
				memberCounter,
				func(namespace string) int { return tt.perInstanceQuota },
				func(namespace string) int { return tt.globalQuota },
			)

			quota := calc.GetQuota(tt.namespace)
			require.InDelta(t, tt.expectedQuota, quota, 0.001)
		})
	}
}
