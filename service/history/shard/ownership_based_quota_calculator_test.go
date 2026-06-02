package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/quotas/calculator"
)

type (
	testOwnershipBasedQuotaScaler struct {
		scaleFactor float64
		ok          bool
		calls       int
	}

	testMemberCounter struct {
		availableMemberCount int
		calls                int
	}

	testNamespaceQuota struct {
		value int
		calls []string
	}
)

var _ OwnershipBasedQuotaScaler = (*testOwnershipBasedQuotaScaler)(nil)
var _ calculator.MemberCounter = (*testMemberCounter)(nil)

func (s *testOwnershipBasedQuotaScaler) ScaleFactor() (float64, bool) {
	s.calls++
	return s.scaleFactor, s.ok
}

func (c *testMemberCounter) AvailableMemberCount() int {
	c.calls++
	return c.availableMemberCount
}

func (q *testNamespaceQuota) getQuota(namespace string) int {
	q.calls = append(q.calls, namespace)
	return q.value
}

func TestGetOwnershipScaledQuota(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		scaler        *testOwnershipBasedQuotaScaler
		globalLimit   int
		expectedQuota float64
		expectedOK    bool
		expectedCalls int
	}{
		{
			name:          "nil scaler",
			globalLimit:   100,
			expectedQuota: 0,
			expectedOK:    false,
		},
		{
			name: "zero global limit skips scaler",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 0.5,
				ok:          true,
			},
			globalLimit:   0,
			expectedQuota: 0,
			expectedOK:    false,
			expectedCalls: 0,
		},
		{
			name: "negative global limit skips scaler",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 0.5,
				ok:          true,
			},
			globalLimit:   -1,
			expectedQuota: 0,
			expectedOK:    false,
			expectedCalls: 0,
		},
		{
			name: "unready scale factor",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 0.5,
				ok:          false,
			},
			globalLimit:   100,
			expectedQuota: 0,
			expectedOK:    false,
			expectedCalls: 1,
		},
		{
			name: "zero scale factor is valid",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 0,
				ok:          true,
			},
			globalLimit:   100,
			expectedQuota: 0,
			expectedOK:    true,
			expectedCalls: 1,
		},
		{
			name: "positive scale factor",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 0.25,
				ok:          true,
			},
			globalLimit:   80,
			expectedQuota: 20,
			expectedOK:    true,
			expectedCalls: 1,
		},
		{
			name: "scale factor above one",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: 1.5,
				ok:          true,
			},
			globalLimit:   80,
			expectedQuota: 120,
			expectedOK:    true,
			expectedCalls: 1,
		},
		{
			name: "negative scale factor is applied when marked valid",
			scaler: &testOwnershipBasedQuotaScaler{
				scaleFactor: -0.5,
				ok:          true,
			},
			globalLimit:   80,
			expectedQuota: -40,
			expectedOK:    true,
			expectedCalls: 1,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var scaler OwnershipBasedQuotaScaler
			if tc.scaler != nil {
				scaler = tc.scaler
			}

			quota, ok := getOwnershipScaledQuota(scaler, tc.globalLimit)

			require.Equal(t, tc.expectedOK, ok)
			require.InDelta(t, tc.expectedQuota, quota, 0.000001)
			if tc.scaler != nil {
				require.Equal(t, tc.expectedCalls, tc.scaler.calls)
			}
		})
	}
}

func TestOwnershipAwareQuotaCalculatorGetQuotaUsesOwnershipScaledQuota(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.25,
		ok:          true,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 6}
	calculator := NewOwnershipAwareQuotaCalculator(
		scaler,
		memberCounter,
		func() int { return 10 },
		func() int { return 120 },
	)

	quota := calculator.GetQuota()

	require.InDelta(t, 30, quota, 0.000001)
	require.Equal(t, 1, scaler.calls)
	require.Equal(t, 0, memberCounter.calls)
}

func TestOwnershipAwareQuotaCalculatorGetQuotaFallsBackToClusterAwareQuota(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.25,
		ok:          false,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 6}
	calculator := NewOwnershipAwareQuotaCalculator(
		scaler,
		memberCounter,
		func() int { return 10 },
		func() int { return 120 },
	)

	quota := calculator.GetQuota()

	require.InDelta(t, 20, quota, 0.000001)
	require.Equal(t, 1, scaler.calls)
	require.Equal(t, 1, memberCounter.calls)
}

func TestOwnershipAwareQuotaCalculatorGetQuotaFallsBackToPerInstanceQuotaWhenGlobalQuotaDisabled(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.25,
		ok:          true,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 6}
	calculator := NewOwnershipAwareQuotaCalculator(
		scaler,
		memberCounter,
		func() int { return 15 },
		func() int { return 0 },
	)

	quota := calculator.GetQuota()

	require.InDelta(t, 15, quota, 0.000001)
	require.Equal(t, 0, scaler.calls)
	require.Equal(t, 0, memberCounter.calls)
}

func TestOwnershipAwareNamespaceQuotaCalculatorGetQuotaUsesOwnershipScaledQuota(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.4,
		ok:          true,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 5}
	perInstanceQuota := &testNamespaceQuota{value: 7}
	globalQuota := &testNamespaceQuota{value: 50}
	calculator := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		memberCounter,
		perInstanceQuota.getQuota,
		globalQuota.getQuota,
	)

	quota := calculator.GetQuota("test-namespace")

	require.InDelta(t, 20, quota, 0.000001)
	require.Equal(t, 1, scaler.calls)
	require.Equal(t, 0, memberCounter.calls)
	require.Empty(t, perInstanceQuota.calls)
	require.Equal(t, []string{"test-namespace"}, globalQuota.calls)
}

func TestOwnershipAwareNamespaceQuotaCalculatorGetQuotaFallsBackToClusterAwareQuota(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.4,
		ok:          false,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 5}
	perInstanceQuota := &testNamespaceQuota{value: 7}
	globalQuota := &testNamespaceQuota{value: 50}
	calculator := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		memberCounter,
		perInstanceQuota.getQuota,
		globalQuota.getQuota,
	)

	quota := calculator.GetQuota("test-namespace")

	require.InDelta(t, 10, quota, 0.000001)
	require.Equal(t, 1, scaler.calls)
	require.Equal(t, 1, memberCounter.calls)
	require.Equal(t, []string{"test-namespace"}, perInstanceQuota.calls)
	require.Equal(t, []string{"test-namespace", "test-namespace"}, globalQuota.calls)
}

func TestOwnershipAwareNamespaceQuotaCalculatorGetQuotaFallsBackToPerInstanceQuotaWhenGlobalQuotaDisabled(t *testing.T) {
	t.Parallel()

	scaler := &testOwnershipBasedQuotaScaler{
		scaleFactor: 0.4,
		ok:          true,
	}
	memberCounter := &testMemberCounter{availableMemberCount: 5}
	perInstanceQuota := &testNamespaceQuota{value: 9}
	globalQuota := &testNamespaceQuota{value: 0}
	calculator := NewOwnershipAwareNamespaceQuotaCalculator(
		scaler,
		memberCounter,
		perInstanceQuota.getQuota,
		globalQuota.getQuota,
	)

	quota := calculator.GetQuota("test-namespace")

	require.InDelta(t, 9, quota, 0.000001)
	require.Equal(t, 0, scaler.calls)
	require.Equal(t, 0, memberCounter.calls)
	require.Equal(t, []string{"test-namespace"}, perInstanceQuota.calls)
	require.Equal(t, []string{"test-namespace", "test-namespace"}, globalQuota.calls)
}
