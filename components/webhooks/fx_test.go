package webhooks_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/webhooks"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/fx"
)

func TestModuleProvidesDependencies(t *testing.T) {
	app := fx.New(
		webhooks.Module,
		fx.Supply(dynamicconfig.NewNoopCollection()),
		fx.Provide(func() metrics.Handler { return metrics.NoopMetricsHandler }),
		fx.Provide(func() log.Logger { return log.NewNoopLogger() }),
		fx.Provide(hsm.NewRegistry),
	)
	require.NoError(t, app.Err())
}
