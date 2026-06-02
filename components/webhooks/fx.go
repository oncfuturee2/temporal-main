package webhooks

import (
	"net/http"

	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.webhooks",
	fx.Provide(ConfigProvider),
	fx.Provide(HTTPClientProvider),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterExecutor),
)

func HTTPClientProvider(logger log.Logger) *http.Client {
	return &http.Client{
		Timeout: 0,
	}
}
