package webhooks

import (
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.webhooks",
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)
