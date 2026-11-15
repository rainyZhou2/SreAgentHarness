package workengine

import "context"

func (e *Engine) runWorkWake(ctx context.Context) error {
	return e.Orch.RunWork(ctx, e.workBroker)
}
