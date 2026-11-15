package replay

import (
	"context"

	"go-sre-harness/internal/security"
)

type ProjectionFunc[Input, Output any] func(context.Context, Input) (Output, error)

func (g *Guard) beginProjection(name string) error {
	switch name {
	case "decision.v3", "work.view", "incident.view", "postmortem.view":
		return g.recorder.Record("started", security.Operation(name))
	default:
		return &security.Error{Code: security.InvalidRequest, Message: "unknown replay projection"}
	}
}

func (g *Guard) endProjection(name string, err error) {
	result := "completed"
	if err != nil {
		result = "failed"
	}
	_ = g.recorder.Record(result, security.Operation(name))
}

func Project[Input, Output any](ctx context.Context, guard *Guard, name string, input Input, fn ProjectionFunc[Input, Output]) (Output, error) {
	var zero Output
	if guard == nil || fn == nil {
		return zero, ErrReadOnly
	}
	projectionCtx, projection, err := guard.Context(ctx, name)
	if err != nil {
		return zero, err
	}
	output, err := fn(projectionCtx, input)
	projection.End(err)
	return output, err
}
