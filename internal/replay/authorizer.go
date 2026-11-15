package replay

import (
	"context"

	"go-sre-harness/internal/security"
)

type authorizer struct {
	reads    security.Authorizer
	recorder Recorder
}

func (a authorizer) Authorize(ctx context.Context, request security.Request) (security.PolicyDecision, error) {
	if request.SideEffecting() {
		_ = a.recorder.Record("blocked", request.Operation)
		return security.PolicyDecision{}, ErrReadOnly
	}
	if a.reads == nil {
		return security.PolicyDecision{}, &security.Error{Code: security.PolicyUnavailable, Message: "replay read authorizer is not configured"}
	}
	return a.reads.Authorize(ctx, request)
}

func (a authorizer) Recheck(ctx context.Context, request security.Request, decision security.PolicyDecision) error {
	if request.SideEffecting() {
		_ = a.recorder.Record("blocked", request.Operation)
		return ErrReadOnly
	}
	if a.reads == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "replay read authorizer is not configured"}
	}
	return a.reads.Recheck(ctx, request, decision)
}
