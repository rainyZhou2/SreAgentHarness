package replay

import (
	"context"
	"sync"
	"sync/atomic"

	"go-sre-harness/internal/security"
)

type projectionContextKey struct{}

type projectionState struct {
	guard  *Guard
	name   string
	active atomic.Bool
}

type Projection struct {
	state *projectionState
	once  sync.Once
}

func (g *Guard) Context(parent context.Context, name string) (context.Context, *Projection, error) {
	if g == nil || parent == nil {
		return nil, nil, ErrReadOnly
	}
	if _, exists := parent.Value(projectionContextKey{}).(*projectionState); exists {
		return nil, nil, &security.Error{Code: security.InvalidRequest, Message: "nested replay projection"}
	}
	if err := g.beginProjection(name); err != nil {
		return nil, nil, err
	}
	state := &projectionState{guard: g, name: name}
	state.active.Store(true)
	projection := &Projection{state: state}
	return context.WithValue(parent, projectionContextKey{}, state), projection, nil
}

func (p *Projection) End(err error) {
	if p == nil || p.state == nil {
		return
	}
	p.once.Do(func() {
		p.state.active.Store(false)
		p.state.guard.endProjection(p.state.name, err)
	})
}

func (p *Projection) Authorizer() security.Authorizer {
	if p == nil || p.state == nil {
		return projectionAuthorizer{}
	}
	return projectionAuthorizer{state: p.state}
}

func AuthorizerFromContext(ctx context.Context) (security.Authorizer, error) {
	if ctx == nil {
		return nil, ErrReadOnly
	}
	state, ok := ctx.Value(projectionContextKey{}).(*projectionState)
	if !ok || state == nil || !state.active.Load() {
		return nil, ErrReadOnly
	}
	return projectionAuthorizer{state: state}, nil
}

type projectionAuthorizer struct {
	state *projectionState
}

func (a projectionAuthorizer) Authorize(ctx context.Context, request security.Request) (security.PolicyDecision, error) {
	if a.state == nil || !a.state.active.Load() {
		return security.PolicyDecision{}, ErrReadOnly
	}
	guard := a.state.guard
	if request.SideEffecting() {
		_ = guard.recorder.Record("blocked", request.Operation)
		return security.PolicyDecision{}, ErrReadOnly
	}
	if guard.reads == nil {
		return security.PolicyDecision{}, &security.Error{Code: security.PolicyUnavailable, Message: "replay read authorizer is not configured"}
	}
	return guard.reads.Authorize(ctx, request)
}

func (a projectionAuthorizer) Recheck(_ context.Context, request security.Request, _ security.PolicyDecision) error {
	if a.state != nil {
		_ = a.state.guard.recorder.Record("blocked", request.Operation)
	}
	return ErrReadOnly
}
