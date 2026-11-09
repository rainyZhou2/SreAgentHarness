package action

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"go-sre-harness/internal/obs"
)

// Observer receives the runtime's metric signals (RFC §21.1). Arguments are
// bounded enums; ids go to spans and logs only.
type Observer interface {
	// Preflight: ready | approval | rejected.
	Preflight(result string)
	// EffectUnknown: dispatch_error | lost_result.
	EffectUnknown(cause string)
	// Verify: the time from dispatch to a settled verification, by state.
	Verify(state State, d time.Duration)
	// Denied: a guard refused the action (policy, precondition, expired,
	// pending_effect, target_busy).
	Denied(reason string)
	// BreakGlass: an emergency approval was used.
	BreakGlass()
}

type nopObserver struct{}

func (nopObserver) Preflight(string)            {}
func (nopObserver) EffectUnknown(string)        {}
func (nopObserver) Verify(State, time.Duration) {}
func (nopObserver) Denied(string)               {}
func (nopObserver) BreakGlass()                 {}

func (rt *Runtime) obs() Observer {
	if rt.Observer != nil {
		return rt.Observer
	}
	return nopObserver{}
}

// span starts an action span (§21.2: action.preflight / execute /
// effect_verify) carrying ids as attributes, never args or results.
func span(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return obs.Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
}

func endSpan(sp trace.Span, rec *Record, err *error) {
	if rec != nil && rec.ID != "" {
		sp.SetAttributes(attribute.String("action_id", rec.ID), attribute.String("work_id", rec.WorkID),
			attribute.String("action.name", rec.ActionName), attribute.String("action.state", string(rec.State)),
			attribute.String("action.risk", string(rec.Risk)))
	}
	if err != nil && *err != nil {
		sp.SetStatus(codes.Error, (*err).Error())
	}
	sp.End()
}
