package policy

import (
	"context"
	"encoding/json"
	"time"

	"go-sre-harness/internal/tools"
)

// Request is the full context of a tool intent. Gates that only need the
// descriptor and target implement Gate; gates that also look at arguments,
// time, tenant or history implement ContextGate.
type Request struct {
	TenantID string
	RunID    string
	// Tool is the effective descriptor (profile risk overrides applied); nil
	// for an unregistered tool.
	Tool   *tools.Descriptor
	Target string
	Args   json.RawMessage
	// Now is the decision time. Zero means time.Now().
	Now time.Time
}

func (r Request) now() time.Time {
	if r.Now.IsZero() {
		return time.Now()
	}
	return r.Now
}

// withTarget derives the target from the arguments when the caller did not.
func (r Request) withTarget() Request {
	if r.Target == "" && r.Tool != nil && len(r.Args) > 0 {
		r.Target = tools.Target(*r.Tool, r.Args)
	}
	return r
}

// ContextGate is an optional Gate extension that receives the whole request.
type ContextGate interface {
	EvaluateRequest(ctx context.Context, req Request) Decision
}

// EnvironmentResolver is an optional Gate extension: it names the
// environment a call operates on (bound into the ActionIntent).
type EnvironmentResolver interface {
	EnvironmentFor(desc tools.Descriptor, args json.RawMessage) string
}

// EvaluateRequest asks g for a decision, using the richer interface when the
// gate implements it.
func EvaluateRequest(ctx context.Context, g Gate, req Request) Decision {
	req = req.withTarget()
	if cg, ok := g.(ContextGate); ok {
		return cg.EvaluateRequest(ctx, req)
	}
	return g.Evaluate(req.Tool, req.Target)
}
