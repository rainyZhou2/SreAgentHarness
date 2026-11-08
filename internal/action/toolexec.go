package action

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"go-sre-harness/internal/tools"
)

// ToolExecutor adapts a registered side-effecting tool to an action, so
// actions run through the same tool implementations (MCP, gRPC, Kubernetes,
// HTTP) as kernel tool steps instead of a second command bus (ADR-004).
//
// The risk class comes from the tool's registry descriptor. The target is
// written into the tool's TargetArg (the last ref segment, or the
// "namespace/name" pair for multi-argument targets), and the idempotency key
// is passed to Invoke like any kernel tool call.
type ToolExecutor struct {
	Tool tools.Tool
	// Desc overrides descriptor metadata (resource kinds, facts,
	// compensation); Name defaults to the tool name, Risk is always the
	// tool's.
	Desc Descriptor
	// InspectTool is an optional read-only tool that reports
	// {"applied": bool} for the same args.
	InspectTool tools.Tool
}

// Descriptor merges the tool descriptor with the action metadata.
func (t *ToolExecutor) Descriptor() Descriptor {
	td := t.Tool.Descriptor()
	d := t.Desc
	if d.Name == "" {
		d.Name = td.Name
	}
	if d.Version == "" {
		d.Version = td.Version
	}
	d.Risk = td.Risk // operator-owned risk; an action cannot claim lower
	if d.Description == "" {
		d.Description = td.Description
	}
	if len(d.InputSchema) == 0 {
		d.InputSchema = td.InputSchema
	}
	if d.IdempotencyMode == "" {
		d.IdempotencyMode = IdemNone
		if t.InspectTool != nil {
			d.IdempotencyMode = IdemQueryBeforeRetry
		}
	}
	return d
}

// Preflight only validates that args can be built; deeper checks belong to
// the world preconditions and the gate.
func (t *ToolExecutor) Preflight(_ context.Context, p Plan) (PreflightResult, error) {
	if _, err := t.args(p); err != nil {
		return PreflightResult{Allowed: false, Reasons: []string{err.Error()}}, nil
	}
	if in := t.Tool.Descriptor().InputSchema; len(in) > 0 && !json.Valid(in) {
		return PreflightResult{Allowed: false, Reasons: []string{"tool input schema is invalid"}}, nil
	}
	return PreflightResult{Allowed: true}, nil
}

// Execute invokes the tool with the plan's idempotency key.
func (t *ToolExecutor) Execute(ctx context.Context, p Plan, key string) (json.RawMessage, error) {
	args, err := t.args(p)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotApplied, err)
	}
	ctx = tools.WithCallInfo(ctx, tools.CallInfo{TenantID: p.TenantID, RunID: p.RunID})
	return t.Tool.Invoke(ctx, key, args)
}

// Inspect calls the inspect tool when configured.
func (t *ToolExecutor) Inspect(ctx context.Context, p Plan) (InspectResult, error) {
	if t.InspectTool == nil {
		return InspectResult{}, ErrUnsupported
	}
	args, err := t.args(p)
	if err != nil {
		return InspectResult{}, err
	}
	out, err := t.InspectTool.Invoke(ctx, "inspect:"+p.EffectKey, args)
	if err != nil {
		return InspectResult{}, err
	}
	var v struct {
		Applied *bool `json:"applied"`
	}
	_ = json.Unmarshal(out, &v)
	return InspectResult{Applied: v.Applied, Detail: out}, nil
}

// args fills the tool target arguments from the plan target (the tail of
// the ref) unless the args already name them consistently.
func (t *ToolExecutor) args(p Plan) (json.RawMessage, error) {
	m := map[string]any{}
	if len(p.Args) > 0 {
		if err := json.Unmarshal(p.Args, &m); err != nil {
			return nil, fmt.Errorf("args must be an object: %w", err)
		}
	}
	targetArg := t.Tool.Descriptor().TargetArg
	if targetArg != "" {
		names := strings.Split(targetArg, ",")
		_, path, _ := strings.Cut(p.TargetRef, "://")
		segs := strings.Split(path, "/")
		if len(segs) < len(names) {
			return nil, fmt.Errorf("target %s does not provide %d target arguments", p.TargetRef, len(names))
		}
		tail := segs[len(segs)-len(names):]
		for i, n := range names {
			n = strings.TrimSpace(n)
			if v, ok := m[n]; ok && fmt.Sprint(v) != tail[i] {
				return nil, fmt.Errorf("argument %s=%v contradicts the target %s", n, v, p.TargetRef)
			}
			m[n] = tail[i]
		}
	}
	return json.Marshal(m)
}
