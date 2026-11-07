// Package tools is the executor-side tool registry. Risk classes come from
// this registry (operator-owned configuration), never from model output or
// from an MCP server's self-description (§4.6, §16.3).
package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go-sre-harness/internal/domain"
)

type Descriptor struct {
	Name        string           `json:"name"`
	Version     string           `json:"version"`
	Risk        domain.RiskClass `json:"risk"`
	Description string           `json:"description"`
	// InputSchema is the JSON Schema shown to the model and enforced by the
	// gateway before the policy gate (§31.3).
	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	// TargetArg names the argument identifying the operated resource; it is
	// bound into the ActionIntent so an approval cannot be reused elsewhere.
	// Several arguments may be named, comma-separated ("namespace,deployment");
	// their values are joined with "/".
	TargetArg string `json:"target_arg"`
	// Trust is the trust tier of the tool's output (§17): T2 for first-party
	// systems, T3 for telemetry, T4 for MCP and free text. Operator-owned.
	Trust string `json:"trust,omitempty"`
	// Source names where the tool comes from (native, mcp:<server>, remote:<name>).
	Source string `json:"source,omitempty"`
}

// CapabilityRef is the stable graph reference for a tool descriptor.
func CapabilityRef(d Descriptor) string {
	version := d.Version
	if version == "" {
		version = "0"
	}
	return "tool:" + d.Name + "@" + version
}

// CallInfo identifies the step attempt a tool call belongs to. Remote
// executors forward it so downstream systems can audit and fence.
type CallInfo struct {
	TenantID          string
	RunID             string
	StepID            string
	AttemptID         string
	FenceToken        int64
	ContextSnapshotID string
	InputRef          string
	Deadline          time.Time
}

type callInfoKey struct{}

func WithCallInfo(ctx context.Context, c CallInfo) context.Context {
	return context.WithValue(ctx, callInfoKey{}, c)
}

func CallInfoFrom(ctx context.Context) (CallInfo, bool) {
	c, ok := ctx.Value(callInfoKey{}).(CallInfo)
	return c, ok
}

type Tool interface {
	Descriptor() Descriptor
	// Invoke executes the tool. idempotencyKey is stable across attempts of
	// the same logical action and must be forwarded to downstream systems.
	Invoke(ctx context.Context, idempotencyKey string, args json.RawMessage) (json.RawMessage, error)
}

type Registry struct {
	mu     sync.RWMutex
	tools  map[string]Tool
	tenant TenantSource
}

// TenantSource supplies tools that exist for one tenant only (MCP servers
// and HTTP tools added at runtime). Process-wide tools always win a name
// clash, so a tenant can never shadow a built-in tool.
type TenantSource interface {
	TenantTools(tenantID string) []Tool
}

// SetTenantSource installs the tenant overlay.
func (r *Registry) SetTenantSource(s TenantSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenant = s
}

// LookupFor resolves a tool for a tenant: process-wide tools first, then
// the tenant's own tools.
func (r *Registry) LookupFor(tenantID, name string) (Tool, bool) {
	if t, ok := r.Lookup(name); ok {
		return t, true
	}
	r.mu.RLock()
	src := r.tenant
	r.mu.RUnlock()
	if src == nil || tenantID == "" {
		return nil, false
	}
	for _, t := range src.TenantTools(tenantID) {
		if t.Descriptor().Name == name {
			return t, true
		}
	}
	return nil, false
}

// DescriptorsFor lists process-wide and tenant tools, sorted by name.
func (r *Registry) DescriptorsFor(tenantID string) []Descriptor {
	out := r.Descriptors()
	r.mu.RLock()
	src := r.tenant
	r.mu.RUnlock()
	if src == nil || tenantID == "" {
		return out
	}
	seen := make(map[string]bool, len(out))
	for _, d := range out {
		seen[d.Name] = true
	}
	for _, t := range src.TenantTools(tenantID) {
		if d := t.Descriptor(); !seen[d.Name] {
			seen[d.Name] = true
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func NewRegistry(ts ...Tool) *Registry {
	r := &Registry{tools: map[string]Tool{}}
	for _, t := range ts {
		r.Register(t)
	}
	return r
}

func (r *Registry) Register(t Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[t.Descriptor().Name] = t
}

func (r *Registry) Lookup(name string) (Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

func (r *Registry) Descriptors() []Descriptor {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Descriptor, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t.Descriptor())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Target extracts the target value named by d.TargetArg from args.
func Target(d Descriptor, args json.RawMessage) string {
	if d.TargetArg == "" {
		return "*"
	}
	var m map[string]any
	if json.Unmarshal(args, &m) != nil {
		return ""
	}
	var parts []string
	for _, k := range strings.Split(d.TargetArg, ",") {
		v, ok := m[strings.TrimSpace(k)]
		if !ok {
			return ""
		}
		parts = append(parts, fmt.Sprint(v))
	}
	return strings.Join(parts, "/")
}

// Func adapts a function into a Tool (used by demos and tests).
type Func struct {
	Desc  Descriptor
	Fn    func(ctx context.Context, idempotencyKey string, args json.RawMessage) (json.RawMessage, error)
	calls atomic.Int64
}

func (f *Func) Descriptor() Descriptor { return f.Desc }
func (f *Func) Invoke(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
	f.calls.Add(1)
	return f.Fn(ctx, key, args)
}

// Calls reports how many times the tool body ran (for side-effect tests).
func (f *Func) Calls() int64 { return f.calls.Load() }

// DemoTools returns simulated SRE tools. They touch no real system:
// demo.metrics is read-only (R0); demo.restart pretends to restart a service
// and is classified R2 so it always goes through human approval.
func DemoTools() []Tool {
	metrics := &Func{
		Desc: Descriptor{Name: "demo.metrics", Version: "1", Risk: domain.RiskR0, TargetArg: "service",
			Description: "Read simulated latency/error/connection-pool metrics for a service.", InputSchema: serviceSchema},
		Fn: func(_ context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
			var in struct {
				Service string `json:"service"`
			}
			if err := json.Unmarshal(args, &in); err != nil || in.Service == "" {
				return nil, fmt.Errorf("demo.metrics: service is required")
			}
			return json.Marshal(map[string]any{
				"service": in.Service, "p99_latency_ms": 2400, "error_rate": 0.07,
				"db_pool_active": 100, "db_pool_max": 100, "db_pool_wait_ms": 1800,
				"cpu_utilization": 0.41, "source": "simulated",
			})
		},
	}
	restart := &Func{
		Desc: Descriptor{Name: "demo.restart", Version: "1", Risk: domain.RiskR2, TargetArg: "service",
			Description: "Simulated rolling restart of a service (no real effect).", InputSchema: serviceSchema},
		Fn: func(_ context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
			var in struct {
				Service string `json:"service"`
			}
			_ = json.Unmarshal(args, &in)
			return json.Marshal(map[string]any{
				"service": in.Service, "operation_id": "sim-" + key[:min(12, len(key))], "result": "simulated restart completed",
			})
		},
	}
	return []Tool{metrics, restart}
}

var serviceSchema = json.RawMessage(`{"type":"object","properties":{"service":{"type":"string","description":"Service name, e.g. checkout"}},"required":["service"],"additionalProperties":false}`)

// SchemaSetHash identifies the exact tool schema set shown to a model (§33.2).
func SchemaSetHash(ds []Descriptor) string {
	h := sha256.New()
	for _, d := range ds {
		b, _ := json.Marshal([]any{d.Name, d.Version, string(d.Risk), d.Description, json.RawMessage(nonEmpty(d.InputSchema))})
		h.Write(b)
		h.Write([]byte{'\n'})
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func nonEmpty(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return json.RawMessage(`{}`)
	}
	return b
}
