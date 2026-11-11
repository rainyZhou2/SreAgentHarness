package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// Recipe is a semantic read tool: a named set of queries about one subject
// (design §5.4). The agent names a subject; which sources answer and with
// which expressions is operator configuration.
type Recipe struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Plan        []PlanItem `json:"plan"`
}

// DefaultRecipes are the built-in semantic tools. Each item names a query
// template; a source answers the item when the operator defined a template
// of that name on it. Items no source answers are reported as
// source_missing — the tool never pretends a signal was checked.
func DefaultRecipes() []Recipe {
	m, l, t, c := SignalMetrics, SignalLogs, SignalTraces, SignalChange
	hour := cpkit.Duration(time.Hour)
	return []Recipe{
		{Name: "sre.service_health", Description: "Service health from SLIs: error rate, latency, traffic and saturation, with recent changes. Read-only.",
			Plan: []PlanItem{{Role: "error_rate", Signal: m, QueryRef: "error_rate", Required: true},
				{Role: "latency_p99", Signal: m, QueryRef: "latency_p99"}, {Role: "request_rate", Signal: m, QueryRef: "request_rate"},
				{Role: "saturation", Signal: m, QueryRef: "saturation"}, {Role: "recent_changes", Signal: c, Window: hour}}},
		{Name: "sre.latency_diagnose", Description: "Latency breakdown: P99 by route, slow traces and the changes that preceded them. Read-only.",
			Plan: []PlanItem{{Role: "latency_p99", Signal: m, QueryRef: "latency_p99", Required: true},
				{Role: "latency_by_route", Signal: m, QueryRef: "latency_by_route"}, {Role: "slow_traces", Signal: t, QueryRef: "slow"},
				{Role: "recent_changes", Signal: c, Window: hour}}},
		{Name: "sre.error_diagnose", Description: "Error rate with the most frequent error log shapes and sample references. Read-only.",
			Plan: []PlanItem{{Role: "error_rate", Signal: m, QueryRef: "error_rate", Required: true},
				{Role: "error_logs", Signal: l, QueryRef: "errors"}, {Role: "error_traces", Signal: t, QueryRef: "errors"}}},
		{Name: "sre.dependency_health", Description: "Health of the dependencies a service calls: failing edges and their error rates. Read-only.",
			Plan: []PlanItem{{Role: "dependency_errors", Signal: m, QueryRef: "dependency_error_rate", Required: true},
				{Role: "dependency_latency", Signal: m, QueryRef: "dependency_latency"}, {Role: "failing_traces", Signal: t, QueryRef: "errors"}}},
		{Name: "sre.gpu_health", Description: "GPU health: Xid errors, ECC errors, NVLink errors, temperature and utilization. Read-only.",
			Plan: []PlanItem{{Role: "xid_errors", Signal: m, QueryRef: "gpu_xid", Required: true},
				{Role: "ecc_errors", Signal: m, QueryRef: "gpu_ecc"}, {Role: "nvlink_errors", Signal: m, QueryRef: "gpu_nvlink"},
				{Role: "temperature", Signal: m, QueryRef: "gpu_temperature"}, {Role: "utilization", Signal: m, QueryRef: "gpu_utilization"}}},
		{Name: "sre.inference_health", Description: "Inference SLO signals: TTFT, TPOT, end-to-end latency, queue, KV cache and GPU load. Read-only.",
			Plan: []PlanItem{{Role: "ttft_p95", Signal: m, QueryRef: "ttft_p95", Required: true},
				{Role: "tpot_p95", Signal: m, QueryRef: "tpot_p95"}, {Role: "e2e_p95", Signal: m, QueryRef: "e2e_p95"},
				{Role: "queue_depth", Signal: m, QueryRef: "queue_depth"}, {Role: "kv_cache_usage", Signal: m, QueryRef: "kv_cache_usage"},
				{Role: "gpu_utilization", Signal: m, QueryRef: "gpu_utilization"}}},
	}
}

// WorkScope is what the server knows about the Work a run belongs to.
type WorkScope struct {
	WorkID    string
	Subjects  []string
	DataClass string
}

// Toolset exposes the broker to agents as read-only tools.
type Toolset struct {
	Broker *Broker
	// Scope resolves the Work of a run (ok=false: the run has no Work and
	// only the source scope applies).
	Scope func(ctx context.Context, tenantID, runID string) (WorkScope, bool)
	// Sink records usable results in the Work ledger.
	Sink    EvidenceSink
	Recipes []Recipe
	// Context adds what the world knows about the subject (owner, current
	// release, fact freshness) to a recipe's output.
	Context func(ctx context.Context, tenantID, subject string) any
	// MaxDataClass is the most sensitive class tools may hand to a model.
	MaxDataClass string
}

const subjectSchema = `{"type":"object","properties":{"subject":{"type":"string","description":"resource ref, e.g. service://prod/search-api"},` +
	`"window":{"type":"string","description":"how far back to look, e.g. 30m (default 15m)"}},"required":["subject"],"additionalProperties":false}`

// caller builds the server-side identity of a tool call. The tenant and the
// Work come from the kernel's call info; no argument can set them.
func (t *Toolset) caller(ctx context.Context) (Caller, error) {
	ci, ok := tools.CallInfoFrom(ctx)
	if !ok || ci.TenantID == "" {
		return Caller{}, fmt.Errorf("observability tools need an authenticated run context")
	}
	c := Caller{RC: cpkit.RequestContext{TenantID: ci.TenantID, RunID: ci.RunID, PrincipalType: "agent",
		DataClass: firstNonEmpty(t.MaxDataClass, "internal")}}
	if t.Scope != nil && ci.RunID != "" {
		if ws, ok := t.Scope(ctx, ci.TenantID, ci.RunID); ok {
			c.RC.WorkID, c.Subjects = ws.WorkID, ws.Subjects
			// The stricter of the deployment limit and the Work's class.
			if ws.DataClass != "" && cpkit.DataClassRank(ws.DataClass) > cpkit.DataClassRank(c.RC.DataClass) {
				c.RC.DataClass = ws.DataClass
			}
		}
	}
	return c, nil
}

type subjectArgs struct {
	Subject string         `json:"subject"`
	Window  cpkit.Duration `json:"window"`
}

func (t *Toolset) recipe(r Recipe) tools.Tool {
	return &tools.Func{Desc: tools.Descriptor{Name: r.Name, Version: "1", Risk: domain.RiskR0, Trust: "T3", Source: "native",
		TargetArg: "subject", Description: r.Description, InputSchema: json.RawMessage(subjectSchema)},
		Fn: func(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
			var a subjectArgs
			if err := json.Unmarshal(raw, &a); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			subject, err := cpkit.NormalizeRef(a.Subject)
			if err != nil {
				return nil, fmt.Errorf("subject must be a resource ref like service://prod/search-api")
			}
			c, err := t.caller(ctx)
			if err != nil {
				return nil, err
			}
			plan := append([]PlanItem(nil), r.Plan...)
			if a.Window > 0 {
				for i := range plan {
					if plan[i].Window == 0 {
						plan[i].Window = a.Window
					}
				}
			}
			b := t.Broker.Collect(ctx, c, subject, plan, t.Sink)
			out := map[string]any{"bundle": b, "healthy_claim_supported": b.Healthy(),
				"note": "Cite data_ref/evidence_ref, not the summaries. Items that are empty, stale, denied or unavailable are unknown, not healthy."}
			if t.Context != nil {
				out["world"] = t.Context(ctx, c.RC.TenantID, subject)
			}
			return json.Marshal(out)
		}}
}

// Tools returns the recipes plus obs.sources (what can be asked) and
// obs.query (one named query on one source).
func (t *Toolset) Tools() []tools.Tool {
	out := make([]tools.Tool, 0, len(t.Recipes)+2)
	for _, r := range t.Recipes {
		out = append(out, t.recipe(r))
	}
	out = append(out, &tools.Func{Desc: tools.Descriptor{Name: "obs.sources", Version: "1", Risk: domain.RiskR0, Trust: "T2",
		Source: "native", Description: "List the telemetry sources and named queries available for a subject (no endpoints, no credentials).",
		InputSchema: json.RawMessage(subjectSchema)},
		Fn: func(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
			var a subjectArgs
			_ = json.Unmarshal(raw, &a)
			c, err := t.caller(ctx)
			if err != nil {
				return nil, err
			}
			subject, _ := cpkit.NormalizeRef(a.Subject)
			srcs, err := t.Broker.Catalog.ListSources(ctx, c.RC.TenantID, Filter{UsableOnly: true})
			if err != nil {
				return nil, err
			}
			type row struct {
				ID      string   `json:"id"`
				Kind    Signal   `json:"kind"`
				Backend string   `json:"backend"`
				State   string   `json:"state"`
				Queries []string `json:"queries"`
				Healthy *bool    `json:"healthy,omitempty"`
			}
			var rows []row
			for _, s := range srcs {
				if subject != "" && !s.Allows(subject) {
					continue
				}
				r := row{ID: s.ID, Kind: s.Kind, Backend: s.Backend, State: s.State}
				for name := range s.Queries {
					r.Queries = append(r.Queries, name)
				}
				sortStrings(r.Queries)
				if s.Health != nil {
					ok := s.Health.OK
					r.Healthy = &ok
				}
				rows = append(rows, r)
			}
			return json.Marshal(map[string]any{"sources": rows})
		}})
	out = append(out, &tools.Func{Desc: tools.Descriptor{Name: "obs.query", Version: "1", Risk: domain.RiskR0, Trust: "T3",
		Source: "native", TargetArg: "subject",
		Description: "Run one named, operator-managed query on one source for a subject. Returns a summary and a data_ref to cite.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"source_id":{"type":"string"},"query_ref":{"type":"string"},` +
			`"subject":{"type":"string"},"window":{"type":"string"}},"required":["source_id","subject"],"additionalProperties":false}`)},
		Fn: func(ctx context.Context, _ string, raw json.RawMessage) (json.RawMessage, error) {
			var a struct {
				subjectArgs
				SourceID string `json:"source_id"`
				QueryRef string `json:"query_ref"`
			}
			if err := json.Unmarshal(raw, &a); err != nil {
				return nil, fmt.Errorf("invalid arguments: %w", err)
			}
			c, err := t.caller(ctx)
			if err != nil {
				return nil, err
			}
			window := a.Window.D()
			if window <= 0 {
				window = 15 * time.Minute
			}
			now := t.Broker.Clock.Now()
			res := t.Broker.Query(ctx, c, Query{SourceID: a.SourceID, SubjectRef: a.Subject, QueryRef: a.QueryRef,
				Start: now.Add(-window), End: now})
			out := map[string]any{"result": res}
			if t.Sink != nil && c.RC.WorkID != "" {
				if rec, ok := EvidenceOf(c.RC.TenantID, c.RC.WorkID, res, ""); ok {
					if stored, _, err := t.Sink.AddEvidence(ctx, rec); err == nil {
						out["evidence_ref"] = "evidence:" + stored.ID
					}
				}
			}
			return json.Marshal(out)
		}})
	return out
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
