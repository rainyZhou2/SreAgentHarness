package workengine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/provenance"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
	"go-sre-harness/migrations"
)

// ObservabilityConfig is the "work.observability" section: the operator's
// endpoint and credential records, the sources to register and the limits
// of the Query Broker. Endpoints and credentials exist only here; a source
// (from this file or the API) refers to them by name.
type ObservabilityConfig struct {
	Endpoints   map[string]observability.Endpoint `json:"endpoints,omitempty"`
	Credentials observability.Credentials         `json:"credentials,omitempty"`
	// Sources are registered at start. A source that differs from the
	// stored one becomes a new revision (actor "config"); sources created
	// through the API and not listed here are left alone.
	Sources []observability.Source `json:"sources,omitempty"`
	Limits  observability.Limits   `json:"limits,omitempty"`
	// Recipes add to (or, by name, replace) the built-in semantic tools.
	Recipes []observability.Recipe `json:"recipes,omitempty"`
	// Required lists "<tenant>/<source id>" that must be healthy before a
	// production deployment may dispatch actions.
	Required []string `json:"required,omitempty"`
	// ProbeEvery is the health probe interval (default 1m; "0" disables).
	ProbeEvery *cpkit.Duration `json:"probe_every,omitempty"`
}

// SchedulingConfig is the "work.scheduling" section (design §6.3).
type SchedulingConfig struct {
	// Aging raises a waiting Work's effective priority by one per interval,
	// up to MaxBoost (default 2m and 20; "0" disables aging).
	Aging    *cpkit.Duration `json:"aging,omitempty"`
	MaxBoost int             `json:"max_boost,omitempty"`
	// TenantWeights enables weighted fair queuing between tenants.
	TenantWeights map[string]int `json:"tenant_weights,omitempty"`
	FairTenants   *bool          `json:"fair_tenants,omitempty"`
}

func (c *SchedulingConfig) fairness() cpsql.Fairness {
	f := cpsql.Fairness{Aging: 2 * time.Minute, MaxBoost: 20, FairTenants: true}
	if c == nil {
		return f
	}
	if c.Aging != nil {
		f.Aging = c.Aging.D()
	}
	if c.MaxBoost > 0 {
		f.MaxBoost = min(c.MaxBoost, 200)
	}
	f.TenantWeights = c.TenantWeights
	if c.FairTenants != nil {
		f.FairTenants = *c.FairTenants
	}
	return f
}

// Deployment describes the process for the governance gate (the parts the
// engine cannot see for itself).
type Deployment struct {
	Tier          governance.Tier
	BuildSHA      string
	StoreKind     string // postgres | sqlite
	AuthMode      string // dev | token | oidc
	ArtifactStore string
	PolicyCustom  bool
	PolicyVersion string
	AuditSink     bool
	// ConfigDigest identifies the effective configuration.
	ConfigDigest string
}

// govFlags converts the rollout switches for the gate.
func govFlags(f Flags) governance.Flags {
	return governance.Flags{Engine: f.Engine, Shadow: f.Shadow, ActionV2: f.ActionV2, ActionDryRun: f.ActionDryRun,
		RequireKnownTarget: f.RequireKnownTarget, RequireDistinctApprover: f.RequireDistinctApprover,
		InlineTemplates: f.InlineTemplates, ActionAllowlist: f.ActionAllowlist, ActionScope: f.ActionScope,
		Preauthorized: f.Preauthorized, ModelMaxDataClass: f.ModelMaxDataClass}
}

// ExpectedSchema is the newest migration compiled into this binary.
func ExpectedSchema() string {
	v := migrations.Versions()
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}

// GateInput assembles what the governance gate judges.
func GateInput(d Deployment, f Flags, schema string, capabilities []string, actions []action.Descriptor, metricSources []string,
	sources []governance.SourceStatus, now time.Time) governance.Input {
	in := governance.Input{Tier: d.Tier, Flags: govFlags(f), BuildSHA: d.BuildSHA, SchemaVersion: schema,
		ExpectedSchema: ExpectedSchema(), Store: d.StoreKind, AuthMode: d.AuthMode, ArtifactStore: d.ArtifactStore,
		PolicyVersion: d.PolicyVersion, PolicyCustom: d.PolicyCustom, AuditSink: d.AuditSink, Capabilities: capabilities,
		MetricSources: metricSources, DataSources: sources, ConfigDigest: d.ConfigDigest, Now: now}
	in.PolicyDigest = governance.Digest(struct {
		Version string
		Custom  bool
	}{d.PolicyVersion, d.PolicyCustom})
	for _, a := range actions {
		in.Actions = append(in.Actions, governance.ActionInfo{Name: a.Name, Risk: string(a.Risk), IdempotencyMode: a.IdempotencyMode,
			HasVersionFact: a.VersionFact != ""})
	}
	sort.Slice(in.Actions, func(i, j int) bool { return in.Actions[i].Name < in.Actions[j].Name })
	return in
}

// sourceStatuses reads the configured sources for the gate. A source the
// catalog cannot return counts as disabled: the gate fails closed.
func sourceStatuses(ctx context.Context, store *cpsql.Store, cfg *ObservabilityConfig) []governance.SourceStatus {
	if cfg == nil {
		return nil
	}
	required := map[string]bool{}
	for _, r := range cfg.Required {
		required[r] = true
	}
	var out []governance.SourceStatus
	seen := map[string]bool{}
	add := func(tenant, id string) {
		key := tenant + "/" + id
		if seen[key] {
			return
		}
		seen[key] = true
		st := governance.SourceStatus{ID: key, State: observability.StateDisabled, Required: required[key]}
		if src, err := store.GetSource(ctx, tenant, id); err == nil {
			st.Kind, st.State = string(src.Kind), src.State
			if src.Health != nil {
				st.Probed, st.Healthy = true, src.Health.OK
			}
		}
		out = append(out, st)
	}
	for _, s := range cfg.Sources {
		add(s.TenantID, s.ID)
	}
	for r := range required {
		if tenant, id, ok := strings.Cut(r, "/"); ok {
			add(tenant, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// seedSources registers the configured sources. Differences become a new
// revision, so a config rollout is as auditable as an API change.
func seedSources(ctx context.Context, store *cpsql.Store, cfg *ObservabilityConfig) error {
	for _, src := range cfg.Sources {
		if src.TenantID == "" {
			return fmt.Errorf("work.observability.sources.%s: tenant_id is required", src.ID)
		}
		if _, ok := cfg.Endpoints[src.EndpointRef]; !ok && src.Backend != "ledger" {
			return fmt.Errorf("work.observability.sources.%s: endpoint_ref %q is not declared", src.ID, src.EndpointRef)
		}
		if src.CredentialID != "" {
			if _, ok := cfg.Credentials[src.CredentialID]; !ok {
				return fmt.Errorf("work.observability.sources.%s: credential_id %q is not declared", src.ID, src.CredentialID)
			}
		}
		if src.State == "" {
			src.State = observability.StateActive
		}
		if err := src.Normalize(); err != nil {
			return fmt.Errorf("work.observability.sources.%s: %w", src.ID, err)
		}
		cur, err := store.GetSource(ctx, src.TenantID, src.ID)
		switch {
		case errors.Is(err, observability.ErrNotFound):
			if _, err := store.PutSource(ctx, src, 0, "config", "registered from configuration"); err != nil &&
				!errors.Is(err, observability.ErrVersionConflict) {
				return err
			}
		case err != nil:
			return err
		default:
			a, b := cur, src
			a.Version, a.Health, a.UpdatedAt, a.State = 0, nil, time.Time{}, ""
			b.Version, b.Health, b.UpdatedAt, b.State = 0, nil, time.Time{}, ""
			// A degraded source stays degraded until a probe clears it.
			if cpkit.MustHash(a) == cpkit.MustHash(b) {
				continue
			}
			if cur.State == observability.StateDegraded && src.State == observability.StateActive {
				src.State = observability.StateDegraded
			}
			if _, err := store.PutSource(ctx, src, cur.Version, "config", "configuration changed"); err != nil &&
				!errors.Is(err, observability.ErrVersionConflict) {
				return err
			}
		}
	}
	return nil
}

// brokerMetrics adapts the metrics registry to the broker's observer.
type brokerMetrics struct {
	queries  obs.Counter
	duration obs.Histogram
	denied   obs.Counter
	breaker  obs.Counter
}

// NewBrokerMetrics registers the Query Broker metrics.
func NewBrokerMetrics(reg *obs.Registry) *brokerMetrics {
	return &brokerMetrics{
		queries: reg.Counter("harness_observability_query_total",
			"Broker queries by signal, backend, result status and cache use.", "signal", "backend", "status", "cached"),
		duration: reg.Histogram("harness_observability_query_seconds", "Backend query latency.",
			[]float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}, "signal", "backend"),
		denied: reg.Counter("harness_observability_denied_total",
			"Queries refused before reaching a backend, by reason (scope, work_scope, raw_query, budget, ...).", "reason"),
		breaker: reg.Counter("harness_observability_breaker_total", "Source circuit breaker transitions.", "backend", "state"),
	}
}

func (m *brokerMetrics) Query(signal, backend, status string, d time.Duration, cached bool) {
	c := "false"
	if cached {
		c = "true"
	}
	m.queries.Inc(signal, backend, status, c)
	if !cached {
		m.duration.Observe(d.Seconds(), signal, backend)
	}
}
func (m *brokerMetrics) Denied(reason string)          { m.denied.Inc(reason) }
func (m *brokerMetrics) Breaker(backend, state string) { m.breaker.Inc(backend, state) }

// wireObservability builds the Query Broker and registers its read tools
// with the kernel's tool registry (so stage runs can call them).
func (e *Engine) wireObservability(ctx context.Context, o Options) error {
	cfg := o.Config.Observability
	if cfg == nil {
		cfg = &ObservabilityConfig{}
	}
	b := &observability.Broker{Catalog: o.Store, Audit: o.Store, Endpoints: cfg.Endpoints, Credentials: cfg.Credentials,
		Artifacts: o.Server.Artifacts, Clock: o.Store.Clock, Limits: cfg.Limits, Log: e.log.With("component", "observability"),
		Local: map[string]observability.Factory{"ledger": impact.Ledger(o.Store)}}
	if b.Credentials == nil {
		b.Credentials = observability.Credentials{}
	}
	if o.Metrics != nil {
		b.Observer = NewBrokerMetrics(o.Metrics)
	}
	if err := seedSources(ctx, o.Store, cfg); err != nil {
		return err
	}
	e.Broker, e.API.Broker, e.obsCfg = b, b, cfg
	if o.Server.Tools == nil {
		return nil
	}
	recipes := observability.DefaultRecipes()
	for _, r := range cfg.Recipes {
		replaced := false
		for i := range recipes {
			if recipes[i].Name == r.Name {
				recipes[i], replaced = r, true
			}
		}
		if !replaced {
			recipes = append(recipes, r)
		}
	}
	maxClass := o.Flags.ModelMaxDataClass
	if maxClass == "" {
		maxClass = "confidential"
	}
	quality := provenance.Quality{Health: o.Store, Authority: e.authority, Clock: o.Store.Clock}
	ts := &observability.Toolset{Broker: b, Sink: o.Store, Recipes: recipes, MaxDataClass: maxClass,
		Scope: func(ctx context.Context, tenantID, runID string) (observability.WorkScope, bool) {
			link, ok, err := o.Store.WorkForRun(ctx, tenantID, runID)
			if err != nil || !ok {
				return observability.WorkScope{}, false
			}
			w, err := o.Store.GetWork(ctx, tenantID, link.WorkID)
			if err != nil {
				return observability.WorkScope{}, false
			}
			return observability.WorkScope{WorkID: w.ID, Subjects: w.Goal.SubjectRefs, DataClass: w.Goal.Constraints.DataClass}, true
		},
		Context: func(ctx context.Context, tenantID, subject string) any {
			snap, err := e.World.Snapshot(ctx, world.SnapshotRequest{TenantID: tenantID, Subjects: []string{subject}, MaxAge: 10 * time.Minute})
			if err != nil {
				return map[string]any{"available": false}
			}
			facts := map[string]string{}
			for _, f := range snap.Facts {
				if f.SubjectRef == subject && len(facts) < 24 {
					facts[f.Predicate] = f.Value
				}
			}
			return map[string]any{"available": true, "facts": facts, "quality": quality.Summarize(ctx, tenantID, &snap)}
		}}
	all := append(ts.Tools(), changeCorrelationTool(o.Store, e.World, o.Store.Clock))
	for _, t := range all {
		if _, dup := o.Server.Tools.Lookup(t.Descriptor().Name); dup {
			return fmt.Errorf("work engine: tool %q is already registered", t.Descriptor().Name)
		}
	}
	for _, t := range all {
		o.Server.Tools.Register(t)
	}
	return nil
}

// probeSources keeps source health current (and with it the governance
// attestation and the routing of semantic tools).
func (e *Engine) probeSources(ctx context.Context) {
	every := time.Minute
	if e.obsCfg != nil && e.obsCfg.ProbeEvery != nil {
		every = e.obsCfg.ProbeEvery.D()
	}
	if every <= 0 || e.Broker == nil {
		return
	}
	tenants := map[string]bool{}
	if e.obsCfg != nil {
		for _, s := range e.obsCfg.Sources {
			tenants[s.TenantID] = true
		}
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		for tenant := range tenants {
			pctx, cancel := context.WithTimeout(ctx, every)
			if _, err := e.Broker.ProbeAll(pctx, tenant); err != nil && ctx.Err() == nil {
				e.log.Warn("source probe failed", "tenant", tenant, "err", err)
			}
			cancel()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Attestation evaluates the governance gate against the current state.
func (e *Engine) Attestation(ctx context.Context) governance.Attestation {
	schema := ExpectedSchema()
	if e.schema != nil {
		if v, err := e.schema(ctx); err == nil {
			schema = v
		}
	}
	in := GateInput(e.deployment, e.requested, schema, e.capabilities, e.actionDescs, e.metricNames,
		sourceStatuses(ctx, e.store, e.obsCfg), e.store.Clock.Now())
	return governance.Evaluate(in)
}

// eventSources validates the operator's event source policies.
func eventSources(m map[string]eventing.SourcePolicy) (map[string]eventing.SourcePolicy, error) {
	for name, p := range m {
		if name == "" || len(name) > 128 {
			return nil, fmt.Errorf("work.event_sources: invalid source name %q", name)
		}
		if p.MaxLateness < 0 {
			return nil, fmt.Errorf("work.event_sources.%s: max_lateness must not be negative", name)
		}
	}
	return m, nil
}

// neighbors adapts the world's neighborhood to the ingestor.
func neighbors(ws *world.Service) func(ctx context.Context, tenantID, subject string) ([]string, error) {
	return func(ctx context.Context, tenantID, subject string) ([]string, error) {
		m, err := ws.Neighbors(ctx, tenantID, subject, 1)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(m))
		for ref, d := range m {
			if d == 1 {
				out = append(out, ref)
			}
		}
		sort.Strings(out)
		return out, nil
	}
}

// changeCorrelationTool is sre.change_correlation: the changes around a
// symptom, ranked, with the checks that would confirm or dismiss each.
func changeCorrelationTool(store impact.Store, ws *world.Service, clock cpkit.Clock) tools.Tool {
	return impact.Tool(store, ws, clock)
}

// RunWaker returns the event sink that wakes Works when their runs end.
func (e *Engine) RunWaker() *orchestration.RunWaker {
	return &orchestration.RunWaker{Store: e.store, Clock: e.store.Clock, Log: e.log, Metrics: e.metrics}
}
