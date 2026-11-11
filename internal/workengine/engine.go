// Package workengine assembles the v2 control plane (Works, events,
// watches, world state, actions, outcomes) for the team server and the
// local app, behind feature flags:
//
//	HARNESS_WORK_ENGINE_ENABLED  Work API, orchestrator, watches (default off)
//	HARNESS_EVENT_SHADOW_MODE    events are routed and recorded, no Works are
//	                             created from them (default on while rolling out)
//	HARNESS_ACTION_V2_ENABLED    action runtime and action stages (default off)
//
// With the engine off nothing here runs and the v1 run API is unchanged.
package workengine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/api"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/dataguard"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/playbook"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/provenance"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workapi"
	"go-sre-harness/internal/workeval"
	"go-sre-harness/internal/world"
)

// Config is the "work" section of the config file.
type Config struct {
	// Templates add to (or override) the built-in templates.
	Templates []work.Template `json:"templates,omitempty"`
	// CorrelationRules route ingested events to Works (first match wins).
	CorrelationRules []eventing.Rule `json:"correlation_rules,omitempty"`
	// Queues overrides the per-queue limits (concurrency, batch, model runs
	// per tenant) by queue name; unset fields keep their defaults.
	Queues map[string]orchestration.QueueLimit `json:"queues,omitempty"`
	// Pools overrides the resource pools; unset fields keep their defaults,
	// -1 means unlimited.
	Pools *orchestration.PoolLimits `json:"pools,omitempty"`
	// Observability configures the Query Broker: endpoints, credentials,
	// sources and limits.
	Observability *ObservabilityConfig `json:"observability,omitempty"`
	// Scheduling tunes claim fairness (aging, tenant weights).
	Scheduling *SchedulingConfig `json:"scheduling,omitempty"`
	// EventSources declares event sources: allowed types, signing key,
	// lateness.
	EventSources map[string]eventing.SourcePolicy `json:"event_sources,omitempty"`
	// Authority names the authoritative sources per world predicate (adds
	// to and overrides the defaults).
	Authority provenance.Authority `json:"authority,omitempty"`
}

// Flags are the rollout switches.
type Flags struct {
	Engine   bool
	Shadow   bool
	ActionV2 bool
	// RequireDistinctApprover forbids a Work owner to approve its actions.
	RequireDistinctApprover bool
	// RequireKnownTarget rejects action targets the world does not know.
	RequireKnownTarget bool
	ApprovalTTL        time.Duration
	// InlineTemplates allows admins to submit template bodies.
	InlineTemplates bool
	// ModelMaxDataClass is the most sensitive data class sent to the model
	// provider (HARNESS_MODEL_MAX_DATA_CLASS, default confidential).
	ModelMaxDataClass string
	// TriggerMirror copies v1 webhook events into the event inbox for the
	// shadow comparison (HARNESS_TRIGGER_MIRROR, default: on in shadow mode).
	TriggerMirror bool
	// InboxPayloadRetention / InboxRecordRetention bound event_inbox
	// (HARNESS_EVENT_PAYLOAD_RETENTION default 336h, HARNESS_EVENT_RETENTION
	// default 2160h; "0" keeps forever).
	InboxPayloadRetention time.Duration
	InboxRecordRetention  time.Duration
	// EvidencePlanning ranks read-only checks for run stages (RFC §12.4;
	// HARNESS_EVIDENCE_PLANNING, default on).
	EvidencePlanning bool
	// Tier is the deployment tier (HARNESS_DEPLOYMENT_TIER: local, staging,
	// production). It selects how strictly the governance gate judges the
	// other flags; it never turns anything on by itself.
	Tier governance.Tier
	// ActionDryRun is rollout stage G3 (HARNESS_ACTION_DRY_RUN): actions
	// are planned, preflighted and approved, dispatch is simulated.
	ActionDryRun bool
	// ActionAllowlist and ActionScope bound what may be dispatched (stage
	// G4; HARNESS_ACTION_ALLOWLIST, HARNESS_ACTION_SCOPE, comma-separated).
	ActionAllowlist []string
	ActionScope     []string
	// Preauthorized actions may run without a per-action approval for
	// Works with production_writes=preauthorized (stage G5;
	// HARNESS_ACTION_PREAUTHORIZED).
	Preauthorized []string
}

func csv(v string) []string {
	var out []string
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}

// FlagsFromEnv reads the flags.
func FlagsFromEnv(getenv func(string) string) Flags {
	b := func(k string, def bool) bool {
		if v, err := strconv.ParseBool(strings.TrimSpace(getenv(k))); err == nil {
			return v
		}
		return def
	}
	f := Flags{Engine: b("HARNESS_WORK_ENGINE_ENABLED", false), Shadow: b("HARNESS_EVENT_SHADOW_MODE", true),
		ActionV2: b("HARNESS_ACTION_V2_ENABLED", false), RequireDistinctApprover: b("HARNESS_REQUIRE_DISTINCT_APPROVER", false),
		RequireKnownTarget: b("HARNESS_ACTION_REQUIRE_KNOWN_TARGET", false), InlineTemplates: b("HARNESS_WORK_INLINE_TEMPLATES", false),
		ApprovalTTL: 30 * time.Minute, EvidencePlanning: b("HARNESS_EVIDENCE_PLANNING", true)}
	if d, err := time.ParseDuration(getenv("HARNESS_ACTION_APPROVAL_TTL")); err == nil && d > 0 {
		f.ApprovalTTL = d
	}
	f.ModelMaxDataClass = strings.TrimSpace(getenv("HARNESS_MODEL_MAX_DATA_CLASS"))
	// An unknown tier is treated as production: a typo must not relax the gate.
	if t, err := governance.ParseTier(getenv("HARNESS_DEPLOYMENT_TIER")); err == nil {
		f.Tier = t
	} else {
		f.Tier = governance.TierProduction
	}
	f.ActionDryRun = b("HARNESS_ACTION_DRY_RUN", false)
	f.ActionAllowlist, f.ActionScope = csv(getenv("HARNESS_ACTION_ALLOWLIST")), csv(getenv("HARNESS_ACTION_SCOPE"))
	f.Preauthorized = csv(getenv("HARNESS_ACTION_PREAUTHORIZED"))
	f.TriggerMirror = b("HARNESS_TRIGGER_MIRROR", f.Shadow)
	f.InboxPayloadRetention, f.InboxRecordRetention = 14*24*time.Hour, 90*24*time.Hour
	if v := strings.TrimSpace(getenv("HARNESS_EVENT_PAYLOAD_RETENTION")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			f.InboxPayloadRetention = d
		}
	}
	if v := strings.TrimSpace(getenv("HARNESS_EVENT_RETENTION")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			f.InboxRecordRetention = d
		}
	}
	return f
}

// Options assemble an engine.
type Options struct {
	Store  *cpsql.Store
	Server *api.Server // kernel run API used by run stages
	Policy policy.Gate // nil = policy.Default()
	// Extensions contribute action executors and metric sources.
	Extensions []plugin.WorkExtension
	Config     Config
	Flags      Flags
	Metrics    *obs.Registry
	Log        *slog.Logger
	Owner      string
	// Embedder embeds Playbook candidates (default: the hashing embedder;
	// the memory indexer re-embeds for the configured model).
	Embedder memory.Embedder
	// Deployment describes the process for the governance gate.
	Deployment Deployment
	// Schema reads the applied schema version (nil: assumed current).
	Schema func(ctx context.Context) (string, error)
	// Capabilities names the loaded domain capabilities (attestation).
	Capabilities []string
}

// Engine is an assembled control plane.
type Engine struct {
	API        *workapi.API
	Service    *orchestration.Service
	Orch       *orchestration.Orchestrator
	Reconciler *orchestration.Reconciler
	Ingestor   *orchestration.Ingestor
	Actions    *action.Runtime
	World      *world.Service
	// Playbooks proposes Playbook candidates and records outcome feedback
	// (nil when the kernel store keeps no memories).
	Playbooks *playbook.Service
	// Eval scores Works on the Improvement Loop dimensions.
	Eval    *workeval.Evaluator
	runners []plugin.WorkRunner
	log     *slog.Logger
	// Broker is the observability Query Broker.
	Broker *observability.Broker
	// Launch is the attestation the engine started with. Flags are the
	// effective ones: a degraded deployment runs with actions off.
	Launch governance.Attestation
	Flags  Flags

	store        *cpsql.Store
	metrics      *orchestration.Metrics
	obsCfg       *ObservabilityConfig
	authority    provenance.Authority
	deployment   Deployment
	requested    Flags
	schema       func(ctx context.Context) (string, error)
	capabilities []string
	actionDescs  []action.Descriptor
	metricNames  []string
}

// New assembles the engine without starting it.
func New(o Options) (*Engine, error) {
	if o.Store == nil || o.Server == nil {
		return nil, errors.New("workengine: store and server are required")
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	log = log.With("component", "work")
	clock := o.Store.Clock
	var templates []work.Template
	for _, x := range o.Extensions {
		if tc, ok := x.(plugin.TemplateContributor); ok {
			templates = append(templates, tc.WorkTemplates()...)
		}
	}
	// Operator templates come last: same id overrides the extension's.
	cat, err := orchestration.NewCatalog(append(templates, o.Config.Templates...))
	if err != nil {
		return nil, err
	}
	rules, sources, err := extensionEvents(o.Config.CorrelationRules, o.Extensions)
	if err != nil {
		return nil, err
	}
	router, err := eventing.NewRouter(rules)
	if err != nil {
		return nil, fmt.Errorf("correlation rules: %w", err)
	}
	for _, r := range rules {
		if r.Queue != "" && !work.ValidQueue(work.Queue(r.Queue)) {
			return nil, fmt.Errorf("correlation rule %s: unknown queue %q", r.Name, r.Queue)
		}
	}
	queues, err := orchestration.ValidateQueueLimits(o.Config.Queues)
	if err != nil {
		return nil, err
	}
	pools := orchestration.NewPools(orchestration.MergePoolLimits(orchestration.DefaultPoolLimits(), o.Config.Pools))
	ws := &world.Service{Store: o.Store, Clock: clock, Gate: pools.SnapshotGate()}
	metricSources := outcome.MetricSourceMap{}
	reg := action.NewRegistry()
	var runners []plugin.WorkRunner
	for _, x := range o.Extensions {
		for name, src := range x.MetricSources() {
			if _, dup := metricSources[name]; dup {
				return nil, fmt.Errorf("metric source %q is registered twice", name)
			}
			metricSources[name] = src
		}
		for _, ex := range x.Actions() {
			if err := reg.Register(ex); err != nil {
				return nil, err
			}
		}
		if r, ok := x.(plugin.WorkRunner); ok {
			runners = append(runners, r)
		}
	}
	// The governance gate judges the requested flags before anything that
	// could write is assembled. A refused deployment does not start; a
	// degraded one runs in investigation mode with actions withheld.
	if o.Deployment.Tier == "" {
		o.Deployment.Tier = o.Flags.Tier
	}
	if o.Deployment.Tier == "" {
		o.Deployment.Tier = governance.TierLocal
	}
	if o.Deployment.StoreKind == "" {
		o.Deployment.StoreKind = "postgres"
		if o.Store.Dialect() == cpsql.SQLite {
			o.Deployment.StoreKind = "sqlite"
		}
	}
	requested := o.Flags
	schema := ExpectedSchema()
	if o.Schema != nil {
		if v, err := o.Schema(context.Background()); err == nil {
			schema = v
		}
	}
	names := make([]string, 0, len(metricSources))
	for n := range metricSources {
		names = append(names, n)
	}
	sort.Strings(names)
	if o.Config.Observability != nil {
		if err := seedSources(context.Background(), o.Store, o.Config.Observability); err != nil {
			return nil, err
		}
	}
	att := governance.Evaluate(GateInput(o.Deployment, o.Flags, schema, o.Capabilities, reg.Descriptors(), names,
		sourceStatuses(context.Background(), o.Store, o.Config.Observability), clock.Now()))
	if !att.OK() {
		return nil, fmt.Errorf("deployment gate refused to start (tier %s): %s", att.Tier, strings.Join(att.Reasons, "; "))
	}
	if att.Mode == governance.ModeInvestigation {
		log.Warn("deployment gate: production actions are withheld; running in investigation mode",
			"tier", att.Tier, "requested_stage", att.RequestedStage, "stage", att.Stage, "reasons", att.Reasons)
		o.Flags.ActionV2, o.Flags.ActionDryRun, o.Flags.Preauthorized = false, false, nil
	}
	sources2, err := eventSources(o.Config.EventSources)
	if err != nil {
		return nil, err
	}
	authority := provenance.DefaultAuthority()
	for pred, srcs := range o.Config.Authority {
		authority[pred] = srcs
	}
	o.Store.Fairness = o.Config.Scheduling.fairness()
	ev := &outcome.Evaluator{Metrics: metricSources, States: orchestration.WorldFacts{World: ws}, Events: o.Store}
	var m *orchestration.Metrics
	if o.Metrics != nil {
		m = orchestration.NewMetrics(o.Metrics, o.Store)
		m.RegisterPools(o.Metrics, pools)
	}
	var rt *action.Runtime
	if o.Flags.ActionV2 {
		gate := o.Policy
		if gate == nil {
			gate = policy.Default()
		}
		rt = &action.Runtime{Store: o.Store, Registry: reg, Policy: gate, World: ws, Outcomes: ev, Clock: clock,
			Log: log.With("component", "action"), ApprovalTTL: o.Flags.ApprovalTTL,
			RequireKnownTarget: o.Flags.RequireKnownTarget, RequireDistinctApprover: o.Flags.RequireDistinctApprover,
			Effects: o.Store, Identity: action.WorldIdentity{World: ws},
			Quality: provenance.Quality{Health: o.Store, Authority: authority, Clock: clock},
			DryRun:  o.Flags.ActionDryRun, Allowed: o.Flags.ActionAllowlist, Scope: o.Flags.ActionScope,
			Preauthorized: o.Flags.Preauthorized}
	}
	runs := orchestration.KernelRuns{Server: o.Server}
	ing := &orchestration.Ingestor{Store: o.Store, Router: router, Catalog: cat, Shadow: o.Flags.Shadow, Clock: clock,
		Log: log, Metrics: m, Sources: sources2, Changes: o.Store, Neighbors: neighbors(ws)}
	svc := &orchestration.Service{Store: o.Store, Runs: runs, Catalog: cat, Clock: clock, Log: log,
		AllowInlineTemplates: o.Flags.InlineTemplates, Queues: queues}
	e := &Engine{
		Service:  svc,
		Ingestor: ing,
		Actions:  rt,
		World:    ws,
		runners:  runners,
		log:      log,
		Launch:   att, Flags: o.Flags, store: o.Store, metrics: m, authority: authority, deployment: o.Deployment,
		requested: requested, schema: o.Schema, capabilities: o.Capabilities, actionDescs: reg.Descriptors(), metricNames: names,
		Orch: &orchestration.Orchestrator{Store: o.Store, Runs: runs, Actions: rt, Evaluator: ev, Clock: clock, Log: log,
			Metrics: m, Owner: o.Owner, World: ws, Queues: queues, Pools: pools, Catalog: cat},
		Reconciler: &orchestration.Reconciler{Store: o.Store, Ingestor: ing, Actions: rt, Clock: clock, Log: log, Metrics: m,
			Retention: inboxRetention(o.Flags)},
		API: &workapi.API{Store: o.Store, Service: svc, Ingestor: ing, Actions: rt, World: ws, Clock: clock, Log: log,
			Audit: o.Server.Audit, Sources: sources},
	}
	if o.Flags.EvidencePlanning && o.Server.Tools != nil {
		e.Orch.Checks = KernelChecks{Server: o.Server}
	}
	maxClass := dataguard.Confidential
	if o.Flags.ModelMaxDataClass != "" {
		if maxClass, err = dataguard.Parse(o.Flags.ModelMaxDataClass); err != nil {
			return nil, fmt.Errorf("HARNESS_MODEL_MAX_DATA_CLASS: %w", err)
		}
	}
	e.Orch.SetMaxDataClass(maxClass)
	e.API.Attestation = func() governance.Attestation {
		actx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return e.Attestation(actx)
	}
	if err := e.wireObservability(context.Background(), o); err != nil {
		return nil, err
	}
	e.wirePlaybooks(o)
	e.wireEval(o)
	if m != nil {
		ws.Observer = m
		svc.Metrics = m
		e.API.Denials = m
		if rt != nil {
			rt.Observer = m
		}
	}
	log.Info("work engine assembled", "shadow_events", o.Flags.Shadow, "actions_v2", o.Flags.ActionV2,
		"templates", len(cat.List()), "correlation_rules", len(o.Config.CorrelationRules),
		"actions", len(reg.Descriptors()), "metric_sources", len(metricSources))
	return e, nil
}

// Run starts the orchestrator, the reconciler and plugin feeders; it
// returns when ctx ends or one of them fails.
func (e *Engine) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return e.Orch.Run(ctx) })
	g.Go(func() error { return e.Reconciler.Run(ctx) })
	if e.Playbooks != nil {
		g.Go(func() error { e.sweepPlaybooks(ctx); return nil })
	}
	g.Go(func() error { e.probeSources(ctx); return nil })
	env := plugin.WorkEnv{
		AssertFact: func(ctx context.Context, f world.Fact) error {
			_, _, err := e.World.Store.AssertFact(ctx, f)
			return err
		},
		Ingest: func(ctx context.Context, env eventing.Envelope) error {
			_, _, err := e.Ingestor.Ingest(ctx, env)
			return err
		},
		AssertRelation: func(ctx context.Context, r world.Relation) error {
			_, _, err := e.World.Store.AssertRelation(ctx, r)
			return err
		},
		RetractRelation: func(ctx context.Context, tenantID, from, typ, to, source string, at time.Time) error {
			return e.World.Store.RetractRelation(ctx, tenantID, from, typ, to, source, at, at)
		},
		UpsertEntity: func(ctx context.Context, ent world.Entity) error {
			_, err := e.World.Store.UpsertEntity(ctx, ent, e.World.Clock.Now())
			return err
		},
		CollectorReport: func(ctx context.Context, tenantID, collector string, at time.Time, every time.Duration, cerr error) {
			if err := e.store.RecordCollection(ctx, provenance.Report{TenantID: tenantID, Collector: collector, At: at,
				Every: every, Err: cerr, Full: true}); err != nil {
				e.log.Warn("record collector health", "collector", collector, "err", err)
			}
		},
	}
	for _, r := range e.runners {
		g.Go(func() error {
			if err := r.RunWork(ctx, env); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("work runner: %w", err)
			}
			return nil
		})
	}
	return g.Wait()
}

// MirrorTriggers attaches the v1 trigger service to the event inbox when
// the TriggerMirror flag is on (v1 behaviour is unchanged).
func (e *Engine) MirrorTriggers(t *trigger.Service, flags Flags) {
	if t == nil || !flags.TriggerMirror {
		return
	}
	t.Mirror = orchestration.TriggerMirror{Ingestor: e.Ingestor}
	e.log.Info("v1 trigger events are mirrored into the event inbox", "shadow", flags.Shadow)
}

// AttachContext serves World facts to runs through the kernel's context
// gatherer and records what other context providers report as World
// history (RFC §7.6).
func (e *Engine) AttachContext(g *envctx.Gatherer) {
	if g == nil {
		return
	}
	g.Providers = append(g.Providers, orchestration.WorldContext{World: e.World})
	rec := orchestration.EnvFactRecorder{Store: e.World.Store, Clock: e.World.Clock, Log: e.log}
	g.OnFacts = func(ctx context.Context, q envctx.Query, facts []domain.ContextFact) {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		rec.Record(rctx, q, facts)
	}
}

// Owner returns a replica id for leases ("<host>-<random>").
func Owner(prefix string) string {
	if prefix == "" {
		prefix = "work"
	}
	return prefix + "-" + cpkit.NewID()[24:]
}

// inboxRetention keeps payloads no longer than the records they belong to.
func inboxRetention(f Flags) cpsql.InboxRetention {
	r := cpsql.InboxRetention{Payload: f.InboxPayloadRetention, Record: f.InboxRecordRetention}
	if r.Record > 0 && (r.Payload == 0 || r.Payload > r.Record) {
		r.Payload = r.Record
	}
	return r
}

// extensionEvents merges the extensions' default correlation rules under
// the operator's (an operator rule replaces a default with the same name)
// and collects their webhook normalizers.
func extensionEvents(operator []eventing.Rule, exts []plugin.WorkExtension) ([]eventing.Rule, map[string]workapi.Normalizer, error) {
	rules := append([]eventing.Rule(nil), operator...)
	have := map[string]bool{}
	for _, r := range operator {
		have[r.Name] = true
	}
	sources := map[string]workapi.Normalizer{}
	for _, x := range exts {
		if ec, ok := x.(plugin.EventContributor); ok {
			for _, r := range ec.EventRules() {
				if have[r.Name] {
					continue
				}
				have[r.Name] = true
				rules = append(rules, r)
			}
		}
		if sn, ok := x.(plugin.SourceNormalizers); ok {
			for _, name := range sn.Sources() {
				if _, dup := sources[name]; dup {
					return nil, nil, fmt.Errorf("event source %q is registered twice", name)
				}
				if fn, ok := sn.Normalizer(name); ok {
					sources[name] = fn
				}
			}
		}
	}
	return rules, sources, nil
}
