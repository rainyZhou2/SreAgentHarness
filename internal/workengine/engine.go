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
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/playbook"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
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
		ApprovalTTL: 30 * time.Minute}
	if d, err := time.ParseDuration(getenv("HARNESS_ACTION_APPROVAL_TTL")); err == nil && d > 0 {
		f.ApprovalTTL = d
	}
	f.ModelMaxDataClass = strings.TrimSpace(getenv("HARNESS_MODEL_MAX_DATA_CLASS"))
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
			Effects: o.Store}
	}
	runs := orchestration.KernelRuns{Server: o.Server}
	ing := &orchestration.Ingestor{Store: o.Store, Router: router, Catalog: cat, Shadow: o.Flags.Shadow, Clock: clock,
		Log: log, Metrics: m}
	svc := &orchestration.Service{Store: o.Store, Runs: runs, Catalog: cat, Clock: clock, Log: log,
		AllowInlineTemplates: o.Flags.InlineTemplates}
	e := &Engine{
		Service:  svc,
		Ingestor: ing,
		Actions:  rt,
		World:    ws,
		runners:  runners,
		log:      log,
		Orch: &orchestration.Orchestrator{Store: o.Store, Runs: runs, Actions: rt, Evaluator: ev, Clock: clock, Log: log,
			Metrics: m, Owner: o.Owner, World: ws, Queues: queues, Pools: pools},
		Reconciler: &orchestration.Reconciler{Store: o.Store, Ingestor: ing, Actions: rt, Clock: clock, Log: log, Metrics: m,
			Retention: inboxRetention(o.Flags)},
		API: &workapi.API{Store: o.Store, Service: svc, Ingestor: ing, Actions: rt, World: ws, Clock: clock, Log: log,
			Audit: o.Server.Audit, Sources: sources},
	}
	maxClass := dataguard.Confidential
	if o.Flags.ModelMaxDataClass != "" {
		if maxClass, err = dataguard.Parse(o.Flags.ModelMaxDataClass); err != nil {
			return nil, fmt.Errorf("HARNESS_MODEL_MAX_DATA_CLASS: %w", err)
		}
	}
	e.Orch.SetMaxDataClass(maxClass)
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
