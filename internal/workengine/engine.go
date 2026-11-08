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
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workapi"
	"go-sre-harness/internal/world"
)

// Config is the "work" section of the config file.
type Config struct {
	// Templates add to (or override) the built-in templates.
	Templates []work.Template `json:"templates,omitempty"`
	// CorrelationRules route ingested events to Works (first match wins).
	CorrelationRules []eventing.Rule `json:"correlation_rules,omitempty"`
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
	runners    []plugin.WorkRunner
	log        *slog.Logger
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
	cat, err := orchestration.NewCatalog(o.Config.Templates)
	if err != nil {
		return nil, err
	}
	router, err := eventing.NewRouter(o.Config.CorrelationRules)
	if err != nil {
		return nil, fmt.Errorf("correlation rules: %w", err)
	}
	ws := &world.Service{Store: o.Store, Clock: clock}
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
	}
	var rt *action.Runtime
	if o.Flags.ActionV2 {
		gate := o.Policy
		if gate == nil {
			gate = policy.Default()
		}
		rt = &action.Runtime{Store: o.Store, Registry: reg, Policy: gate, World: ws, Outcomes: ev, Clock: clock,
			Log: log.With("component", "action"), ApprovalTTL: o.Flags.ApprovalTTL,
			RequireKnownTarget: o.Flags.RequireKnownTarget, RequireDistinctApprover: o.Flags.RequireDistinctApprover}
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
			Metrics: m, Owner: o.Owner},
		Reconciler: &orchestration.Reconciler{Store: o.Store, Ingestor: ing, Actions: rt, Clock: clock, Log: log, Metrics: m},
		API:        &workapi.API{Store: o.Store, Service: svc, Ingestor: ing, Actions: rt, World: ws, Clock: clock, Log: log},
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
	env := plugin.WorkEnv{
		AssertFact: func(ctx context.Context, f world.Fact) error {
			_, _, err := e.World.Store.AssertFact(ctx, f)
			return err
		},
		Ingest: func(ctx context.Context, env eventing.Envelope) error {
			_, _, err := e.Ingestor.Ingest(ctx, env)
			return err
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

// Owner returns a replica id for leases ("<host>-<random>").
func Owner(prefix string) string {
	if prefix == "" {
		prefix = "work"
	}
	return prefix + "-" + cpkit.NewID()[24:]
}
