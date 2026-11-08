// Package app bootstraps the harness: the non-pluggable kernel (state store,
// outbox publisher, reconciler, worker pool, API core) plus every plugin the
// configuration enables. Built-in plugins are compiled in by importing
// go-sre-harness/plugins/all; custom binaries add their own packages.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/eventsink"
	"go-sre-harness/internal/extensions"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/graph"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/scheduler"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/artifactq"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/migrations"
)

var _ kernel.StateStore = (*pg.Store)(nil)
var _ api.Store = (*pg.Store)(nil)
var _ scheduler.OutboxStore = (*pg.Store)(nil)
var _ scheduler.ReconcileStore = (*pg.Store)(nil)

// Main is the command-line entry point:
//
//	harness migrate              apply database migrations
//	harness all                  API + scheduler + worker in one process
//	harness api|worker|scheduler run a single role
//	harness plugins              list compiled-in plugins and extension points
//	harness hash-token <token>   print the SHA-256 for a static token file
func Main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mode := "all"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	switch mode {
	case "hash-token":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: harness hash-token <token>")
			os.Exit(2)
		}
		fmt.Println(auth.HashToken(os.Args[2]))
		return
	case "plugins":
		for _, k := range plugin.Kinds() {
			fmt.Printf("%s (multi=%v): %s\n", k.Name, k.Multi, k.Doc)
			for _, p := range plugin.Registered() {
				if p.Kind == k.Name {
					fmt.Printf("    %-16s %s\n", p.Name, p.Doc)
				}
			}
		}
		return
	case "migrate", "all", "api", "worker", "scheduler":
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (migrate|all|api|worker|scheduler|plugins|hash-token)\n", mode)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Run(ctx, mode, os.Getenv, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

// Run starts the harness in the given role.
func Run(ctx context.Context, mode string, getenv func(string) string, log *slog.Logger) error {
	e := Env(getenv)
	dbURL := e("HARNESS_DATABASE_URL")
	if dbURL == "" {
		return errors.New("HARNESS_DATABASE_URL is required")
	}
	pcfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return err
	}
	pg.ConfigureRLS(pcfg)
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	if mode == "migrate" || e.bool("HARNESS_AUTO_MIGRATE", true) {
		if err := migrations.Apply(ctx, pool); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
		if mode == "migrate" {
			log.Info("migrations applied")
			return nil
		}
	}
	a, err := Build(ctx, mode, e, pool, log)
	if err != nil {
		return err
	}
	return a.Run(ctx)
}

// App is an assembled harness process.
type App struct {
	Mode    string
	Host    *plugin.Host
	Store   *pg.Store
	Server  *api.Server
	Handler http.Handler
	Config  File

	env     Env
	log     *slog.Logger
	broker  plugin.MessageBroker
	worker  *kernel.Worker
	sinks   []*eventsink.Dispatcher
	metrics *obs.Metrics

	triggerCfgs []trigger.Config
	triggers    *trigger.Service
}

// Build assembles the core and all enabled plugins without starting anything.
func Build(ctx context.Context, mode string, e Env, pool *pgxpool.Pool, log *slog.Logger) (a *App, err error) {
	cfgFile, err := FromEnv(e)
	if err != nil {
		return nil, err
	}
	if cfgFile, err = LoadFile(e("HARNESS_CONFIG"), cfgFile); err != nil {
		return nil, err
	}
	specs := cfgFile.Plugins

	metrics := obs.NewMetrics()
	cfg := pg.DefaultConfig()
	cfg.MaxStepsPerRun = e.int("HARNESS_MAX_STEPS_PER_RUN", cfg.MaxStepsPerRun)
	cfg.ApprovalTTL = e.dur("HARNESS_APPROVAL_TTL", cfg.ApprovalTTL)
	cfg.MemoryRetrieval = e.bool("HARNESS_MEMORY_RETRIEVAL", true) && len(specs["memory_retriever"].Enabled()) > 0
	cfg.MaxRunningPerTenant = e.int("HARNESS_MAX_RUNNING_PER_TENANT", 0)
	cfg.RequireDistinctApprover = e.bool("HARNESS_REQUIRE_DISTINCT_APPROVER", false)
	store := pg.New(pool, cfg)
	metrics.Reg.Collect("harness_runs", "Non-terminal runs by status.", func(ctx context.Context, emit func(string, map[string]string, float64)) {
		if m, err := store.RunsByStatus(ctx); err == nil {
			for st, n := range m {
				emit("harness_runs", map[string]string{"status": st}, float64(n))
			}
		}
		if n, err := store.OutboxBacklog(ctx); err == nil {
			emit("harness_outbox_backlog", nil, float64(n))
		}
	})

	h := plugin.NewHost(mode, log)
	h.Store, h.Metrics, h.Settings = store, metrics, cfgFile.Settings
	h.HTTPAddr = e.str("HARNESS_HTTP_ADDR", "127.0.0.1:8080")
	defer func() {
		if err != nil {
			cctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = h.Close(cctx)
		}
	}()
	a = &App{Mode: mode, Host: h, Store: store, Config: cfgFile, env: e, log: log, metrics: metrics}

	// telemetry
	exporters, err := plugin.BuildAll(ctx, plugin.Telemetry, h, specs["telemetry"])
	if err != nil {
		return nil, err
	}
	observers := multiObserver{metrics}
	for _, x := range exporters {
		if o, ok := x.(kernel.Observer); ok {
			observers = append(observers, o)
		}
	}
	// artifact store
	if h.Artifacts, err = plugin.BuildOne(ctx, plugin.ArtifactStore, h, specs["artifact_store"]); err != nil {
		return nil, err
	}
	// network policy for everything that dials tenant-controlled URLs
	allowNets, err := extensions.ParseNetworks(e.str("HARNESS_EXTENSIONS_ALLOW_NETWORKS", ""))
	if err != nil {
		return nil, fmt.Errorf("HARNESS_EXTENSIONS_ALLOW_NETWORKS: %w", err)
	}
	h.Network = extensions.NetPolicy{Allow: allowNets}
	// tools
	h.Tools = tools.NewRegistry()
	sources, err := plugin.BuildAll(ctx, plugin.Tools, h, specs["tools"])
	if err != nil {
		return nil, err
	}
	for _, src := range sources {
		ts, err := src.Tools(ctx)
		if err != nil {
			return nil, err
		}
		for _, t := range ts {
			if _, dup := h.Tools.Lookup(t.Descriptor().Name); dup {
				return nil, fmt.Errorf("tool %q is provided by more than one tool plugin", t.Descriptor().Name)
			}
			h.Tools.Register(t)
		}
	}
	// framework tools: reading offloaded tool results
	if _, dup := h.Tools.Lookup(artifactq.Name); dup {
		return nil, fmt.Errorf("tool %q is reserved for the harness", artifactq.Name)
	}
	h.Tools.Register(artifactq.New(h.Artifacts))
	if err := applyToolOverlays(h.Tools, cfgFile.ToolOverlays, log); err != nil {
		return nil, err
	}
	// tenant extensions: MCP servers, HTTP tools and skills managed through
	// /v1/extensions are loaded per tenant and overlay the global registry.
	h.Extensions = &extensions.Runtime{Store: store, Log: log.With("component", "extensions"),
		Options: extensions.Options{AllowStdio: e.bool("HARNESS_MCP_ALLOW_STDIO", false),
			Network: h.Network},
		Interval: e.dur("HARNESS_EXTENSIONS_SYNC_INTERVAL", 5*time.Second)}
	h.Tools.SetTenantSource(h.Extensions)
	// profiles
	profiles, err := registerProfiles(ctx, h, specs["profile_source"])
	if err != nil {
		return nil, err
	}
	autoProfileID := e.str("HARNESS_AUTO_PROFILE", h.Settings.DefaultProfile)
	autoProfileVersion := e.int64("HARNESS_AUTO_PROFILE_VERSION", 10)
	autoFound := false
	for _, p := range profiles {
		if p.ID == autoProfileID && p.Version == autoProfileVersion {
			autoFound = true
			break
		}
	}
	if !autoFound {
		log.Warn("auto profile is not registered; falling back to the default profile",
			"profile", autoProfileID, "version", autoProfileVersion)
		autoProfileID, autoProfileVersion = h.Settings.DefaultProfile, h.Settings.DefaultProfileVersion
	}
	// policy
	if h.Policy, err = plugin.BuildOne(ctx, plugin.Policy, h, specs["policy"]); err != nil {
		return nil, err
	}
	h.Policy = policyGuard(h.Policy)
	// streaming and broker
	if h.Deltas, err = plugin.BuildOne(ctx, plugin.StreamBus, h, specs["stream_bus"]); err != nil {
		return nil, err
	}
	if mode == "all" || mode == "worker" || mode == "scheduler" {
		if a.broker, err = plugin.BuildOne(ctx, plugin.Broker, h, specs["broker"]); err != nil {
			return nil, err
		}
	}
	if mode == "all" || mode == "api" || mode == "scheduler" {
		if h.Events, err = optionalOne(ctx, plugin.EventBus, h, specs["event_bus"]); err != nil {
			return nil, err
		}
	}
	// routing score
	scorer, err := optionalOne(ctx, plugin.RoutePolicy, h, specs["route_policy"])
	if err != nil {
		return nil, err
	}
	breakers := gw.NewBreakers(orInt(cfgFile.Models.CircuitFailureThreshold, 3), orDur(cfgFile.Models.CircuitCooldown.D(), 30*time.Second))
	// models
	backends, err := plugin.BuildAll(ctx, plugin.ModelProvider, h, specs["model_provider"])
	if err != nil {
		return nil, err
	}
	var parts []gw.PolicyPart
	var providers []gw.Provider
	for _, b := range backends {
		parts = append(parts, gw.PolicyPart{Endpoints: b.Endpoints, Aliases: b.Aliases})
		providers = append(providers, b.Provider)
	}
	route, err := gw.ComposePolicy("default", parts...)
	if err != nil {
		return nil, err
	}
	if len(route.Aliases[h.Settings.ModelAlias]) == 0 {
		log.Warn("default model alias has no endpoints", "alias", h.Settings.ModelAlias)
	}
	h.Gateway = gw.New(route, store, h.Artifacts, gw.Admission{DailyBudgetMicros: int64(cfgFile.Models.DailyBudgetUSD * 1e6),
		MaxInflightPerTenant: cfgFile.Models.MaxInflightPerTenant}, log, providers...)
	h.Gateway.SetBreakers(breakers)
	if scorer != nil {
		h.Gateway.SetScorer(scorer)
	}
	log.Info("model gateway", "alias", h.Settings.ModelAlias, "endpoints", route.Aliases[h.Settings.ModelAlias])
	// Tenant-configured models join the gateway as an overlay; the first
	// sync loads them (and tenant tools) before any run is claimed.
	h.Extensions.Models = h.Gateway
	{
		sctx, c := context.WithTimeout(ctx, 20*time.Second)
		if err := h.Extensions.Sync(sctx); err != nil {
			log.Warn("initial extensions sync", "err", err)
		}
		c()
	}
	// memory
	if h.Embedder, err = plugin.BuildOne(ctx, plugin.Embedder, h, specs["embedder"]); err != nil {
		return nil, err
	}
	var retriever kernel.MemoryRetriever
	if len(specs["memory_retriever"].Enabled()) > 0 {
		if retriever, err = plugin.BuildOne(ctx, plugin.MemoryRetriever, h, specs["memory_retriever"]); err != nil {
			return nil, err
		}
	}
	// context assembly and compaction
	assembler, err := optionalOne(ctx, plugin.ContextAssembler, h, specs["context_assembler"])
	if err != nil {
		return nil, err
	}
	compactor, err := optionalOne(ctx, plugin.Compactor, h, specs["compactor"])
	if err != nil {
		return nil, err
	}
	// environment context: facts pinned per run
	ctxProviders, err := plugin.BuildAll(ctx, plugin.ContextProvider, h, specs["context_provider"])
	if err != nil {
		return nil, err
	}
	var gatherer kernel.ContextGatherer
	if len(ctxProviders) > 0 {
		gatherer = &envctx.Gatherer{Providers: ctxProviders,
			Timeout:  e.dur("HARNESS_CONTEXT_PROVIDER_TIMEOUT", 5*time.Second),
			MaxFacts: e.int("HARNESS_CONTEXT_MAX_FACTS", 30),
			Log:      log.With("component", "envctx")}
	}
	// strategies
	strategies := map[string]plugin.StrategyBuilder{}
	for _, s := range specs["strategy"].Enabled() {
		b, err := plugin.Build(ctx, plugin.Strategy, h, s)
		if err != nil {
			return nil, err
		}
		id := s.Instance
		if id == "" {
			id = s.Name
		}
		strategies[id] = b
	}
	sel := &strategySelector{builders: strategies, cfg: cfgFile.Strategy,
		live: plugin.StrategyRuntime{Gateway: h.Gateway, Context: assembler, Artifacts: h.Artifacts, Settings: h.Settings,
			Deltas: &stream.Recorder{Bus: h.Deltas, Store: store, Artifacts: h.Artifacts, Log: log}}}
	defaultStrategy, err := sel.build(sel.live, profile.Spec{})
	if err != nil {
		return nil, fmt.Errorf("default strategy: %w", err)
	}
	// evaluation
	grader, err := plugin.BuildOne(ctx, plugin.Grader, h, specs["grader"])
	if err != nil {
		return nil, err
	}
	h.Eval = &eval.Service{Store: store, Artifacts: h.Artifacts, Registry: h.Tools, Grader: grader, Log: log,
		Factory: func(spec profile.Spec) (cognitive.Strategy, error) {
			// Offline replay gets its own gateway with an in-memory ledger:
			// replay calls belong to no run and cannot be lease-fenced.
			g := gw.New(route, gw.NewMemLedger(), h.Artifacts, gw.Admission{}, log, providers...)
			rt := plugin.StrategyRuntime{Gateway: g, Artifacts: h.Artifacts, Settings: h.Settings}
			return sel.build(rt, spec)
		}}

	for _, d := range profiles {
		if _, err := sel.build(sel.live, d.spec); err != nil {
			log.Warn("profile strategy is not available", "profile", d.ID, "version", d.Version, "tenant", d.Tenant, "err", err)
		}
	}

	// event sinks (scheduler role)
	if mode == "all" || mode == "scheduler" {
		if a.sinks, err = buildEventSinks(ctx, h, specs["event_sink"], metrics); err != nil {
			return nil, err
		}
	}
	rules := &profile.RulesFor{Loader: store}

	// jobs (scheduler role); their Run methods are started by the host
	if mode == "all" || mode == "scheduler" {
		if _, err := plugin.BuildAll(ctx, plugin.Job, h, specs["job"]); err != nil {
			return nil, err
		}
	}
	// graph projection: capability map for prompts and the read-only API
	graphSvc := &graph.Service{Store: store, Tools: h.Tools, Extensions: h.Extensions,
		Log: log.With("component", "graph")}
	// planner: auto runs decide between ReAct and a generated flow
	router, err := planner.NewRouter(
		planner.ParsePatterns(e("HARNESS_PLANNER_REACT_PATTERNS"), planner.DefaultReactPatterns),
		planner.ParsePatterns(e("HARNESS_PLANNER_FLOW_PATTERNS"), planner.DefaultFlowPatterns))
	if err != nil {
		return nil, err
	}
	plannerSvc := &planner.Service{
		Gateway: h.Gateway, Artifacts: h.Artifacts, Profiles: store,
		Log: log.With("component", "planner"), Observer: metrics,
		Config: planner.Config{
			ModelAlias:         e.str("HARNESS_PLANNER_MODEL_ALIAS", h.Settings.ModelAlias),
			Effort:             e.str("HARNESS_PLANNER_EFFORT", h.Settings.ModelEffort),
			MaxRepairs:         e.int("HARNESS_PLANNER_MAX_REPAIRS", 2),
			MaxNodes:           e.int("HARNESS_PLANNER_MAX_NODES", 20),
			MaxOutputTokens:    e.int64("HARNESS_PLANNER_MAX_OUTPUT_TOKENS", 8000),
			MaxChildren:        e.int("HARNESS_PLANNER_MAX_CHILDREN", 8),
			MaxTotalRuns:       e.int("HARNESS_PLANNER_MAX_TOTAL_RUNS", 32),
			MaxDepth:           e.int("HARNESS_PLANNER_MAX_DEPTH", 3),
			MaxIterations:      e.int("HARNESS_PLANNER_MAX_ITERATIONS", 10),
			MaxModelCalls:      e.int("HARNESS_PLANNER_MAX_MODEL_CALLS", 40),
			MaxEstimatedTokens: e.int64("HARNESS_PLANNER_MAX_ESTIMATED_TOKENS", 400000),
			ApprovalMode:       e.str("HARNESS_PLAN_APPROVAL", "auto"),
			ApprovalApprovers:  e.int("HARNESS_PLAN_APPROVERS", 1),
			FallbackProfileID:  e.str("HARNESS_PLANNER_FALLBACK_PROFILE", h.Settings.DefaultProfile),
			FallbackVersion:    e.int64("HARNESS_PLANNER_FALLBACK_VERSION", 8),
			FallbackReact:      e.bool("HARNESS_PLANNER_FALLBACK_REACT", true),
			Router:             router,
		},
	}
	// worker
	if mode == "all" || mode == "worker" {
		hostname, _ := os.Hostname()
		a.worker = kernel.NewWorker(kernel.Config{
			ID:                 e.str("HARNESS_WORKER_ID", fmt.Sprintf("%s-%d", hostname, os.Getpid())),
			LeaseTTL:           e.dur("HARNESS_LEASE_TTL", 45*time.Second),
			StepTimeout:        e.dur("HARNESS_STEP_TIMEOUT", 2*time.Minute),
			Checkpoints:        e.bool("HARNESS_CHECKPOINTS", true),
			Memory:             retriever,
			Compactor:          compactor,
			Rules:              rules,
			Skills:             h.Extensions,
			ToolToggles:        store,
			CapabilityMap:      graphSvc,
			Planner:            plannerSvc,
			PlanApprovers:      e.int("HARNESS_PLAN_APPROVERS", 1),
			PlanApprovalMode:   e.str("HARNESS_PLAN_APPROVAL", "auto"),
			PlanMaxReplans:     e.int("HARNESS_PLAN_MAX_REPLANS", kernel.DefaultPlanMaxReplans),
			Strategies:         &profile.Resolver{Loader: store, Factory: sel.liveFactory()},
			Observer:           observers,
			Context:            gatherer,
			OutputOffloadBytes: e.int("HARNESS_OUTPUT_OFFLOAD_BYTES", kernel.DefaultOffloadBytes),
		}, store, h.Artifacts, defaultStrategy, h.Tools, h.Policy, log)
	}
	// triggers: webhooks are served by the API role, schedules fire in the
	// scheduler role; deduplication is shared through the store.
	if len(cfgFile.Triggers) > 0 && (mode == "all" || mode == "api" || mode == "scheduler") {
		a.triggerCfgs = cfgFile.Triggers
	}
	// API
	if mode == "all" || mode == "api" {
		streamMode := e.str("HARNESS_SSE_MODE", api.StreamPush)
		if h.Events == nil || (mode == "api" && !h.Events.Shared()) {
			if streamMode == api.StreamPush {
				log.Warn("event stream falls back to polling; configure a shared event_bus")
			}
			streamMode = api.StreamPoll
		}
		if mode == "all" && streamMode == api.StreamPush {
			hasRelay := false
			for _, s := range specs["job"].Enabled() {
				if s.Name == "event_relay" {
					hasRelay = true
				}
			}
			if !hasRelay {
				log.Warn("SSE push is enabled but the event_relay job is not configured; live events will not be published")
			}
		}
		authns, err := plugin.BuildAll(ctx, plugin.Authenticator, h, specs["authenticator"])
		if err != nil {
			return nil, err
		}
		if len(authns) == 0 {
			return nil, errors.New("no authenticator plugin is enabled")
		}
		srv := &api.Server{Store: store, Artifacts: h.Artifacts, DefaultProfile: h.Settings.DefaultProfile,
			DefaultVersion: h.Settings.DefaultProfileVersion,
			AutoProfileID:  autoProfileID, AutoProfileVersion: autoProfileVersion, Log: log,
			Deltas: h.Deltas, BranchPolicy: domain.BranchPolicy(e.str("HARNESS_BRANCH_POLICY", string(domain.BranchQueue))),
			MaxPendingTurn: e.int("HARNESS_MAX_PENDING_TURNS", 8), Tools: h.Tools, Policy: h.Policy,
			Events: h.Events, StreamMode: streamMode, StreamTick: e.dur("HARNESS_SSE_TICK", 30*time.Second),
			PollInterval:   e.dur("HARNESS_SSE_POLL", 500*time.Millisecond),
			ResolveProfile: h.Eval.ResolveProfile, Rules: rules.Rules, Auth: &auth.Chain{Authenticators: authns, Roles: store},
			Audit: store, Admin: store, Metrics: metrics, Plugins: func() any { return a.PluginReport() }}
		h.Server = srv
		exts, err := plugin.BuildAll(ctx, plugin.API, h, specs["api"])
		if err != nil {
			return nil, err
		}
		for _, x := range exts {
			srv.Extend(x)
		}
		if a.triggerCfgs != nil {
			if a.triggers, err = trigger.New(a.triggerCfgs, srv, store, log.With("component", "trigger")); err != nil {
				return nil, err
			}
			srv.Extend(&trigger.API{Service: a.triggers})
		}
		active := h.Active()
		plugins := make([]graph.PluginInfo, 0, len(active))
		for _, p := range active {
			plugins = append(plugins, graph.PluginInfo{Kind: p.Kind, Name: p.Name, Instance: p.Instance})
		}
		graphSvc.Plugins = plugins
		srv.Graph = graphSvc
		a.Server, a.Handler = srv, srv.Handler()
	}
	if a.triggerCfgs != nil && a.triggers == nil {
		// scheduler-only role: schedules start runs through an API core
		// without listeners.
		srv := &api.Server{Store: store, Artifacts: h.Artifacts, DefaultProfile: h.Settings.DefaultProfile,
			DefaultVersion: h.Settings.DefaultProfileVersion, AutoProfileID: autoProfileID,
			AutoProfileVersion: autoProfileVersion, Log: log, Tools: h.Tools, Policy: h.Policy, Rules: rules.Rules,
			ResolveProfile: h.Eval.ResolveProfile}
		if a.triggers, err = trigger.New(a.triggerCfgs, srv, store, log.With("component", "trigger")); err != nil {
			return nil, err
		}
	}
	log.Info("plugins active", "mode", mode, "plugins", h.Active(), "tools", len(h.Tools.Descriptors()))
	return a, nil
}

// PluginReport describes extension points, compiled-in and active plugins.
type PluginReport struct {
	Kinds    []plugin.KindInfo `json:"kinds"`
	Compiled []plugin.Info     `json:"compiled"`
	Active   []plugin.Active   `json:"active"`
	Tools    []string          `json:"tools"`
}

func (a *App) PluginReport() PluginReport {
	r := PluginReport{Kinds: plugin.Kinds(), Compiled: plugin.Registered(), Active: a.Host.Active()}
	for _, d := range a.Host.Tools.Descriptors() {
		r.Tools = append(r.Tools, d.Name)
	}
	return r
}

// Run starts the core loops, plugin runners and listeners, and blocks until
// ctx is cancelled or one of them fails.
func (a *App) Run(ctx context.Context) error {
	defer func() {
		cctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if err := a.Host.Close(cctx); err != nil {
			a.log.Warn("plugin shutdown", "err", err)
		}
	}()
	e, log, mode := a.env, a.log, a.Mode
	g, ctx := errgroup.WithContext(ctx)
	if mode == "all" || mode == "scheduler" {
		g.Go(func() error { return (&scheduler.Publisher{Store: a.Store, Broker: a.broker, Log: log}).Run(ctx) })
		for _, d := range a.sinks {
			g.Go(func() error { return d.Run(ctx) })
		}
		if a.triggers != nil {
			g.Go(func() error { return a.triggers.RunSchedules(ctx) })
			g.Go(func() error { return purgeTriggerClaims(ctx, a.Store, log) })
		}
		g.Go(func() error {
			return (&scheduler.Reconciler{Store: a.Store, Interval: e.dur("HARNESS_RECONCILE_INTERVAL", 10*time.Second), Log: log}).Run(ctx)
		})
	}
	if x := a.Host.Extensions; x != nil {
		g.Go(func() error {
			if err := x.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("extensions: %w", err)
			}
			return nil
		})
	}
	for id, r := range a.Host.Runners() {
		g.Go(func() error {
			if err := r.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("%s: %w", id, err)
			}
			return nil
		})
	}
	if a.worker != nil {
		p := kernel.NewPool(a.worker, e.int("HARNESS_WORKER_MAX_INFLIGHT", 90))
		g.Go(func() error {
			err := a.broker.Run(ctx, p.Handle)
			p.Wait()
			return err
		})
	}
	if a.Handler != nil {
		addr := a.Host.HTTPAddr
		httpSrv := &http.Server{Addr: addr, ReadHeaderTimeout: 10 * time.Second, Handler: a.Handler}
		g.Go(func() error {
			log.Info("http listening", "addr", addr)
			if err := httpSrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-ctx.Done()
			sctx, c := context.WithTimeout(context.Background(), 30*time.Second)
			defer c()
			return httpSrv.Shutdown(sctx)
		})
	} else if addr := e("HARNESS_METRICS_ADDR"); addr != "" {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", a.metrics.Reg.Handler())
		msrv := &http.Server{Addr: addr, ReadHeaderTimeout: 5 * time.Second, Handler: mux}
		g.Go(func() error {
			if err := msrv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error { <-ctx.Done(); return msrv.Close() })
	}
	return g.Wait()
}
