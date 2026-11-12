// Package localapp assembles the single-process harness used by the desktop
// local runtime. It has no PostgreSQL, NATS, scheduler process or plugin host:
// the SQLite store, kernel worker, API and event loop live in one process.
package localapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-sre-harness/internal/runsearch"
	"go-sre-harness/internal/sandbox"
	"go-sre-harness/internal/tools/memsearch"
	runsearchtool "go-sre-harness/internal/tools/runsearch"
	"go-sre-harness/internal/tools/sandboxexec"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/curate"
	curatewire "go-sre-harness/internal/curate/wire"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/envctx"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/extensions"
	"go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/gemini"
	"go-sre-harness/internal/gateway/openai"
	"go-sre-harness/internal/graph"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/planner"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/internal/reasoning/improve"
	"go-sre-harness/internal/retention"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/tools/artifactq"
	"go-sre-harness/internal/trigger"
	"go-sre-harness/internal/verify"
	"go-sre-harness/internal/workengine"
	"go-sre-harness/plugins/profiles"
)

const tenant = "local"

type Config struct {
	DataDir   string
	DBPath    string
	HTTPAddr  string
	TokenFile string
	Log       *slog.Logger
	MaxRuns   int
	// LockHeld means the caller already holds the data directory lock
	// (AcquireLock) for the life of Run.
	LockHeld bool
	// OnReady is called once the store is open and the server is about to
	// accept requests (used to print the banner only when it is true).
	OnReady func()
	// ConfigFile is the optional JSON configuration (default
	// <DataDir>/harness.local.json, or HARNESS_LOCAL_CONFIG).
	ConfigFile string
}

// Run starts the local harness and blocks until ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = "127.0.0.1:0"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = cfg.DataDir + "/harness.db"
	}
	if cfg.TokenFile == "" {
		// Standalone use (harness local): keep a token in the data directory.
		tf, _, err := EnsureToken(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("local token: %w", err)
		}
		cfg.TokenFile = tf
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.MaxRuns <= 0 {
		cfg.MaxRuns = 8
	}

	cfgPath, explicit := cfg.ConfigFile, cfg.ConfigFile != ""
	if !explicit {
		if v := os.Getenv("HARNESS_LOCAL_CONFIG"); v != "" {
			cfgPath, explicit = v, true
		} else if cfg.DataDir != "" {
			cfgPath = filepath.Join(cfg.DataDir, "harness.local.json")
		}
	}
	local, err := loadLocalFile(cfgPath, explicit)
	if err != nil {
		return err
	}

	if cfg.DataDir != "" && !cfg.LockHeld {
		release, err := AcquireLock(cfg.DataDir)
		if err != nil {
			return err
		}
		defer release()
	}
	store, err := localstore.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open local store: %w", err)
	}
	defer store.Close()
	if up := store.Upgraded(); up != nil {
		cfg.Log.Warn("local database upgraded", "from", up.From, "to", up.To, "backup", up.Backup)
	}
	if err := seedProfiles(ctx, store); err != nil {
		return err
	}

	arts := &artifact.FS{Root: cfg.DataDir + "/artifacts"}
	modelGW, err := buildGateway(arts, store, cfg.Log)
	if err != nil {
		return err
	}
	reg := tools.NewRegistry(tools.DemoTools()...)
	registerLocalTools(reg, os.Getenv, cfg.Log)
	reg.Register(artifactq.New(arts))
	reg.Register(verify.ReviewTool{})
	reg.Register(kernel.BatchTool{})
	reg.Register(kernel.LoadToolsTool{Registry: reg})
	if len(local.ToolOverlays) > 0 {
		reg.SetOverlays(local.ToolOverlays)
	}
	var pol policy.Gate = policy.Default()
	if local.Policy != nil {
		engine, err := policy.NewRuleEngine(*local.Policy, store)
		if err != nil {
			return fmt.Errorf("local config policy: %w", err)
		}
		pol = engine
	}
	pol = policy.Guarded(pol)
	rules := &profile.RulesFor{Loader: store}
	metrics := obs.NewMetrics()

	networks := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8"), netip.MustParsePrefix("::1/128")}
	if raw := os.Getenv("HARNESS_EXTENSIONS_ALLOW_NETWORKS"); raw != "" {
		extra, err := extensions.ParseNetworks(raw)
		if err != nil {
			return fmt.Errorf("HARNESS_EXTENSIONS_ALLOW_NETWORKS: %w", err)
		}
		networks = append(networks, extra...)
	}
	extRT := &extensions.Runtime{Store: store, Log: cfg.Log.With("component", "extensions"),
		Options: extensions.Options{AllowStdio: strings.EqualFold(os.Getenv("HARNESS_MCP_ALLOW_STDIO"), "true"),
			Network: extensions.NetPolicy{Allow: networks}},
		Interval: 5 * time.Second, Models: modelGW}
	reg.SetTenantSource(extRT)

	assembler := cognitive.Assembler{PromptPrefixVersion: "sre-prefix-v6",
		ProfileRef: "sre-diagnosis@7", MaxContextTokens: 120_000,
		MaxObservationBytes: 16_000, MaxOutputTokens: 8_000}
	strategy := &cognitive.LLM{Gateway: modelGW, Artifacts: arts, Assembler: assembler, Alias: "sre-reasoning"}
	// Per-profile strategies carry the profile's evolvable reasoning layer
	// (learned playbook) into the prompt; versions are immutable, so each is
	// built once.
	llmFor := func(g *gateway.Gateway) profile.Factory {
		return func(spec profile.Spec) (cognitive.Strategy, error) {
			return &cognitive.LLM{Gateway: g, Artifacts: arts, Alias: "sre-reasoning",
				Assembler: assembler.Apply(cognitive.AssemblerOptions{Playbook: spec.Playbook()})}, nil
		}
	}
	strategies := &profile.Resolver{Loader: store, Factory: llmFor(modelGW)}
	var embedder memory.Embedder = memory.HashingEmbedder{Dims: 256}
	if he := memory.EmbedderFromEnv(os.Getenv, cfg.Log.With("component", "embedder")); he != nil {
		embedder = he
	}
	graphSvc := &graph.Service{Store: store, Tools: reg, Extensions: extRT, Log: cfg.Log.With("component", "graph")}
	retriever := &memory.Retriever{Store: store, Embedder: embedder, Artifacts: arts, TopK: 5}
	reg.Register(memsearch.Tool{Retriever: retriever})
	runHistory := &runsearch.Searcher{Runs: store, Artifacts: arts}
	reg.Register(runsearchtool.Tool{Search: runHistory, Runs: store})
	if rt := os.Getenv("HARNESS_SANDBOX_RUNTIME"); rt != "" {
		// Opt-in: commands run in a container per call, without network or
		// credentials, in a per-run workspace under the data directory.
		image := os.Getenv("HARNESS_SANDBOX_IMAGE")
		if image == "" {
			image = "python:3.12-alpine"
		}
		sbx, err := sandbox.NewContainer(sandbox.ContainerConfig{Runtime: rt, Image: image,
			Root: filepath.Join(cfg.DataDir, "sandbox"), Log: cfg.Log.With("component", "sandbox")})
		if err != nil {
			cfg.Log.Warn("sandbox disabled", "err", err)
		} else {
			reg.Register(sandboxexec.Tool{Backend: sbx})
		}
	}
	offlineGW, err := buildGateway(arts, gateway.NewMemLedger(), cfg.Log)
	if err != nil {
		return err
	}
	evalSvc := &eval.Service{Store: store, Artifacts: arts, Registry: reg, Log: cfg.Log.With("component", "eval"),
		// Offline replay uses its own ledger: evaluation calls belong to no
		// run and must not be charged to the tenant. The candidate's
		// reasoning layer is applied, so the gate measures the change.
		Factory: llmFor(offlineGW)}
	router, err := planner.NewRouter(orPatterns(local.PlannerReactPatterns, planner.ParsePatterns(os.Getenv("HARNESS_PLANNER_REACT_PATTERNS"), planner.DefaultReactPatterns)),
		orPatterns(local.PlannerFlowPatterns, planner.ParsePatterns(os.Getenv("HARNESS_PLANNER_FLOW_PATTERNS"), planner.DefaultFlowPatterns)))
	if err != nil {
		return err
	}
	if router, err = router.WithClassifier(os.Getenv("HARNESS_PLANNER_ROUTER_EXAMPLES")); err != nil {
		return err
	}
	var gatherer kernel.ContextGatherer
	var envGatherer *envctx.Gatherer
	if len(local.ContextProviders) > 0 || workengine.FlagsFromEnv(os.Getenv).Engine {
		deps := envctx.Deps{Registry: reg, Dependencies: store, Network: extensions.NetPolicy{Allow: networks}, Timeout: 5 * time.Second}
		var providers []envctx.Provider
		for i, pc := range local.ContextProviders {
			p, err := envctx.Build(pc, deps)
			if err != nil {
				return fmt.Errorf("local config context_providers[%d]: %w", i, err)
			}
			providers = append(providers, p)
		}
		envGatherer = &envctx.Gatherer{Providers: providers, Log: cfg.Log.With("component", "envctx")}
		gatherer = envGatherer
	}
	offload := local.OutputOffloadBytes
	if offload == 0 {
		offload = kernel.DefaultOffloadBytes
	}
	plannerSvc := &planner.Service{Gateway: modelGW, Artifacts: arts, Profiles: store,
		Log: cfg.Log.With("component", "planner"), Observer: metrics,
		Config: planner.Config{ModelAlias: "sre-reasoning", MaxRepairs: 2, MaxNodes: 20,
			MaxOutputTokens: 8000, MaxChildren: 8, MaxTotalRuns: 24, MaxDepth: 3, MaxIterations: 8,
			MaxModelCalls: 40, MaxEstimatedTokens: 400000, ApprovalMode: "auto", ApprovalApprovers: 1,
			FallbackProfileID: "sre-diagnosis", FallbackVersion: 8, FallbackReact: true, Router: router,
			Cache: planner.NewMemoryCache(10*time.Minute, 256)}}

	worker := kernel.NewWorker(kernel.Config{ID: "local-worker", Compactor: localCompactor(modelGW, cfg.Log), LeaseTTL: 30 * time.Second,
		StepTimeout: 2 * time.Minute, Checkpoints: true, Rules: rules, Planner: plannerSvc,
		Memory: retriever, PlanMemory: retriever, CapabilityMap: graphSvc,
		Skills: extRT, ToolToggles: store, PlanApprovers: 1, PlanApprovalMode: "auto", Observer: metrics,
		Context: gatherer, OutputOffloadBytes: offload, Strategies: strategies,
		Verifier: &verify.Verifier{}, Calibration: &reasoning.CalibrationService{Store: store}},
		store, arts, strategy, reg, pol, cfg.Log)
	pool := kernel.NewPool(worker, cfg.MaxRuns)

	tokens, err := auth.LoadStaticTokens(cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("load local token: %w", err)
	}
	server := &api.Server{Store: store, Artifacts: arts, DefaultProfile: "sre-diagnosis", DefaultVersion: 7,
		AutoProfileID: "sre-diagnosis", AutoProfileVersion: 10, PollInterval: 250 * time.Millisecond,
		StreamMode: api.StreamPoll, Log: cfg.Log, Rules: rules.Rules, Tools: reg, Policy: pol, Metrics: metrics,
		Graph:        graphSvc,
		Auth:         &auth.Chain{Authenticators: []auth.Authenticator{tokens}},
		BranchPolicy: domain.BranchQueue, MaxPendingTurn: 8}

	server.Extend(&extensions.API{Runtime: extRT, Registry: reg, Toggles: store,
		DefaultRules: func(ctx context.Context, tenantID string) (profile.Rules, string, error) {
			r, err := rules.Rules(ctx, tenantID, "sre-diagnosis", 10)
			return r, "sre-diagnosis@10", err
		},
		ResolveModel: func(tenantID string) (string, string, error) {
			ep, err := modelGW.Resolve(gateway.Request{ModelAlias: "sre-reasoning"}, gateway.Scope{TenantID: tenantID})
			return ep.ID, ep.ModelID, err
		}})
	curator := &curate.Curator{Memories: curatewire.Memories(store), History: runHistory, Store: store}
	server.Extend(&api.MemoryAPI{Store: store, Writer: memory.Writer{Embedder: embedder, Artifacts: arts}, Server: server, Curator: curator})
	server.Extend(&api.EvalAPI{Service: evalSvc, Store: store, Server: server})
	// Reasoning quality, pattern library and on-demand improvement. The
	// desktop runtime never improves on a schedule.
	server.Extend(&api.ReasoningAPI{Store: store, Evolution: evalSvc, Server: server,
		Defaults: improve.Config{ProfileID: "sre-diagnosis"}})
	memWriter := memory.Writer{Embedder: embedder, Artifacts: arts}
	consolidator := &memory.Consolidator{Store: store, Artifacts: arts, Writer: memWriter,
		SideEffecting: func(name string) bool {
			t, ok := reg.Lookup(name)
			return !ok || t.Descriptor().Risk.HasSideEffects()
		}, Interval: 5 * time.Second, Log: cfg.Log.With("component", "consolidator"),
		Hooks:   []memory.RunHook{reasoning.NewRecorder(store, arts, memWriter, cfg.Log.With("component", "reasoning"))},
		Indexer: &memory.Indexer{Store: store, Embedder: embedder, Log: cfg.Log.With("component", "memory_indexer")}}
	var triggers *trigger.Service
	if len(local.Triggers) > 0 {
		if triggers, err = trigger.New(local.Triggers, server, store, cfg.Log.With("component", "trigger")); err != nil {
			return fmt.Errorf("local config triggers: %w", err)
		}
		server.Extend(&trigger.API{Service: triggers})
	}
	if err := extRT.Sync(ctx); err != nil {
		cfg.Log.Warn("initial extensions sync", "err", err)
	}
	if err := graphSvc.Sync(ctx, tenant); err != nil {
		cfg.Log.Warn("initial graph sync", "err", err)
	}
	go func() {
		if err := extRT.Run(ctx); err != nil && ctx.Err() == nil {
			cfg.Log.Warn("extensions loop", "err", err)
		}
	}()

	work, err := buildLocalWork(ctx, cfg, local, server, pol, metrics, embedder)
	if err != nil {
		return err
	}
	if work != nil {
		work.engine.MirrorTriggers(triggers, work.flags)
		work.engine.AttachContext(envGatherer)
		retriever.Env = work.engine.MemoryEnv
		defer work.close()
		go func() {
			if err := work.engine.Run(ctx); err != nil && ctx.Err() == nil {
				cfg.Log.Error("work engine stopped", "err", err)
			}
		}()
	}
	if cfg.OnReady != nil {
		cfg.OnReady()
	}
	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		cfg.Log.Info("local harness listening", "addr", cfg.HTTPAddr, "db", cfg.DBPath)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("local http", "err", err)
		}
	}()
	go runLoop(ctx, store, pool, cfg.Log)
	go reconcileLoop(ctx, store, graphSvc, cfg.Log)
	go curateLoop(ctx, curator, curateInterval(os.Getenv), cfg.Log.With("component", "curator"))
	go func() {
		if err := consolidator.Run(ctx); err != nil && ctx.Err() == nil {
			cfg.Log.Warn("consolidator", "err", err)
		}
	}()
	if triggers != nil {
		go func() {
			if err := triggers.RunSchedules(ctx); err != nil && ctx.Err() == nil {
				cfg.Log.Warn("trigger schedules", "err", err)
			}
		}()
	}

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdown)
	pool.Wait()
	return nil
}

func runLoop(ctx context.Context, store *localstore.Store, pool *kernel.Pool, log *slog.Logger) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for _, id := range store.RunnableRuns() {
				pool.Handle(ctx, id, func(bool, time.Duration) {})
			}
		}
	}
}

func reconcileLoop(ctx context.Context, store *localstore.Store, graphSvc *graph.Service, log *slog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := store.ExpireApprovals(ctx, 100); err != nil {
				log.Warn("expire approvals", "err", err)
			}
			if _, err := store.RecoverExpiredLeases(ctx, 100); err != nil {
				log.Warn("recover leases", "err", err)
			}
			if err := graphSvc.Sync(ctx, tenant); err != nil {
				log.Warn("graph sync", "err", err)
			}
			if _, err := store.PurgeTriggerClaims(ctx, time.Now(), 500); err != nil {
				log.Warn("purge trigger claims", "err", err)
			}
			if _, err := store.DeleteOldReasoningScores(ctx, time.Now().Add(-retention.DefaultReasoningRetention), 1000); err != nil {
				log.Warn("prune reasoning scores", "err", err)
			}
		}
	}
}

func seedProfiles(ctx context.Context, store *localstore.Store) error {
	type seed struct {
		version int64
		spec    string
	}
	for _, p := range []seed{{1, profiles.DemoSpec}, {7, profiles.CapabilityMapSpec}, {8, profiles.FlowSpec},
		{9, profiles.InteractiveFlowSpec}, {10, profiles.AutoSpec}} {
		if _, err := store.RegisterProfile(ctx, tenant, "sre-diagnosis", p.version, json.RawMessage(p.spec)); err != nil {
			return fmt.Errorf("seed profile %d: %w", p.version, err)
		}
	}
	// The auto profile is the default channel version; rollout/canary
	// resolution reads this flag.
	if err := store.SetActiveVersion(ctx, tenant, "sre-diagnosis", 10); err != nil {
		return fmt.Errorf("set active profile: %w", err)
	}
	return nil
}

func buildGateway(arts *artifact.FS, ledger gateway.Ledger, log *slog.Logger) (*gateway.Gateway, error) {
	var providers []gateway.Provider
	anthropicEnabled, openaiEnabled := false, false
	openaiModel := ""
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		providers = append(providers, anthropic.New(anthropic.Config{APIKey: key, BaseURL: os.Getenv("HARNESS_ANTHROPIC_BASE_URL")}))
		anthropicEnabled = true
	}
	if url := os.Getenv("HARNESS_OPENAI_BASE_URL"); url != "" {
		openaiModel = os.Getenv("HARNESS_OPENAI_MODEL")
		providers = append(providers, openai.New(openai.Config{BaseURL: url, APIKey: os.Getenv("HARNESS_OPENAI_API_KEY")}))
		openaiEnabled = true
	}
	if key := os.Getenv("GEMINI_API_KEY"); key != "" {
		model := os.Getenv("HARNESS_GEMINI_MODEL")
		if model == "" {
			model = "gemini-flash-latest"
		}
		providers = append(providers, gemini.New(gemini.Config{APIKey: key, BaseURL: os.Getenv("HARNESS_GEMINI_BASE_URL")}))
		_ = model // Gemini uses an endpoint; the default policy below is extended by env providers only.
	}
	providers = append(providers, cognitive.ScriptedModel(cognitive.RuleBased{}))
	pol := gateway.DefaultPolicy(anthropicEnabled, openaiEnabled, openaiModel)
	return gateway.New(pol, ledger, arts, gateway.Admission{}, log, providers...), nil
}

// orPatterns prefers patterns from the local file over the environment.
func orPatterns(fromFile, fromEnv []string) []string {
	if len(fromFile) > 0 {
		return fromFile
	}
	return fromEnv
}

func FirstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

// curateInterval is how often the curator proposes clean-ups
// (HARNESS_CURATE_INTERVAL; default 24h; "off" or 0 disables the schedule,
// the API and CLI still run it on demand).
func curateInterval(getenv func(string) string) time.Duration {
	v := strings.TrimSpace(getenv("HARNESS_CURATE_INTERVAL"))
	switch strings.ToLower(v) {
	case "":
		return 24 * time.Hour
	case "off", "0", "false":
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < time.Minute {
		return 24 * time.Hour
	}
	return d
}

// curateLoop proposes memory clean-ups one minute after start and then every
// interval. It only creates proposals; a person decides.
func curateLoop(ctx context.Context, c *curate.Curator, every time.Duration, log *slog.Logger) {
	if every <= 0 {
		return
	}
	wait := time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		rep, err := c.Run(ctx, tenant)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Warn("memory curation failed", "err", err)
		case rep.Total() > 0:
			log.Info("memory curation proposed clean-ups", "new", rep.Total(), "examined", rep.Examined)
		}
		wait = every
	}
}
