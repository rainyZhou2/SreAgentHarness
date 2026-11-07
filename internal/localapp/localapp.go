// Package localapp assembles the single-process harness used by the desktop
// local runtime. It has no PostgreSQL, NATS, scheduler process or plugin host:
// the SQLite store, kernel worker, API and event loop live in one process.
package localapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
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
	"go-sre-harness/internal/tools"
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
		return errors.New("local token file is required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.MaxRuns <= 0 {
		cfg.MaxRuns = 8
	}

	store, err := localstore.Open(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("open local store: %w", err)
	}
	defer store.Close()
	if err := seedProfiles(ctx, store); err != nil {
		return err
	}

	arts := &artifact.FS{Root: cfg.DataDir + "/artifacts"}
	modelGW, err := buildGateway(arts, store, cfg.Log)
	if err != nil {
		return err
	}
	reg := tools.NewRegistry(tools.DemoTools()...)
	pol := policy.Default()
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
	embedder := memory.HashingEmbedder{Dims: 256}
	graphSvc := &graph.Service{Store: store, Tools: reg, Extensions: extRT, Log: cfg.Log.With("component", "graph")}
	retriever := &memory.Retriever{Store: store, Embedder: embedder, Artifacts: arts, TopK: 5}
	offlineGW, err := buildGateway(arts, gateway.NewMemLedger(), cfg.Log)
	if err != nil {
		return err
	}
	evalSvc := &eval.Service{Store: store, Artifacts: arts, Registry: reg, Log: cfg.Log.With("component", "eval"),
		Factory: func(spec profile.Spec) (cognitive.Strategy, error) {
			// Offline replay uses its own ledger: evaluation calls belong to no
			// run and must not be charged to the tenant.
			return &cognitive.LLM{Gateway: offlineGW, Artifacts: arts, Assembler: assembler, Alias: "sre-reasoning"}, nil
		}}
	plannerSvc := &planner.Service{Gateway: modelGW, Artifacts: arts, Profiles: store,
		Log: cfg.Log.With("component", "planner"), Observer: metrics,
		Config: planner.Config{ModelAlias: "sre-reasoning", MaxRepairs: 2, MaxNodes: 20,
			MaxOutputTokens: 8000, MaxChildren: 8, MaxTotalRuns: 24, MaxDepth: 3, MaxIterations: 8,
			MaxModelCalls: 40, MaxEstimatedTokens: 400000, ApprovalMode: "auto", ApprovalApprovers: 1,
			FallbackProfileID: "sre-diagnosis", FallbackVersion: 8, FallbackReact: true}}

	worker := kernel.NewWorker(kernel.Config{ID: "local-worker", LeaseTTL: 30 * time.Second,
		StepTimeout: 2 * time.Minute, Checkpoints: true, Rules: rules, Planner: plannerSvc,
		Memory: retriever, CapabilityMap: graphSvc,
		Skills: extRT, ToolToggles: store, PlanApprovers: 1, PlanApprovalMode: "auto", Observer: metrics},
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
	server.Extend(&api.MemoryAPI{Store: store, Writer: memory.Writer{Embedder: embedder, Artifacts: arts}, Server: server})
	server.Extend(&api.EvalAPI{Service: evalSvc, Store: store, Server: server})
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

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		cfg.Log.Info("local harness listening", "addr", cfg.HTTPAddr, "db", cfg.DBPath)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("local http", "err", err)
		}
	}()
	go runLoop(ctx, store, pool, cfg.Log)
	go reconcileLoop(ctx, store, graphSvc, cfg.Log)

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

func FirstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
