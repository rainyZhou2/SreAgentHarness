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
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/gateway"
	"go-sre-harness/internal/gateway/anthropic"
	"go-sre-harness/internal/gateway/gemini"
	"go-sre-harness/internal/gateway/openai"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
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
	gateway, err := buildGateway(arts, cfg.Log)
	if err != nil {
		return err
	}
	reg := tools.NewRegistry(tools.DemoTools()...)
	pol := policy.Default()
	rules := &profile.RulesFor{Loader: store}
	metrics := obs.NewMetrics()

	assembler := cognitive.Assembler{PromptPrefixVersion: "sre-prefix-v6",
		ProfileRef: "sre-diagnosis@7", MaxContextTokens: 120_000,
		MaxObservationBytes: 16_000, MaxOutputTokens: 8_000}
	strategy := &cognitive.LLM{Gateway: gateway, Artifacts: arts, Assembler: assembler, Alias: "sre-reasoning"}
	plannerSvc := &planner.Service{Gateway: gateway, Artifacts: arts, Profiles: store,
		Log: cfg.Log.With("component", "planner"), Observer: metrics,
		Config: planner.Config{ModelAlias: "sre-reasoning", MaxRepairs: 2, MaxNodes: 20,
			MaxOutputTokens: 8000, MaxChildren: 8, MaxTotalRuns: 24, MaxDepth: 3, MaxIterations: 8,
			MaxModelCalls: 40, MaxEstimatedTokens: 400000, ApprovalMode: "auto", ApprovalApprovers: 1,
			FallbackProfileID: "sre-diagnosis", FallbackVersion: 8, FallbackReact: true}}

	worker := kernel.NewWorker(kernel.Config{ID: "local-worker", LeaseTTL: 30 * time.Second,
		StepTimeout: 2 * time.Minute, Checkpoints: false, Rules: rules, Planner: plannerSvc,
		PlanApprovers: 1, PlanApprovalMode: "auto", Observer: metrics}, store, arts, strategy, reg, pol, cfg.Log)
	pool := kernel.NewPool(worker, cfg.MaxRuns)

	tokens, err := auth.LoadStaticTokens(cfg.TokenFile)
	if err != nil {
		return fmt.Errorf("load local token: %w", err)
	}
	server := &api.Server{Store: store, Artifacts: arts, DefaultProfile: "sre-diagnosis", DefaultVersion: 7,
		AutoProfileID: "sre-diagnosis", AutoProfileVersion: 10, PollInterval: 250 * time.Millisecond,
		StreamMode: api.StreamPoll, Log: cfg.Log, Rules: rules.Rules, Tools: reg, Policy: pol, Metrics: metrics,
		Auth:         &auth.Chain{Authenticators: []auth.Authenticator{tokens}},
		BranchPolicy: domain.BranchQueue, MaxPendingTurn: 8}

	httpSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		cfg.Log.Info("local harness listening", "addr", cfg.HTTPAddr, "db", cfg.DBPath)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			cfg.Log.Error("local http", "err", err)
		}
	}()
	go runLoop(ctx, store, pool, cfg.Log)
	go reconcileLoop(ctx, store, cfg.Log)

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

func reconcileLoop(ctx context.Context, store *localstore.Store, log *slog.Logger) {
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
	return nil
}

func buildGateway(arts *artifact.FS, log *slog.Logger) (*gateway.Gateway, error) {
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
	return gateway.New(pol, gateway.NewMemLedger(), arts, gateway.Admission{}, log, providers...), nil
}

func FirstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
