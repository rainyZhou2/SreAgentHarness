package workengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go-sre-harness/internal/action"
	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
	"go-sre-harness/migrations"
)

type continuousKernelStore interface {
	api.Store
	kernel.StateStore
	memory.Store
	RegisterProfile(context.Context, string, string, int64, json.RawMessage) (bool, error)
}

type inferenceProbe struct{ calls int }

func (*inferenceProbe) Descriptor() tools.Descriptor {
	return tools.Descriptor{Name: "inference.probe", Version: "1", Risk: domain.RiskR0, Trust: "T2", InputSchema: json.RawMessage(`{"type":"object"}`)}
}
func (p *inferenceProbe) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	p.calls++
	return json.RawMessage(`{"ttft_p95":1.8,"gpu_xid":0,"kv_usage":0.92,"queue":42}`), nil
}

type continuousInvestigator struct{}

func (continuousInvestigator) Name() string { return "scripted-inference" }

func (continuousInvestigator) Decide(_ context.Context, in cognitive.ReasoningInput) (domain.Decision, error) {
	if len(in.Observations) == 0 {
		return domain.Decision{SchemaVersion: 1, Kind: domain.DecisionTool, ReasonSummary: "collect current inference telemetry", Tool: &domain.ToolCall{Name: "inference.probe", Args: json.RawMessage(`{}`)}}, nil
	}
	return domain.Decision{SchemaVersion: 1, Kind: domain.DecisionFinal, ReasonSummary: "investigation complete", Answer: "TTFT and KV pressure correlate; root cause remains unconfirmed", Hypotheses: []domain.Hypothesis{{ID: "pressure", Statement: "KV pressure causes TTFT", Status: "VERIFIED", Supports: []string{"unverified:model-claim"}}}}, nil
}

type unknownScale struct {
	world      *world.Service
	calls      int
	effectFile string
	crash      bool
}

func (*unknownScale) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "serving.scale_prefill", Version: "1", Risk: domain.RiskR2, ResourceKinds: []string{"deployment"}, IdempotencyMode: action.IdemNative, BlastRadius: "single_resource"}
}
func (*unknownScale) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}
func (e *unknownScale) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	e.calls++
	now := e.world.Clock.Now()
	if e.effectFile != "" {
		f, err := os.OpenFile(e.effectFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, err
		}
		_, writeErr := fmt.Fprintln(f, key)
		syncErr, closeErr := f.Sync(), f.Close()
		if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
			return nil, err
		}
		if e.crash {
			if err := os.WriteFile(e.effectFile+".clock", []byte(now.Format(time.RFC3339Nano)), 0600); err != nil {
				return nil, err
			}
			os.Exit(91) // The remote effect survived; no action result or deferred cleanup did.
		}
	}
	if _, _, err := e.world.Store.AssertFact(ctx, world.Fact{TenantID: p.TenantID, SubjectRef: p.TargetRef, Predicate: "healthy", Value: "yes", Source: "serving-probe", TrustTier: world.TrustFirstParty, ObservedAt: now, ValidFrom: now}); err != nil {
		return nil, err
	}
	return nil, errors.New("response lost after applying scale")
}
func (e *unknownScale) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	applied := true
	if e.effectFile != "" {
		data, err := os.ReadFile(e.effectFile)
		if err != nil {
			return action.InspectResult{}, err
		}
		applied = false
		for _, key := range strings.Fields(string(data)) {
			applied = applied || key == p.EffectKey
		}
		if applied {
			now := e.world.Clock.Now()
			if _, _, err := e.world.Store.AssertFact(ctx, world.Fact{TenantID: p.TenantID, SubjectRef: p.TargetRef, Predicate: "healthy", Value: "yes", Source: "serving-probe", TrustTier: world.TrustFirstParty, ObservedAt: now, ValidFrom: now}); err != nil {
				return action.InspectResult{}, err
			}
		}
	}
	return action.InspectResult{Applied: &applied}, nil
}

func TestContinuousWorkKernelFailover(t *testing.T) {
	for _, backend := range []string{"local", "postgres", "local-process", "postgres-process"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			phase := os.Getenv("HARNESS_CONTINUOUS_PROCESS_PHASE")
			process := strings.HasSuffix(backend, "-process")
			root := os.Getenv("HARNESS_CONTINUOUS_PROCESS_ROOT")
			if !process || phase == "" {
				root = t.TempDir()
			}
			if process {
				if phase == "" {
					database := ""
					if backend == "postgres-process" {
						database = kernelTestDatabase(t, ctx)
					}
					for _, next := range []string{"crash", "recover"} {
						cmdCtx, cancel := context.WithTimeout(ctx, time.Minute)
						cmd := exec.CommandContext(cmdCtx, os.Args[0], "-test.run=^TestContinuousWorkKernelFailover$/^"+backend+"$")
						cmd.Env = append(os.Environ(), "HARNESS_CONTINUOUS_PROCESS_PHASE="+next, "HARNESS_CONTINUOUS_PROCESS_ROOT="+root, "HARNESS_CONTINUOUS_PROCESS_DATABASE="+database)
						out, err := cmd.CombinedOutput()
						cancel()
						var ee *exec.ExitError
						if next == "crash" {
							if !errors.As(err, &ee) || ee.ExitCode() != 91 {
								t.Fatalf("effect crash: %v\n%s", err, out)
							}
						} else if err != nil {
							t.Fatalf("process recovery: %v\n%s", err, out)
						}
					}
					return
				}
				root = os.Getenv("HARNESS_CONTINUOUS_PROCESS_ROOT")
				if root == "" {
					t.Fatal("missing process state directory")
				}
			}
			var cp *cpsql.Store
			var ks continuousKernelStore
			if strings.HasPrefix(backend, "local") {
				var err error
				cp, err = cpsql.OpenSQLite(filepath.Join(root, "work.db"))
				if err != nil {
					t.Fatal(err)
				}
				local, err := localstore.Open(filepath.Join(root, "kernel.db"))
				if err != nil {
					t.Fatal(err)
				}
				ks = local
				t.Cleanup(func() { _ = local.Close() })
			} else {
				raw := os.Getenv("HARNESS_PG_TEST_URL")
				if raw == "" {
					t.Skip("HARNESS_PG_TEST_URL not set")
				}
				name := os.Getenv("HARNESS_CONTINUOUS_PROCESS_DATABASE")
				if !process {
					name = kernelTestDatabase(t, ctx)
				}
				if name == "" {
					t.Fatal("missing isolated process database")
				}
				cfg, err := pgxpool.ParseConfig(raw)
				if err != nil {
					t.Fatal(err)
				}
				cfg.ConnConfig.Database = name
				pool, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(pool.Close)
				if err := migrations.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}
				cp, ks = cpsql.OpenPostgres(pool), pg.New(pool, pg.Config{MemoryRetrieval: true})
			}
			t.Cleanup(func() { _ = cp.Close() })
			now := time.Now().UTC()
			if process && phase == "recover" {
				b, err := os.ReadFile(filepath.Join(root, "effects.clock"))
				if err != nil {
					t.Fatal(err)
				}
				now, err = time.Parse(time.RFC3339Nano, string(b))
				if err != nil {
					t.Fatal(err)
				}
				now = now.Add(3 * time.Minute)
			}
			clock := cpkit.Clock(func() time.Time { return now })
			cp.Clock = clock.Now
			const tenant = "continuous"
			const target = "deployment://prod/sglang"
			if _, err := ks.RegisterProfile(ctx, tenant, "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":20}`)); err != nil {
				t.Fatal(err)
			}
			arts := &artifact.FS{Root: filepath.Join(root, "artifacts")}
			server := &api.Server{Store: ks, Artifacts: arts, DefaultProfile: "p", DefaultVersion: 1}
			runs := KernelRuns{Server: server, WorkAuthority: cp}
			ws := &world.Service{Store: cp, Clock: clock}
			exec := &unknownScale{world: ws}
			if process {
				exec.effectFile, exec.crash = filepath.Join(root, "effects"), phase == "crash"
			}
			reg := action.NewRegistry()
			if err := reg.Register(exec); err != nil {
				t.Fatal(err)
			}
			eval := &outcome.Evaluator{States: orchestration.WorldFacts{World: ws}, Events: cp}
			rt := &action.Runtime{Store: cp, Registry: reg, World: ws, Outcomes: eval, Clock: clock, Policy: policy.Engine{Version: "test", Environment: "production"}}
			tpl := work.Template{ID: "sglang-live", RepeatFrom: "observe", Stages: []work.Stage{
				{ID: "observe", Type: work.StageWait, Wait: &continuous.Spec{Mode: continuous.OnEvent, Predicate: &eventing.Predicate{EventTypes: []string{"inference.ttft_breach"}}}},
				{ID: "investigate", Type: work.StageRun, Run: &work.RunSpec{Goal: "Diagnose TTFT/GPU/KV on ${subjects}", ProfileID: "p", ProfileVersion: 1}},
				{ID: "mitigate", Type: work.StageAction, Action: &work.ActionSpec{Name: "serving.scale_prefill", Target: target, Args: json.RawMessage(`{"replicas":6}`)}},
				{ID: "verify", Type: work.StageVerify, Verify: &work.VerifySpec{RecheckEvery: cpkit.Duration(5 * time.Second)}},
			}}
			cat, err := orchestration.NewCatalog([]work.Template{tpl})
			if err != nil {
				t.Fatal(err)
			}
			svc := &orchestration.Service{Store: cp, Runs: runs, Catalog: cat, Clock: clock}
			w, _, err := svc.Create(ctx, orchestration.CreateRequest{TenantID: tenant, ActorID: "owner", IdempotencyKey: "work", Kind: work.KindAIInfra, TemplateID: tpl.ID, Goal: work.GoalSpec{Intent: "maintain inference", SubjectRefs: []string{target}, Success: &outcome.Spec{StableFor: cpkit.Duration(5 * time.Second), AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, Freshness: cpkit.Duration(time.Minute), State: &outcome.StateExpr{Predicate: "healthy", Equals: "yes"}}}}}})
			if err != nil {
				t.Fatal(err)
			}
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			orch := &orchestration.Orchestrator{Store: cp, Runs: runs, Actions: rt, Evaluator: eval, Clock: clock, Owner: "work-a", RunPoll: time.Second, Log: log}
			ing := &orchestration.Ingestor{Store: cp, Catalog: cat, Clock: clock, Log: log}
			probe := &inferenceProbe{}
			newWorker := func(id string) *kernel.Worker {
				return kernel.NewWorker(kernel.Config{ID: id, Memory: &memory.Retriever{Store: ks, Artifacts: arts, Embedder: memory.HashingEmbedder{}}}, ks, arts, continuousInvestigator{}, tools.NewRegistry(probe), policy.Engine{Environment: "production"}, nil)
			}
			worker := newWorker("kernel-a")
			if _, err := orch.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			var oldApproval string
			var crashed bool
			for cycle := 1; cycle <= 2; cycle++ {
				for delivery := 0; delivery < 2; delivery++ {
					if process && phase == "recover" && cycle == 1 {
						continue
					}
					if _, _, err := ing.Ingest(ctx, eventing.Envelope{ID: fmt.Sprintf("anomaly-%d", cycle), TenantID: tenant, Source: "inference", Type: "inference.ttft_breach", SubjectRef: target, OccurredAt: now}); err != nil {
						t.Fatal(err)
					}
				}
				for tick := 0; tick < 40; tick++ {
					if _, err := orch.Tick(ctx); err != nil {
						t.Fatal(err)
					}
					links, err := cp.ListRunLinks(ctx, tenant, w.ID)
					if err != nil {
						t.Fatal(err)
					}
					for _, link := range links {
						if link.Generation != cycle {
							continue
						}
						if cycle == 2 && !crashed {
							lease, err := ks.ClaimStep(ctx, link.RunID, "dead-worker", time.Millisecond)
							if err != nil {
								t.Fatal(err)
							}
							time.Sleep(5 * time.Millisecond)
							worker = newWorker("kernel-b")
							orch.Owner = "work-b"
							if err := worker.ExecuteOne(ctx, link.RunID); err != nil {
								t.Fatal(err)
							}
							if err := ks.CommitSuccess(ctx, lease, domain.StepSuccess{Complete: true}); !errors.Is(err, domain.ErrLeaseLost) {
								t.Fatalf("old worker commit: %v", err)
							}
							crashed = true
						} else if err := worker.ExecuteOne(ctx, link.RunID); err != nil && !errors.Is(err, domain.ErrNoRunnableStep) {
							t.Fatal(err)
						}
					}
					actions, err := cp.ListActions(ctx, tenant, w.ID)
					if err != nil {
						t.Fatal(err)
					}
					for _, a := range actions {
						if a.State != action.StateWaitingApproval {
							continue
						}
						if oldApproval != "" {
							if _, err := rt.Decide(ctx, tenant, a.ID, "approver", "approve", oldApproval, "old", "owner"); !errors.Is(err, action.ErrApprovalMismatch) {
								t.Fatalf("stale approval accepted: %v", err)
							}
						}
						if _, err := rt.Decide(ctx, tenant, a.ID, "approver", "approve", a.IntentHash, "current", "owner"); err != nil {
							t.Fatal(err)
						}
						if err := cp.WakeWork(ctx, tenant, w.ID, "approval", "user", "approver", nil); err != nil {
							t.Fatal(err)
						}
						oldApproval = a.IntentHash
					}
					w, err = cp.GetWork(ctx, tenant, w.ID)
					if err != nil {
						t.Fatal(err)
					}
					if w.Generation == cycle+1 && w.Status == work.StatusWaiting {
						break
					}
					now = now.Add(5 * time.Second)
				}
				if w.Generation != cycle+1 || w.Status != work.StatusWaiting {
					t.Fatalf("cycle %d: %+v", cycle, w)
				}
			}
			wantCalls := 2
			if process && phase == "recover" {
				wantCalls = 1
				b, err := os.ReadFile(exec.effectFile)
				if err != nil {
					t.Fatal(err)
				}
				keys := strings.Fields(string(b))
				if len(keys) != 2 || keys[0] == keys[1] {
					t.Fatalf("effects repeated or missing after process exit: %q", b)
				}
			}
			if !crashed || exec.calls != wantCalls || probe.calls != wantCalls || w.RunsStarted != 2 {
				t.Fatalf("crash=%v dispatch=%d probes=%d runs=%d", crashed, exec.calls, probe.calls, w.RunsStarted)
			}
			report, err := (&postmortem.Builder{Ledger: cp}).Build(ctx, tenant, w.ID)
			if err != nil {
				t.Fatal(err)
			}
			if report.RootCause != nil {
				t.Fatal("model claim became confirmed root cause")
			}
			var cursor int64
			for {
				events, err := cp.ListWorkEvents(ctx, tenant, w.ID, cursor, 3)
				if err != nil {
					t.Fatal(err)
				}
				if len(events) == 0 {
					break
				}
				for _, e := range protocol.FromWorkEvents(events) {
					if e.Sequence != cursor+1 {
						t.Fatalf("timeline gap after %d: %+v", cursor, e)
					}
					cursor = e.Sequence
				}
			}
			if cursor != w.LastSeq {
				t.Fatalf("resume lost timeline: %d != %d", cursor, w.LastSeq)
			}
		})
	}
}

func kernelTestDatabase(t *testing.T, ctx context.Context) string {
	t.Helper()
	raw := os.Getenv("HARNESS_PG_TEST_URL")
	if raw == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "continuous_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error(err)
		}
	})
	return name
}
