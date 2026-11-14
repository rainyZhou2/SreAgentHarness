package kernel_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/tools"
	"go-sre-harness/migrations"
)

type workEffectStore interface {
	kernel.StateStore
	CreateRun(context.Context, domain.CreateRunRequest) (domain.Run, bool, error)
	RegisterProfile(context.Context, string, string, int64, json.RawMessage) (bool, error)
	ActivateWorkRun(context.Context, string, string, domain.WorkRunOwner) error
	LoadFlow(context.Context, string, string) (domain.FlowSnapshot, error)
}

type prepareCounter struct {
	kernel.StateStore
	prepared int
}

func (s *prepareCounter) PrepareEffect(ctx context.Context, l domain.Lease, target string) (domain.EffectState, string, error) {
	s.prepared++
	return s.StateStore.PrepareEffect(ctx, l, target)
}

func TestWorkOwnedDirectEffects(t *testing.T) {
	for _, backend := range []string{"local", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var store workEffectStore
			if backend == "local" {
				s, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				store = s
			} else {
				url := os.Getenv("HARNESS_PG_TEST_URL")
				if url == "" {
					t.Skip("HARNESS_PG_TEST_URL not set")
				}
				pool, err := pgxpool.New(ctx, url)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(pool.Close)
				if err := migrations.Apply(ctx, pool); err != nil {
					t.Fatal(err)
				}
				store = pg.New(pool, pg.Config{})
			}
			for _, mode := range []string{"owned", "legacy", "standalone"} {
				for _, child := range []bool{false, true} {
					for _, risk := range []domain.RiskClass{domain.RiskR0, domain.RiskR1} {
						name := mode + "/" + string(risk)
						if child {
							name += "/flow_child"
						}
						t.Run(name, func(t *testing.T) {
							tenant := uuid.NewString()
							if _, err := store.RegisterProfile(ctx, tenant, "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":40}`)); err != nil {
								t.Fatal(err)
							}
							arts := artifact.NewMemory()
							ref, hash, err := arts.Put(ctx, tenant, "goal", []byte(`{"goal":"investigate"}`))
							if err != nil {
								t.Fatal(err)
							}
							req := domain.CreateRunRequest{TenantID: tenant, ProfileID: "p", ProfileVersion: 1, IdempotencyKey: "root", GoalRef: ref, GoalHash: hash, InitiatorType: "user", InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash, Status: domain.StepPending}}}
							if mode != "standalone" {
								req.InitiatorType, req.InitiatorID = "work", "work:incident"
							}
							if mode == "owned" {
								req.PendingActivation = true
								req.WorkOwner = &domain.WorkRunOwner{WorkID: "incident", Generation: 1, StageID: "diagnose"}
							}
							run, _, err := store.CreateRun(ctx, req)
							if err != nil {
								t.Fatal(err)
							}
							if req.WorkOwner != nil {
								if err := store.ActivateWorkRun(ctx, tenant, run.ID, *req.WorkOwner); err != nil {
									t.Fatal(err)
								}
							}
							if child {
								lease, err := store.ClaimStep(ctx, run.ID, "flow", time.Minute)
								if err != nil {
									t.Fatal(err)
								}
								if err := store.CommitSuccess(ctx, lease, domain.StepSuccess{Wait: domain.WaitChildren, Flow: &domain.FlowCommit{Generation: 1, Status: "RUNNING", CompiledRef: ref, CompiledHash: hash, Children: []domain.ChildRunSpec{{NodeID: "child", NodeType: "agent", ProfileID: "p", ProfileVersion: 1, GoalRef: ref, GoalHash: hash}}}}); err != nil {
									t.Fatal(err)
								}
								flow, err := store.LoadFlow(ctx, tenant, run.ID)
								if err != nil || len(flow.Nodes) != 1 {
									t.Fatalf("flow: %+v %v", flow, err)
								}
								run.ID = flow.Nodes[0].ChildRunID
							}
							var calls int32
							tool := fixedTool{name: "probe", risk: risk, out: `{}`, calls: &calls}
							agent := &scripted{decide: func(n int, _ cognitive.ReasoningInput) (domain.Decision, error) {
								if n == 0 {
									return toolDecision("probe", `{"case":"`+tenant+`","work_owned":false}`), nil
								}
								return finalDecision("done"), nil
							}}
							counted := &prepareCounter{StateStore: store}
							worker := kernel.NewWorker(kernel.Config{ID: "test"}, counted, arts, agent, tools.NewRegistry(tool), policy.Engine{Environment: "staging", AllowR1WithoutApproval: true}, nil)
							for i := 0; i < 2; i++ {
								if err := worker.ExecuteOne(ctx, run.ID); err != nil {
									t.Fatal(err)
								}
							}
							wantCalls, wantPrepared := int32(1), 0
							if risk.HasSideEffects() {
								if mode == "standalone" {
									wantPrepared = 1
								} else {
									wantCalls = 0
								}
							}
							if got := atomic.LoadInt32(&calls); got != wantCalls || counted.prepared != wantPrepared {
								t.Fatalf("executor calls=%d want=%d; effect preparations=%d want=%d", got, wantCalls, counted.prepared, wantPrepared)
							}
						})
					}
				}
			}
		})
	}
}
