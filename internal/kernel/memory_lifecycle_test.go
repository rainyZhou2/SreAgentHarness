package kernel_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/tools/memsearch"
)

func TestWorkerCarriesCurrentMemoryEnvironmentToPinnedContext(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current memory.Env
		known   bool
		want    int
	}{
		{"known", memory.Env{Environment: "prod", Versions: map[string]string{"driver": "550"}}, true, 1},
		{"unknown", memory.Env{}, false, 0},
		{"unknown-version", memory.Env{Environment: "prod"}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			current := memory.Env{Environment: "prod", Versions: map[string]string{"driver": "550"}}
			known := true
			retriever := &memory.Retriever{Embedder: memory.HashingEmbedder{Dims: 32}, Env: func(context.Context, string, string) (memory.Env, bool) { return current, known }}
			agent := &scripted{decide: func(_ int, in cognitive.ReasoningInput) (domain.Decision, error) {
				if len(in.Memories) != tc.want {
					t.Errorf("pinned memories=%+v want %d", in.Memories, tc.want)
				}
				return finalDecision("done"), nil
			}}
			env := newLoopEnv(t, kernel.Config{Memory: retriever}, agent, "", nil)
			retriever.Store, retriever.Artifacts = env.st, env.arts
			prod := "prod"
			_, _, err := env.st.CreateMemory(ctx, memory.NewMemory{TenantID: "t1", Scope: "organization", Type: "semantic", Status: "ACTIVE", Subject: "checkout", Title: "checkout slow", Content: "checkout is slow", ContentHash: "env-memory", Trust: "T2", Sensitivity: "internal", Environment: &prod, Applicability: &memory.Applicability{RiskOfReuse: "high", VersionConstraints: []string{"driver>=550"}}})
			if err != nil {
				t.Fatal(err)
			}
			run, _, err := env.st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1, GoalRef: env.run.GoalRef, GoalHash: "env-goal", IdempotencyKey: "env-run", InitiatorType: "user"})
			if err != nil {
				t.Fatal(err)
			}
			env.run = run
			if err := env.w.ExecuteOne(ctx, run.ID); err != nil {
				t.Fatal(err)
			} // retrieve with known environment
			current, known = tc.current, tc.known
			if got := env.drive(t, 5); got.Status != domain.RunCompleted {
				t.Fatalf("run=%s", got.Status)
			}
			if len(agent.inputs) != 1 {
				t.Fatalf("reasoning calls=%d", len(agent.inputs))
			}
		})
	}
}

func TestHistoricalMemorySearchRevalidatesWithoutChangingAudit(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{true: "batch", false: "direct"}[batch], func(t *testing.T) {
			ctx := context.Background()
			retriever := &memory.Retriever{Embedder: memory.HashingEmbedder{Dims: 32}}
			var env *loopEnv
			var memoryID string
			agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
				switch n {
				case 0:
					if batch {
						return toolDecision(memsearch.Name, `{"query":"checkout"}`, domain.ToolCall{Name: "metrics.a", Args: json.RawMessage(`{}`)}), nil
					}
					return toolDecision(memsearch.Name, `{"query":"checkout"}`), nil
				case 1:
					if len(in.RecalledMemories) != 1 {
						t.Fatalf("active search missing: %+v", in.RecalledMemories)
					}
					if _, err := env.st.TransitionMemory(ctx, "t1", memoryID, "ACTIVE", "STALE", "reviewer", "retracted"); err != nil {
						t.Fatal(err)
					}
					return toolDecision("metrics.b", `{}`), nil
				default:
					if len(in.RecalledMemories) != 0 {
						t.Errorf("revoked memory remains citable: %+v", in.RecalledMemories)
					}
					for _, o := range in.Observations {
						if o.Tool == memsearch.Name && strings.Contains(string(o.Content), "obsolete advice") {
							t.Errorf("revoked content remains in model context: %s", o.Content)
						}
					}
					return finalDecision("done"), nil
				}
			}}
			env = newLoopEnv(t, kernel.Config{Memory: retriever, CompactEvery: -1, CompactTokens: -1}, agent, "", nil, memsearch.Tool{Retriever: retriever}, fixedTool{name: "metrics.a", out: `{"a":1}`}, fixedTool{name: "metrics.b", out: `{"b":1}`})
			retriever.Store, retriever.Artifacts = env.st, env.arts
			m, _, err := env.st.CreateMemory(ctx, memory.NewMemory{TenantID: "t1", Scope: "organization", Type: "semantic", Status: "ACTIVE", Subject: "checkout", Title: "checkout", Content: "checkout obsolete advice", ContentHash: "recall-memory", Trust: "T2", Sensitivity: "internal"})
			if err != nil {
				t.Fatal(err)
			}
			memoryID = m.ID
			if got := env.drive(t, 15); got.Status != domain.RunCompleted {
				t.Fatalf("run=%s", got.Status)
			}
			history, err := env.st.History(ctx, "t1", env.run.ID, 999)
			if err != nil {
				t.Fatal(err)
			}
			audited := false
			for _, step := range history {
				if step.OutputRef != nil {
					raw, err := env.arts.Get(ctx, "t1", *step.OutputRef)
					if err != nil {
						t.Fatal(err)
					}
					audited = audited || strings.Contains(string(raw), "obsolete advice")
				}
			}
			if !audited {
				t.Fatal("historical raw memory search audit was lost")
			}
		})
	}
}
