package kernel_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/runsearch"
	"go-sre-harness/internal/tools"
	runsearchtool "go-sre-harness/internal/tools/runsearch"
)

// An agent in a later run finds how an earlier run ended through the real
// kernel path (call context, store, artifacts), sees its own earlier chats
// and never another user's.
func TestRunSearchToolFindsEarlierRunsInTheLoop(t *testing.T) {
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	arts := artifact.NewMemory()
	search := runsearchtool.Tool{Search: &runsearch.Searcher{Runs: st, Artifacts: arts}, Runs: st}

	var searchOut string
	byRun := map[string]string{} // run id -> what it should do
	agent := &scripted{decide: func(n int, in cognitive.ReasoningInput) (domain.Decision, error) {
		switch byRun[in.Call.RunID] {
		case "search":
			for _, o := range in.Observations {
				if o.Tool == runsearchtool.Name {
					searchOut = string(o.Content)
					return finalDecision("found it", "obs:step1"), nil
				}
			}
			return toolDecision(runsearchtool.Name, `{"query":"connection pool exhaustion"}`), nil
		default:
			return finalDecision(byRun[in.Call.RunID]), nil
		}
	}}
	cfg := kernel.Config{ID: "w1", Rules: &profile.RulesFor{Loader: st}}
	w := kernel.NewWorker(cfg, st, arts, agent, tools.NewRegistry(search, kernel.BatchTool{}), policy.Engine{}, nil)
	if _, err := st.RegisterProfile(ctx, "t1", "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":40}`)); err != nil {
		t.Fatal(err)
	}
	start := func(key, user, goal, behaviour string) string {
		g, _ := json.Marshal(map[string]string{"goal": goal})
		ref, hash, _ := arts.Put(ctx, "t1", "goal", g)
		run, _, err := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1,
			GoalRef: ref, GoalHash: hash, IdempotencyKey: key, InitiatorType: "user", InitiatorID: user,
			InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash, Status: domain.StepPending}}})
		if err != nil {
			t.Fatal(err)
		}
		byRun[run.ID] = behaviour
		for i := 0; i < 12; i++ {
			_ = w.ExecuteOne(ctx, run.ID)
			if r, _ := st.GetRun(ctx, "t1", run.ID); r.Status.Terminal() {
				if r.Status != domain.RunCompleted {
					t.Fatalf("run %s ended %s", key, r.Status)
				}
				break
			}
		}
		return run.ID
	}
	aliceOld := start("a1", "alice", "checkout latency high", "Probable cause: DB connection pool exhaustion after deploy d-881")
	bobOld := start("b1", "bob", "checkout latency", "Bob's chat: connection pool exhaustion too, secret plan")
	start("a2", "alice", "checkout slow again", "search")

	if searchOut == "" {
		t.Fatal("the agent never received a run_search result")
	}
	var out runsearchtool.Output
	if err := json.Unmarshal([]byte(searchOut), &out); err != nil {
		t.Fatalf("%v: %s", err, searchOut)
	}
	var ids []string
	for _, h := range out.Hits {
		ids = append(ids, h.RunID)
	}
	if len(out.Hits) != 1 || out.Hits[0].RunID != aliceOld {
		t.Fatalf("alice must find exactly her earlier run %s, got %v", aliceOld, ids)
	}
	if strings.Contains(searchOut, bobOld) || strings.Contains(searchOut, "secret plan") {
		t.Fatalf("another user's chat leaked into the result: %s", searchOut)
	}
	if !strings.Contains(out.Hits[0].Answer, "d-881") {
		t.Fatalf("the conclusion must come back: %+v", out.Hits[0])
	}
}
