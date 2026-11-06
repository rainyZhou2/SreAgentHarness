package kernel_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/tools"
)

func metricsCall(svc string) *domain.ToolCall {
	args, _ := json.Marshal(map[string]string{"service": svc})
	return &domain.ToolCall{Name: "demo.metrics", Args: args}
}

// Compaction replaces older observations with a summary that keeps safety facts,
// and the summary is what later reasoning steps see.
func TestCompactionSummarizesOldObservations(t *testing.T) {
	var mu sync.Mutex
	var sawSummary string
	var lastObs int
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		mu.Lock()
		defer mu.Unlock()
		if in.Summary != "" {
			sawSummary = in.Summary
		}
		lastObs = len(in.Observations)
		calls := len(in.Observations)
		if in.CompactedBefore > 0 {
			calls = 99 // after compaction: conclude
		}
		if calls < 4 {
			return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "gather", Tool: metricsCall("checkout")}, nil
		}
		return domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", Answer: "concluded"}, nil
	}}
	h := start(t, opts{strategy: strat, worker: kernel.Config{CompactEvery: 3}})
	run := h.create(t, "investigate service=checkout")
	h.waitStatus(t, run.ID, domain.RunCompleted)
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	compacts := 0
	for _, s := range steps {
		if s.Kind == domain.StepCompact {
			compacts++
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if compacts != 1 || !strings.Contains(sawSummary, "demo.metrics") || lastObs > 2 {
		t.Fatalf("compacts=%d lastObs=%d summary=%q", compacts, lastObs, sawSummary)
	}
	r := h.run(t, run.ID)
	if r.CompactionEpoch != 1 || !has(checkEvents(t, h, run.ID), "context.compacted") {
		t.Fatalf("compaction epoch = %d", r.CompactionEpoch)
	}
}

// A fork continues from a committed checkpoint with the source's history.
func TestForkFromCheckpoint(t *testing.T) {
	h := start(t, opts{worker: kernel.Config{Checkpoints: true}})
	src := h.create(t, "diagnose latency service=checkout")
	h.waitStatus(t, src.ID, domain.RunCompleted)
	cps, err := h.store.ListCheckpoints(context.Background(), tenant, src.ID)
	if err != nil || len(cps) < 2 {
		t.Fatalf("checkpoints = %d err=%v", len(cps), err)
	}
	// Fork after the metrics tool step: the fork must not call metrics again.
	var afterTool pg.Checkpoint
	for _, c := range cps {
		if c.LastCommittedStepSeq == 2 {
			afterTool = c
		}
	}
	fork, created, err := h.store.ForkRun(context.Background(), pg.ForkRunRequest{TenantID: tenant, SourceRunID: src.ID,
		CheckpointID: afterTool.ID, IdempotencyKey: "fork-1", ActorID: "bob"})
	if err != nil || !created {
		t.Fatal(err)
	}
	done := h.waitStatus(t, fork.ID, domain.RunCompleted)
	answer, _ := h.result(t, done)["answer"].(string)
	if !strings.Contains(answer, "pool exhaustion") {
		t.Fatalf("fork answer = %q", answer)
	}
	steps, _ := h.store.ListSteps(context.Background(), tenant, fork.ID)
	if len(steps) != 1 || steps[0].Seq != 3 || steps[0].Kind != domain.StepReason {
		t.Fatalf("fork steps = %+v", steps)
	}
	hist, _ := h.store.History(context.Background(), tenant, fork.ID, 100)
	if len(hist) != 3 {
		t.Fatalf("fork history = %d steps, want parent's 2 + own 1", len(hist))
	}
	if !has(checkEvents(t, h, fork.ID), "run.forked") {
		t.Fatal("missing run.forked")
	}
}

func pendingApproval(t *testing.T, h *harness, runID string) domain.ApprovalRequest {
	t.Helper()
	list, err := h.store.ListApprovals(context.Background(), tenant, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		if a.Status == "PENDING" {
			return a
		}
	}
	t.Fatal("no pending approval")
	return domain.ApprovalRequest{}
}

// replace_pending_input recomputes the intent server-side, supersedes the old
// approval and requires a new one bound to the new action hash.
func TestReplacePendingInputRequiresFreshApproval(t *testing.T) {
	h := start(t, opts{})
	run := h.create(t, "remediate latency service=checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	old := pendingApproval(t, h, run.ID)

	// What the API's prepareCommand computes for {"service":"payments"}.
	call := domain.ToolCall{Name: "demo.restart", Args: json.RawMessage(`{"service":"payments"}`)}
	cj, _ := json.Marshal(call)
	ref, hash, _ := h.arts.Put(context.Background(), tenant, "tool-input", cj)
	pol := policy.Default()
	tool, _ := h.reg.Lookup("demo.restart")
	d := tool.Descriptor()
	pd := pol.Evaluate(&d, tools.Target(d, call.Args))
	intent := pol.BuildIntent(d, call, hash, pd)
	payload, _ := json.Marshal(map[string]string{"step_id": old.StepID})
	_, err := h.store.ApplyCommand(context.Background(), tenant, run.ID, domain.CommandRequest{
		Type: domain.CmdReplacePendingInput, IdempotencyKey: "replace-1", ActorID: "alice", Reason: "wrong service",
		ExpectedVersion: h.ver(t, run.ID), Payload: payload,
		Internal: &domain.CommandInternal{InputRef: ref, InputHash: hash, Intent: &intent, RequiredApprovals: 1}})
	if err != nil {
		t.Fatal(err)
	}
	fresh := pendingApproval(t, h, run.ID)
	if fresh.ID == old.ID || fresh.ActionHash == old.ActionHash || fresh.Target != "payments" {
		t.Fatalf("fresh approval = %+v (old %+v)", fresh, old)
	}
	// The old approval can no longer be used.
	_, err = h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": old.ID,
		"action_hash": old.ActionHash}, h.ver(t, run.ID))
	if err == nil {
		t.Fatal("superseded approval was accepted")
	}
	if _, err := h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": fresh.ID,
		"action_hash": fresh.ActionHash}, h.ver(t, run.ID)); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunCompleted)
}

// override_result records a revision; later reasoning sees the override.
func TestOverrideResult(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		mu.Lock()
		defer mu.Unlock()
		for _, o := range in.Observations {
			seen = append(seen, string(o.Content))
		}
		switch len(in.Observations) {
		case 0:
			return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "m", Tool: metricsCall("checkout")}, nil
		case 1:
			args, _ := json.Marshal(map[string]string{"service": "checkout"})
			return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "r", Tool: &domain.ToolCall{Name: "demo.restart", Args: args}}, nil
		}
		return domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "f", Answer: "ok"}, nil
	}}
	h := start(t, opts{strategy: strat})
	run := h.create(t, "remediate service=checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	metricsStep := steps[1]

	obs, _ := json.Marshal(domain.Observation{Outcome: "ok", Content: json.RawMessage(`{"corrected":true}`), TrustTag: "T1"})
	ref, hash, _ := h.arts.Put(context.Background(), tenant, "observation", obs)
	payload, _ := json.Marshal(map[string]string{"step_id": metricsStep.ID})
	if _, err := h.store.ApplyCommand(context.Background(), tenant, run.ID, domain.CommandRequest{
		Type: domain.CmdOverrideResult, IdempotencyKey: "ovr-1", ActorID: "alice", Reason: "sensor was wrong",
		ExpectedVersion: h.ver(t, run.ID), Payload: payload,
		Internal: &domain.CommandInternal{OutputRef: ref, OutputHash: hash}}); err != nil {
		t.Fatal(err)
	}
	a := pendingApproval(t, h, run.ID)
	if _, err := h.cmd(t, run.ID, domain.CmdRejectTool, map[string]string{"approval_request_id": a.ID,
		"action_hash": a.ActionHash}, h.ver(t, run.ID)); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunCompleted)
	revs, _ := h.store.StepOutputRevisions(context.Background(), tenant, metricsStep.ID)
	if len(revs) != 2 || revs[0].Source != "EXECUTION" || revs[1].Source != "HUMAN_OVERRIDE" {
		t.Fatalf("revisions = %+v", revs)
	}
	mu.Lock()
	defer mu.Unlock()
	if last := seen[len(seen)-2]; last != `{"corrected":true}` {
		t.Fatalf("later reasoning saw %q", last)
	}
}
