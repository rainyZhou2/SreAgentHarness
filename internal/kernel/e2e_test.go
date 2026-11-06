package kernel_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/notify"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/scheduler"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
	"go-sre-harness/internal/tools"
)

const tenant = testutil.Tenant

var quiet = slog.New(slog.NewTextHandler(logOut(), nil))

type harness struct {
	store *pg.Store
	arts  *artifact.Memory
	reg   *tools.Registry
}

type opts struct {
	cfg      pg.Config
	strategy cognitive.Strategy
	// strategyFn builds a strategy that needs the store (e.g. the gateway ledger).
	strategyFn func(h *harness) cognitive.Strategy
	// workerFn adjusts the worker config once the store exists.
	workerFn func(h *harness, c *kernel.Config)
	tools    []tools.Tool
	worker   kernel.Config
}

// start runs outbox publisher, reconciler and a worker pool in-process.
func start(t *testing.T, o opts) *harness {
	t.Helper()
	if o.cfg.MaxStepsPerRun == 0 {
		o.cfg = pg.DefaultConfig()
	}
	if o.strategy == nil {
		o.strategy = cognitive.RuleBased{}
	}
	if o.tools == nil {
		o.tools = tools.DemoTools()
	}
	if o.worker.ID == "" {
		o.worker.ID = "test-worker"
	}
	if o.worker.LeaseTTL == 0 {
		o.worker.LeaseTTL = 5 * time.Second
	}
	h := &harness{store: testutil.Store(t, o.cfg), arts: artifact.NewMemory(), reg: tools.NewRegistry(o.tools...)}
	if o.strategyFn != nil {
		o.strategy = o.strategyFn(h)
	}
	if o.workerFn != nil {
		o.workerFn(h, &o.worker)
	}
	ctx, cancel := context.WithCancel(context.Background())
	broker := notify.NewLocal(1024)
	w := kernel.NewWorker(o.worker, h.store, h.arts, o.strategy, h.reg, policy.Default(), quiet)
	pool := kernel.NewPool(w, 8)
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		_ = (&scheduler.Publisher{Store: h.store, Broker: broker, Interval: 20 * time.Millisecond, Log: quiet}).Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = (&scheduler.Reconciler{Store: h.store, Interval: 50 * time.Millisecond, StaleIdle: time.Second, Log: quiet}).Run(ctx)
	}()
	go func() { defer wg.Done(); _ = broker.Run(ctx, pool.Handle); pool.Wait() }()
	t.Cleanup(func() { cancel(); wg.Wait() })
	return h
}

func (h *harness) create(t *testing.T, goal string) domain.Run {
	t.Helper()
	data, _ := json.Marshal(map[string]string{"goal": goal})
	ref, hash, err := h.arts.Put(context.Background(), tenant, "goal", data)
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := h.store.CreateRun(context.Background(), domain.CreateRunRequest{TenantID: tenant, ProfileID: "sre-diagnosis",
		ProfileVersion: 1, GoalRef: ref, GoalHash: hash, IdempotencyKey: goal + time.Now().String(), InitiatorID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func (h *harness) run(t *testing.T, id string) domain.Run {
	t.Helper()
	r, err := h.store.GetRun(context.Background(), tenant, id)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func (h *harness) waitStatus(t *testing.T, id string, want domain.RunStatus) domain.Run {
	t.Helper()
	var r domain.Run
	testutil.Eventually(t, 10*time.Second, func() bool { r = h.run(t, id); return r.Status == want },
		"run %s: want %s", id, want)
	return r
}

func (h *harness) cmd(t *testing.T, runID string, typ domain.CommandType, payload any, expected *int64) (domain.Command, error) {
	t.Helper()
	p, _ := json.Marshal(payload)
	return h.store.ApplyCommand(context.Background(), tenant, runID, domain.CommandRequest{Type: typ,
		IdempotencyKey: fmt.Sprintf("%s-%d", typ, time.Now().UnixNano()), ActorID: "alice", Payload: p, ExpectedVersion: expected})
}

// ver returns the run's current version for commands that must carry expected_version.
func (h *harness) ver(t *testing.T, id string) *int64 {
	v := h.run(t, id).Version
	return &v
}

func (h *harness) result(t *testing.T, r domain.Run) map[string]any {
	t.Helper()
	if r.ResultRef == nil {
		t.Fatal("completed run has no result")
	}
	data, err := h.arts.Get(context.Background(), tenant, *r.ResultRef)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return out
}

func checkEvents(t *testing.T, h *harness, runID string) []domain.Event {
	t.Helper()
	events, err := h.store.ListEvents(context.Background(), tenant, runID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range events {
		if e.Seq != int64(i+1) {
			t.Fatalf("event log gap at %d: seq %d", i, e.Seq)
		}
	}
	return events
}

func has(events []domain.Event, typ string) bool {
	for _, e := range events {
		if e.Type == typ {
			return true
		}
	}
	return false
}

func TestDiagnosisRunCompletes(t *testing.T) {
	h := start(t, opts{})
	run := h.create(t, "diagnose latency service=checkout")
	r := h.waitStatus(t, run.ID, domain.RunCompleted)
	answer, _ := h.result(t, r)["answer"].(string)
	if !strings.Contains(answer, "pool exhaustion") {
		t.Fatalf("unexpected answer: %q", answer)
	}
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	if len(steps) != 3 {
		t.Fatalf("expected reason → metrics → reason, got %d steps", len(steps))
	}
	for _, s := range steps {
		if s.Kind == domain.StepReason {
			if s.ContextSnapshotID == nil {
				t.Fatal("reason step without context snapshot")
			}
			snap, err := h.store.GetSnapshot(context.Background(), tenant, *s.ContextSnapshotID)
			if err != nil || snap.ContextHash == nil {
				t.Fatalf("snapshot must record the rendered context hash: %+v %v", snap, err)
			}
		}
	}
	events := checkEvents(t, h, run.ID)
	if !has(events, "run.completed") || !has(events, "step.succeeded") {
		t.Fatal("missing lifecycle events")
	}
}

func TestApprovalFlow(t *testing.T) {
	restartCalls := 0
	var mu sync.Mutex
	ts := tools.DemoTools()
	restart := ts[1].(*tools.Func)
	inner := restart.Fn
	restart.Fn = func(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
		mu.Lock()
		restartCalls++
		mu.Unlock()
		return inner(ctx, key, args)
	}
	h := start(t, opts{tools: ts})
	run := h.create(t, "remediate service=checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	approvals, _ := h.store.ListApprovals(context.Background(), tenant, run.ID)
	if len(approvals) != 1 || approvals[0].RiskClass != domain.RiskR2 || approvals[0].Status != "PENDING" {
		t.Fatalf("expected one pending R2 approval, got %+v", approvals)
	}
	a := approvals[0]
	pending, err := h.store.ListPendingApprovals(context.Background(), tenant, 200)
	found := false
	for _, p := range pending {
		found = found || (p.ID == a.ID && p.RunID == run.ID && p.ToolName == a.ToolName && p.SessionID == nil)
	}
	if err != nil || !found {
		t.Fatalf("tenant inbox must list the pending approval: %v %+v", err, pending)
	}

	c, err := h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": a.ID, "action_hash": "sha256:other"}, h.ver(t, run.ID))
	var rej *domain.RejectedError
	if !errors.As(err, &rej) || rej.Reason != "action_hash_mismatch" || c.Status != domain.CommandRejected {
		t.Fatalf("approval of a different action must be rejected: %v %+v", err, c)
	}
	if h.run(t, run.ID).Status != domain.RunWaitingApproval {
		t.Fatal("mismatched approval must not unblock the run")
	}
	if _, err := h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": a.ID, "action_hash": a.ActionHash}, h.ver(t, run.ID)); err != nil {
		t.Fatal(err)
	}
	r := h.waitStatus(t, run.ID, domain.RunCompleted)
	if answer := h.result(t, r)["answer"].(string); !strings.Contains(answer, "restart") {
		t.Fatalf("answer should mention the approved action: %q", answer)
	}
	mu.Lock()
	if restartCalls != 1 {
		t.Fatalf("approved tool must run exactly once, ran %d", restartCalls)
	}
	mu.Unlock()
	if _, err := h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": a.ID, "action_hash": a.ActionHash}, h.ver(t, run.ID)); !errors.As(err, &rej) {
		t.Fatalf("approving a decided request again must be rejected, got %v", err)
	}
	var consumed int
	h.store.Pool().QueryRow(context.Background(), `SELECT coalesce(sum(used_count),0) FROM approval_grants`).Scan(&consumed)
	if consumed != 1 {
		t.Fatalf("grant must be consumed exactly once, got %d", consumed)
	}
	var effects string
	h.store.Pool().QueryRow(context.Background(), `SELECT string_agg(state, ',') FROM external_tool_effects`).Scan(&effects)
	if effects != "CONFIRMED" {
		t.Fatalf("side effect must be recorded as CONFIRMED, got %q", effects)
	}
	checkEvents(t, h, run.ID)
}

func TestRejectedApprovalReplans(t *testing.T) {
	h := start(t, opts{})
	run := h.create(t, "remediate service=checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	approvals, _ := h.store.ListApprovals(context.Background(), tenant, run.ID)
	if _, err := h.cmd(t, run.ID, domain.CmdRejectTool, map[string]string{"approval_request_id": approvals[0].ID,
		"action_hash": approvals[0].ActionHash}, h.ver(t, run.ID)); err != nil {
		t.Fatal(err)
	}
	r := h.waitStatus(t, run.ID, domain.RunCompleted)
	if answer := h.result(t, r)["answer"].(string); !strings.Contains(answer, "not executed") {
		t.Fatalf("agent must observe the rejection: %q", answer)
	}
}

func TestApprovalTimeoutExpires(t *testing.T) {
	cfg := pg.DefaultConfig()
	cfg.ApprovalTTL = 300 * time.Millisecond
	h := start(t, opts{cfg: cfg})
	run := h.create(t, "remediate service=checkout")
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	r := h.waitStatus(t, run.ID, domain.RunCompleted)
	approvals, _ := h.store.ListApprovals(context.Background(), tenant, run.ID)
	if approvals[0].Status != "EXPIRED" {
		t.Fatalf("expected EXPIRED approval, got %s", approvals[0].Status)
	}
	if answer := h.result(t, r)["answer"].(string); !strings.Contains(answer, "not executed") {
		t.Fatalf("expired approval must not execute the action: %q", answer)
	}
}

func toolDecision(name, service string) domain.Decision {
	args, _ := json.Marshal(map[string]string{"service": service})
	return domain.Decision{Kind: domain.DecisionTool, ReasonSummary: "call " + name, Tool: &domain.ToolCall{Name: name, Args: args}}
}

func final(answer string) domain.Decision {
	return domain.Decision{Kind: domain.DecisionFinal, ReasonSummary: "done", Answer: answer}
}

func TestToolErrorAfterDispatchNeedsHumanAndIsNotRetried(t *testing.T) {
	flaky := &tools.Func{Desc: tools.Descriptor{Name: "t.scale", Version: "1", Risk: domain.RiskR1, TargetArg: "service"},
		Fn: func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
			return nil, errors.New("connection reset after request was sent")
		}}
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		if len(in.Observations) == 0 {
			return toolDecision("t.scale", "checkout"), nil
		}
		return final("done"), nil
	}}
	h := start(t, opts{strategy: strat, tools: []tools.Tool{flaky}})
	run := h.create(t, "scale checkout")
	// R1 requires approval by default.
	h.waitStatus(t, run.ID, domain.RunWaitingApproval)
	a, _ := h.store.ListApprovals(context.Background(), tenant, run.ID)
	if _, err := h.cmd(t, run.ID, domain.CmdApproveTool, map[string]string{"approval_request_id": a[0].ID, "action_hash": a[0].ActionHash}, h.ver(t, run.ID)); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunNeedsHuman)
	time.Sleep(300 * time.Millisecond) // give the system a chance to (wrongly) retry
	if flaky.Calls() != 1 {
		t.Fatalf("an effect of unknown outcome must not be retried automatically; calls=%d", flaky.Calls())
	}
	var state string
	h.store.Pool().QueryRow(context.Background(), `SELECT state FROM external_tool_effects`).Scan(&state)
	if state != "UNKNOWN" {
		t.Fatalf("effect must be UNKNOWN, got %s", state)
	}
	r := h.run(t, run.ID)
	steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID)
	if _, err := h.cmd(t, run.ID, domain.CmdResolveUnknown, map[string]string{"step_id": steps[1].ID, "resolution": "failed",
		"note": "verified in console: not applied"}, &r.Version); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunCompleted)
	if flaky.Calls() != 1 {
		t.Fatal("resolving as failed must not re-invoke the tool")
	}
}

func TestArtifactStoreOutageDoesNotCommit(t *testing.T) {
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) { return final("ok"), nil }}
	h := start(t, opts{strategy: strat})
	h.arts.SetFailPuts(false)
	run := h.create(t, "diagnose")
	h.arts.SetFailPuts(true)
	testutil.Eventually(t, 5*time.Second, func() bool { return strat.Calls() >= 1 }, "strategy not called")
	time.Sleep(200 * time.Millisecond)
	if r := h.run(t, run.ID); r.Status == domain.RunCompleted {
		t.Fatal("a step whose artifacts were not durably stored must not commit")
	}
	h.arts.SetFailPuts(false)
	h.waitStatus(t, run.ID, domain.RunCompleted)
	attempts, _ := h.store.ListAttempts(context.Background(), tenant, run.ID)
	if len(attempts) < 2 || attempts[0].Status == domain.AttemptSucceeded {
		t.Fatalf("expected a failed attempt before success: %+v", attempts)
	}
}

func TestEvidenceIsPinnedPerSnapshot(t *testing.T) {
	release := make(chan struct{})
	var mu sync.Mutex
	var seen [][]domain.Evidence
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		mu.Lock()
		seen = append(seen, in.Evidence)
		n := len(seen)
		mu.Unlock()
		if n == 1 {
			<-release
			return toolDecision("demo.metrics", "checkout"), nil
		}
		return final("ok"), nil
	}}
	h := start(t, opts{strategy: strat})
	run := h.create(t, "diagnose service=checkout")
	testutil.Eventually(t, 5*time.Second, func() bool { return strat.Calls() == 1 }, "first reason step not started")
	if _, err := h.cmd(t, run.ID, domain.CmdInjectEvidence, map[string]string{"content": "deploy at 10:02", "source": "change-log"}, nil); err != nil {
		t.Fatal(err)
	}
	close(release)
	h.waitStatus(t, run.ID, domain.RunCompleted)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 || len(seen[0]) != 0 || len(seen[1]) != 1 || seen[1][0].Content != "deploy at 10:02" {
		t.Fatalf("evidence must appear only in the next snapshot: %+v", seen)
	}
}

func TestPauseAndResumeAtStepBoundary(t *testing.T) {
	release := make(chan struct{})
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		if len(in.Observations) == 0 {
			<-release
			return toolDecision("demo.metrics", "checkout"), nil
		}
		return final("ok"), nil
	}}
	h := start(t, opts{strategy: strat})
	run := h.create(t, "diagnose service=checkout")
	testutil.Eventually(t, 5*time.Second, func() bool { return strat.Calls() == 1 }, "step not started")
	if _, err := h.cmd(t, run.ID, domain.CmdPause, nil, nil); err != nil {
		t.Fatal(err)
	}
	if r := h.run(t, run.ID); r.DisplayStatus() != "PAUSE_REQUESTED" {
		t.Fatalf("want PAUSE_REQUESTED while step runs, got %s", r.DisplayStatus())
	}
	close(release)
	r := h.waitStatus(t, run.ID, domain.RunPaused)
	time.Sleep(200 * time.Millisecond)
	if steps, _ := h.store.ListSteps(context.Background(), tenant, run.ID); steps[1].Status != domain.StepPending {
		t.Fatalf("paused run must not start the next step: %s", steps[1].Status)
	}
	if _, err := h.cmd(t, run.ID, domain.CmdResume, nil, &r.Version); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunCompleted)
}

func TestCancelRunningStep(t *testing.T) {
	slow := &tools.Func{Desc: tools.Descriptor{Name: "t.slowread", Version: "1", Risk: domain.RiskR0, TargetArg: "service"},
		Fn: func(ctx context.Context, _ string, _ json.RawMessage) (json.RawMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
	strat := &cognitive.Scripted{Fn: func(in cognitive.ReasoningInput) (domain.Decision, error) {
		return toolDecision("t.slowread", "checkout"), nil
	}}
	h := start(t, opts{strategy: strat, tools: []tools.Tool{slow},
		worker: kernel.Config{HeartbeatInterval: 50 * time.Millisecond}})
	run := h.create(t, "diagnose")
	testutil.Eventually(t, 5*time.Second, func() bool { return slow.Calls() == 1 }, "tool not started")
	if _, err := h.cmd(t, run.ID, domain.CmdCancel, nil, nil); err != nil {
		t.Fatal(err)
	}
	h.waitStatus(t, run.ID, domain.RunCancelled)
	attempts, _ := h.store.ListAttempts(context.Background(), tenant, run.ID)
	if last := attempts[len(attempts)-1]; last.Status != domain.AttemptCancelled {
		t.Fatalf("running attempt must end CANCELLED, got %s", last.Status)
	}
}

// ---------- API ----------

func TestAPI(t *testing.T) {
	h := start(t, opts{})
	srv := httptest.NewServer((&api.Server{Store: h.store, Artifacts: h.arts, DefaultProfile: "sre-diagnosis",
		DefaultVersion: 1, PollInterval: 20 * time.Millisecond, Log: quiet}).Handler())
	defer srv.Close()

	do := func(method, path, key, body string, hdr ...string) (*http.Response, map[string]any) {
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", tenant)
		req.Header.Set("X-Actor-ID", "alice")
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		for i := 0; i+1 < len(hdr); i += 2 {
			req.Header.Set(hdr[i], hdr[i+1])
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp, out
	}

	if resp, _ := do("POST", "/v1/runs", "", `{"goal":"x"}`); resp.StatusCode != 400 {
		t.Fatalf("missing Idempotency-Key: want 400, got %d", resp.StatusCode)
	}
	req, _ := http.NewRequest("GET", srv.URL+"/v1/runs/x", nil)
	if resp, _ := http.DefaultClient.Do(req); resp.StatusCode != 401 {
		t.Fatalf("missing identity: want 401, got %d", resp.StatusCode)
	}
	resp, body := do("POST", "/v1/runs", "k1", `{"goal":"remediate service=checkout"}`)
	if resp.StatusCode != 202 {
		t.Fatalf("create: %d %v", resp.StatusCode, body)
	}
	runID := body["run_id"].(string)
	if _, again := do("POST", "/v1/runs", "k1", `{"goal":"remediate service=checkout"}`); again["run_id"] != runID || again["created"] != false {
		t.Fatalf("idempotent create must return the same run: %v", again)
	}
	if resp, _ := do("GET", "/v1/runs/"+runID, "", "", "X-Tenant-ID", "tenant-b"); resp.StatusCode != 404 {
		t.Fatalf("cross-tenant read: want 404, got %d", resp.StatusCode)
	}
	h.waitStatus(t, runID, domain.RunWaitingApproval)

	if resp, out := do("POST", "/v1/runs/"+runID+"/commands", "c1", `{"type":"pause","expected_version":1}`); resp.StatusCode != 409 {
		t.Fatalf("stale expected_version: want 409, got %d %v", resp.StatusCode, out)
	}
	_, list := do("GET", "/v1/runs/"+runID+"/approvals", "", "")
	appr := list["approvals"].([]any)[0].(map[string]any)
	_, cur := do("GET", "/v1/runs/"+runID, "", "")
	curV := cur["run"].(map[string]any)["version"]
	bad, _ := json.Marshal(map[string]any{"type": "approve_tool", "expected_version": curV,
		"payload": map[string]any{"approval_request_id": appr["id"], "action_hash": "sha256:other"}})
	if resp, out := do("POST", "/v1/runs/"+runID+"/commands", "c2", string(bad)); resp.StatusCode != 422 {
		t.Fatalf("approval of a different action: want 422, got %d %v", resp.StatusCode, out)
	}
	payload, _ := json.Marshal(map[string]any{"type": "approve_tool", "expected_version": curV, "payload": map[string]any{
		"approval_request_id": appr["id"], "action_hash": appr["action_hash"]}})
	resp, cmd := do("POST", "/v1/runs/"+runID+"/commands", "c3", string(payload))
	if resp.StatusCode != 202 {
		t.Fatalf("approve: %d %v", resp.StatusCode, cmd)
	}
	if resp, got := do("GET", fmt.Sprintf("/v1/runs/%s/commands/%s", runID, cmd["id"]), "", ""); resp.StatusCode != 200 || got["status"] != "APPLIED" {
		t.Fatalf("get command: %d %v", resp.StatusCode, got)
	}

	// SSE: resume from Last-Event-ID 2, stream ends after the terminal event.
	sreq, _ := http.NewRequest("GET", srv.URL+"/v1/runs/"+runID+"/events/stream", nil)
	sreq.Header.Set("X-Tenant-ID", tenant)
	sreq.Header.Set("X-Actor-ID", "alice")
	sreq.Header.Set("Last-Event-ID", "2")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sresp, err := http.DefaultClient.Do(sreq.WithContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer sresp.Body.Close()
	if ct := sresp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	var ids []string
	var lastType string
	sc := bufio.NewScanner(sresp.Body)
	for sc.Scan() {
		line := sc.Text()
		if v, ok := strings.CutPrefix(line, "id: "); ok {
			ids = append(ids, v)
		}
		if v, ok := strings.CutPrefix(line, "event: "); ok {
			lastType = v
		}
	}
	if len(ids) == 0 || ids[0] != "3" || lastType != "run.completed" {
		t.Fatalf("SSE must resume after Last-Event-ID and end with run.completed: first=%v last=%s", ids[:min(1, len(ids))], lastType)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] != fmt.Sprint(3+i) {
			t.Fatalf("SSE ids must be contiguous: %v", ids)
		}
	}
	_, final := do("GET", "/v1/runs/"+runID, "", "")
	if final["display_status"] != "COMPLETED" || final["result"] == nil {
		t.Fatalf("final run: %v", final)
	}
}

func logOut() io.Writer {
	if os.Getenv("HARNESS_TEST_LOG") != "" {
		return os.Stderr
	}
	return io.Discard
}
