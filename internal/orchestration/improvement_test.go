package orchestration

import (
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/runbook"
	"go-sre-harness/internal/work"
)

// §12.3: every follow-up becomes a tracked Work, once.
func TestImprovementWorksAreDurableAndIdempotent(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	src := h.create("mitigate", nil)
	items := []Improvement{{Title: "Add a P99 burn-rate alert", Why: "the incident was found by a customer", Priority: "high", Owner: "team-search"},
		{Title: "Fix HPA requests", Priority: "low"}}
	if _, err := h.svc.CreateImprovements(h.ctx, src, "alice", items); err == nil {
		t.Fatal("improvements need a closed source Work")
	}
	if _, err := h.command(src.ID, work.CmdCancel, nil); err != nil {
		t.Fatal(err)
	}
	src, _ = h.store.GetWork(h.ctx, "t1", src.ID)
	first, err := h.svc.CreateImprovements(h.ctx, src, "alice", items)
	if err != nil || len(first) != 2 {
		t.Fatalf("create: %v", err)
	}
	again, err := h.svc.CreateImprovements(h.ctx, src, "alice", items)
	if err != nil || again[0].ID != first[0].ID || again[1].ID != first[1].ID {
		t.Fatalf("asking twice must return the same Works: %v", err)
	}
	w := first[0]
	if w.Queue != work.QueueBackground || w.Goal.Labels["source_work"] != src.ID || w.Goal.Labels["owner"] != "team-search" ||
		w.Goal.Constraints.ProductionWrites != work.WritesApprovalRequired || w.Priority <= first[1].Priority || w.Status.Terminal() {
		t.Fatalf("improvement Work: %+v", w)
	}
	past := h.clock.Now().Add(-time.Hour)
	if _, err := h.svc.CreateImprovements(h.ctx, src, "alice", []Improvement{{Title: "x", Due: &past}}); err == nil {
		t.Fatal("a due date in the past must be refused")
	}
}

func TestRouteIntent(t *testing.T) {
	for text, want := range map[string]work.Kind{
		"给搜索增加 P99 告警规则":           work.KindMonitoring,
		"评估这次扩容成本":                 work.KindCapacity,
		"发布后观察 30 分钟":              work.KindChange,
		"检查 H200 卡健康":              work.KindAIInfra,
		"排查 Redis 超时":              work.KindIncident,
		"紧急回滚这次发布":                 work.KindIncident,
		"investigate checkout 5xx": work.KindIncident,
	} {
		if got := RouteIntent(text); !got.NeedsWork || got.Kind != want || got.Writes == work.WritesPreauthorized {
			t.Errorf("%q: %+v, want %s", text, got, want)
		}
	}
	if got := RouteIntent("what does the search service do?"); got.NeedsWork {
		t.Fatalf("a plain question needs no Work: %+v", got)
	}
	if got := RouteIntent("排查 Redis 超时"); got.Writes != work.WritesForbidden {
		t.Fatalf("an investigation starts read-only: %+v", got)
	}
}

// §12.2: an expired or unrehearsed runbook does not carry a production
// action; a fit one does, and the execution lands in its track record.
func TestRunbookGatesActionStage(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	tpl := mitigateTemplate()
	tpl.ID, tpl.Runbook = "rb-mitigate", &work.RunbookRef{ID: "incident.checkout-5xx"}
	h.withTemplates(tpl)
	spec := runbook.Spec{ID: "incident.checkout-5xx", Version: 1, Owner: "team-checkout",
		AppliesTo: runbook.AppliesTo{CompatibleActions: []string{"deployment.rollback"}},
		Safety:    runbook.Safety{HumanApproval: true, RehearsalRequired: true, ExpireAfter: cpkit.Duration(90 * 24 * time.Hour)},
		Lifecycle: runbook.Lifecycle{TemplateID: "rb-mitigate"}}
	if _, err := h.store.PutRunbook(h.ctx, "t1", spec, runbook.Active, 0); err != nil {
		t.Fatal(err)
	}
	now := h.clock.Now()
	for _, pred := range []string{"image", "generation"} {
		_ = pred
	}
	run := func() work.Work {
		w := h.create("rb-mitigate", nil)
		h.tick(1)
		h.runs.finishAll(map[string]any{"answer": "roll back", "proposed_action": map[string]any{"action": "deployment.rollback",
			"target": "deployment://prod/checkout", "args": map[string]any{"to": "v1"}}}, true)
		h.tick(3)
		got, _ := h.store.GetWork(h.ctx, "t1", w.ID)
		return got
	}
	w := run()
	if w.Status != work.StatusBlocked || !strings.Contains(w.Reason, "never rehearsed") || h.exec.calls != 0 {
		t.Fatalf("an unrehearsed runbook must not act: %s %q calls=%d", w.Status, w.Reason, h.exec.calls)
	}
	if _, err := h.store.RecordRehearsal(h.ctx, "t1", spec.ID, 1, now, &runbook.Approval{Approver: "sre-lead", At: now}); err != nil {
		t.Fatal(err)
	}
	w = run()
	if w.Status == work.StatusBlocked && strings.Contains(w.Reason, "runbook") {
		t.Fatalf("a fit runbook must pass the gate: %q", w.Reason)
	}
	if h.exec.calls != 1 {
		t.Fatalf("the action runs once under a fit runbook: %d", h.exec.calls)
	}
	h.clock.Add(100 * 24 * time.Hour)
	rb, _ := h.store.GetRunbook(h.ctx, "t1", spec.ID, 0)
	if probs := rb.Problems("deployment.rollback", h.clock.Now()); len(probs) == 0 {
		t.Fatal("a runbook past its validity must report it")
	}
	if probs := rb.Problems("node.drain", now); len(probs) != 1 || !strings.Contains(probs[0], "not one runbook") {
		t.Fatalf("an action the runbook was not written for: %v", probs)
	}
}
