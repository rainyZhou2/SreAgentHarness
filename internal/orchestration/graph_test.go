package orchestration

import (
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

func incidentGraph() []work.Template {
	leaf := work.Template{ID: "leaf", Version: 1, Stages: []work.Stage{
		{ID: "do", Type: work.StageRun, Run: &work.RunSpec{Goal: "${goal.intent} for ${subjects}"}},
		{ID: "confirm", Type: work.StageConfirm}}}
	parent := work.Template{ID: "incident-graph", Version: 2, Kind: work.KindIncident,
		Graph: &work.GraphSpec{Children: []work.ChildSpec{
			{Key: "assess", Template: "leaf", Intent: "assess impact", Required: true},
			{Key: "mitigate", Template: "leaf", Intent: "mitigate", After: []string{"assess"}, Required: true,
				Constraints: work.Constraints{ProductionWrites: work.WritesApprovalRequired}},
			{Key: "rca", Template: "leaf", Intent: "find the root cause", After: []string{"assess"}, AfterKind: work.AfterTerminal},
		}},
		Stages: []work.Stage{
			{ID: "coordinate", Type: work.StageChildren, Phase: "mitigate", OnFailure: work.OnFailBlock},
			{ID: "confirm", Type: work.StageConfirm}}}
	return []work.Template{leaf, parent}
}

func (h *harness) withTemplates(ts ...work.Template) {
	cat, err := NewCatalog(append([]work.Template{mitigateTemplate()}, ts...))
	if err != nil {
		h.t.Fatal(err)
	}
	h.svc.Catalog, h.orch.Catalog = cat, cat
}

func (h *harness) tick(n int) {
	h.t.Helper()
	for i := 0; i < n; i++ {
		if _, err := h.orch.Tick(h.ctx); err != nil {
			h.t.Fatal(err)
		}
		h.clock.Add(6 * time.Second)
	}
}

// §8: mitigation and RCA are separate child Works. Mitigation waits for the
// assessment; closing the incident's mitigation does not wait for the RCA,
// and the parent does not succeed because its children did.
func TestChildrenStageRunsTheGraph(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.withTemplates(incidentGraph()...)
	parent := h.create("incident-graph", nil)
	h.tick(3)
	kids, err := h.store.Children(h.ctx, "t1", parent.ID)
	if err != nil || len(kids) != 3 {
		t.Fatalf("children: %d %v", len(kids), err)
	}
	byIntent := map[string]work.Work{}
	for _, k := range kids {
		byIntent[k.Goal.Intent] = k
	}
	get := func(intent string) work.Work {
		w, _ := h.store.GetWork(h.ctx, "t1", byIntent[intent].ID)
		return w
	}
	if w := get("mitigate"); w.Status != work.StatusNew || !strings.HasPrefix(w.Reason, "held") {
		t.Fatalf("mitigation must wait for the assessment: %s %q", w.Status, w.Reason)
	}
	if h.runs.started() != 1 {
		t.Fatalf("only the assessment may run: %d runs", h.runs.started())
	}
	// A wake command must not start a held Work.
	if _, err := h.svc.Command(h.ctx, CommandRequest{TenantID: "t1", WorkID: get("mitigate").ID, ActorID: "bob",
		IdempotencyKey: cpkit.NewID(), Type: work.CmdWake}); err != nil {
		t.Fatal(err)
	}
	h.tick(2)
	if w := get("mitigate"); w.Status != work.StatusNew || h.runs.started() != 1 {
		t.Fatalf("a wake started a held Work: %s", w.Status)
	}
	confirm := func(intent string) {
		h.runs.finishAll(map[string]any{"answer": "done"}, true)
		h.tick(3)
		if _, err := h.command(get(intent).ID, work.CmdConfirm, CommandPayload{Verdict: "satisfied"}); err != nil {
			t.Fatalf("confirm %s: %v", intent, err)
		}
		h.tick(3)
		if w := get(intent); w.Status != work.StatusSucceeded {
			t.Fatalf("%s: %s (%s)", intent, w.Status, w.Reason)
		}
	}
	confirm("assess impact")
	if h.runs.started() != 3 {
		t.Fatalf("mitigation and RCA start once the assessment ended: %d runs", h.runs.started())
	}
	confirm("mitigate")
	// The required children are done; the optional RCA is still open.
	p, _ := h.store.GetWork(h.ctx, "t1", parent.ID)
	if st, _ := p.CurrentStage(); st.ID != "confirm" || p.Status.Terminal() {
		t.Fatalf("the parent moves on but is not done: stage=%s status=%s", st.ID, p.Status)
	}
	if w := get("find the root cause"); w.Status.Terminal() {
		t.Fatalf("the RCA keeps its own life cycle: %s", w.Status)
	}
	g, _ := h.store.WorkGraph(h.ctx, "t1", parent.ID)
	if len(g.Nodes) != 4 || len(g.Edges) != 2 {
		t.Fatalf("graph: %d nodes, %d edges", len(g.Nodes), len(g.Edges))
	}
}

// A failed required child fails the stage; cancelling the parent cancels
// the children that are still open.
func TestChildFailureAndParentCancel(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.withTemplates(incidentGraph()...)
	parent := h.create("incident-graph", nil)
	h.tick(3)
	kids, _ := h.store.Children(h.ctx, "t1", parent.ID)
	var assess work.Work
	for _, k := range kids {
		if k.Goal.Intent == "assess impact" {
			assess = k
		}
	}
	if _, err := h.svc.Command(h.ctx, CommandRequest{TenantID: "t1", WorkID: assess.ID, ActorID: "bob",
		IdempotencyKey: cpkit.NewID(), Type: work.CmdCancel}); err != nil {
		t.Fatal(err)
	}
	h.tick(3)
	p, _ := h.store.GetWork(h.ctx, "t1", parent.ID)
	if p.Status != work.StatusBlocked || !strings.Contains(p.Reason, "required child works did not succeed") {
		t.Fatalf("parent: %s %q", p.Status, p.Reason)
	}
	kids, _ = h.store.Children(h.ctx, "t1", parent.ID)
	for _, k := range kids {
		switch k.Goal.Intent {
		case "mitigate":
			if k.Status != work.StatusCancelled {
				t.Fatalf("mitigation depended on the cancelled assessment: %s", k.Status)
			}
		case "find the root cause":
			if k.Status.Terminal() {
				t.Fatalf("after_terminal RCA must have been released, not closed: %s", k.Status)
			}
		}
	}
	if _, err := h.svc.Command(h.ctx, CommandRequest{TenantID: "t1", WorkID: parent.ID, ActorID: "bob",
		IdempotencyKey: cpkit.NewID(), Type: work.CmdCancel}); err != nil {
		t.Fatal(err)
	}
	kids, _ = h.store.Children(h.ctx, "t1", parent.ID)
	for _, k := range kids {
		if !k.Status.Terminal() {
			t.Fatalf("child %s survived its parent's cancel: %s", k.Goal.Intent, k.Status)
		}
	}
}

// §6.4: a run terminal event makes the owning Work due at once; a duplicate
// or a foreign run is harmless.
func TestRunWakerWakesOwningWork(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	w := h.create("mitigate", nil)
	h.tick(2)
	links, _ := h.store.ListRunLinks(h.ctx, "t1", w.ID)
	if len(links) != 1 {
		t.Fatalf("links: %d", len(links))
	}
	// Park the Work far in the future, as a long poll interval would.
	if err := h.store.ReleaseWork(h.ctx, "t1", w.ID, "nobody", h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.DB().Exec(`UPDATE agent_works SET wake_at=? WHERE id=?`, h.clock.Now().Add(time.Hour).UnixMicro(), w.ID); err != nil {
		t.Fatal(err)
	}
	waker := &RunWaker{Store: h.store, Clock: h.clock.Now}
	ev := domain.Event{TenantID: "t1", RunID: links[0].RunID, Type: "run.completed"}
	for i := 0; i < 2; i++ { // at-least-once delivery
		if err := waker.Deliver(h.ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := h.store.GetWork(h.ctx, "t1", w.ID)
	if got.WakeAt == nil || got.WakeAt.After(h.clock.Now()) || got.Version != mustVersion(t, h, w.ID) {
		t.Fatalf("the Work must be due now: %v", got.WakeAt)
	}
	if err := waker.Deliver(h.ctx, domain.Event{TenantID: "t1", RunID: cpkit.NewID(), Type: "run.failed"}); err != nil {
		t.Fatalf("a run without a Work is not an error: %v", err)
	}
	if err := waker.Deliver(h.ctx, domain.Event{TenantID: "other", RunID: links[0].RunID, Type: "run.completed"}); err != nil {
		t.Fatal(err)
	}
}

func mustVersion(t *testing.T, h *harness, id string) int64 {
	t.Helper()
	w, err := h.store.GetWork(h.ctx, "t1", id)
	if err != nil {
		t.Fatal(err)
	}
	return w.Version
}
