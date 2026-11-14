package orchestration

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

type handoffRuns struct {
	*fakeRuns
	h             *harness
	activated     int
	loseResponse  bool
	afterReserve  func()
	reserveCalls  int
	failReserveAt int
}

func (r *handoffRuns) ReserveRun(ctx context.Context, req RunRequest) (string, error) {
	r.reserveCalls++
	if r.failReserveAt == r.reserveCalls {
		return "", errors.New("reservation response unavailable")
	}
	id, err := r.fakeRuns.ReserveRun(ctx, req)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	r.runs[id].state = RunState{Status: "PENDING_ACTIVATION"}
	r.mu.Unlock()
	if r.afterReserve != nil {
		r.afterReserve()
	}
	return id, nil
}

func (r *handoffRuns) ActivateRun(ctx context.Context, req work.RunActivation) error {
	if err := r.h.store.CheckRunActivation(ctx, req); err != nil {
		return err
	}
	tenant, id := req.TenantID, req.RunID
	link, ok, err := r.h.store.WorkForRun(ctx, tenant, id)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("activation before durable owner link")
	}
	w, err := r.h.store.GetWork(ctx, tenant, link.WorkID)
	if err != nil {
		return err
	}
	if w.Generation != link.Generation || w.Status != work.StatusActive {
		return errors.New("activation with stale work authority")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.runs[id].state.Status == "PENDING_ACTIVATION" {
		r.runs[id].state = RunState{Status: "RUNNING"}
		r.activated++
	}
	if r.loseResponse {
		r.loseResponse = false
		return errors.New("activation response lost")
	}
	return nil
}

func TestRunHandoffActivatesOnlyAfterOwnerCommit(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	r := &handoffRuns{fakeRuns: h.runs, h: h, loseResponse: true}
	h.orch.Runs = r
	w := h.create("mitigate", nil)
	if _, err := h.orch.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if r.activated != 1 {
		t.Fatalf("durably linked reservation not activated: %d", r.activated)
	}
	h.clock.Add(20 * time.Second)
	if _, err := h.orch.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if r.activated != 1 || r.started() != 1 {
		t.Fatalf("retry duplicated handoff: activations=%d runs=%d", r.activated, r.started())
	}
	links, err := h.store.ListRunLinks(h.ctx, "t1", w.ID)
	if err != nil || len(links) != 1 {
		t.Fatalf("links=%v err=%v", links, err)
	}
}

func TestRunHandoffLosingWorkCASCannotActivate(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	r := &handoffRuns{fakeRuns: h.runs, h: h}
	h.orch.Runs = r
	w := h.create("mitigate", nil)
	r.afterReserve = func() {
		r.afterReserve = nil
		if _, err := h.command(w.ID, work.CmdCancel, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.orch.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if r.activated != 0 {
		t.Fatal("cancelled owner activated a reserved run")
	}
	for _, id := range r.order {
		st, _ := r.RunState(h.ctx, "t1", id)
		if st.Status != "PENDING_ACTIVATION" {
			t.Fatalf("unlinked run executable: %+v", st)
		}
	}
}

func TestRunHandoffResumesPartialFanOut(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	tpl := work.Template{ID: "fanout", Version: 1, Kind: work.KindIncident, Stages: []work.Stage{{ID: "diagnose", Type: work.StageAgents, Agents: []work.AgentSpec{
		{Role: "metrics", Required: true, RunSpec: work.RunSpec{Goal: "inspect metrics"}},
		{Role: "logs", Required: true, RunSpec: work.RunSpec{Goal: "inspect logs"}},
	}}}}
	cat, err := NewCatalog([]work.Template{tpl})
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Catalog = cat
	r := &handoffRuns{fakeRuns: h.runs, h: h, failReserveAt: 2}
	h.orch.Runs = r
	w := h.create("fanout", nil)
	if _, err := h.orch.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if r.started() != 1 || r.activated != 0 {
		t.Fatalf("partial fanout executed: runs=%d active=%d", r.started(), r.activated)
	}
	links, err := h.store.ListRunLinks(h.ctx, w.TenantID, w.ID)
	if err != nil || len(links) != 0 {
		t.Fatalf("partial owner commit: %v %v", links, err)
	}
	first := r.order[0]
	h.clock.Add(20 * time.Second)
	if _, err := h.orch.Tick(h.ctx); err != nil {
		t.Fatal(err)
	}
	if r.started() != 2 || r.activated != 2 || r.order[0] != first {
		t.Fatalf("partial fanout not adopted: runs=%d active=%d", r.started(), r.activated)
	}
	links, err = h.store.ListRunLinks(h.ctx, w.TenantID, w.ID)
	if err != nil || len(links) != 2 {
		t.Fatalf("owner links: %v %v", links, err)
	}
}
