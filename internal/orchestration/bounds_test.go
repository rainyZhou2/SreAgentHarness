package orchestration

import (
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

func (h *harness) useTemplates(tpls ...work.Template) {
	h.t.Helper()
	cat, err := NewCatalog(tpls)
	if err != nil {
		h.t.Fatal(err)
	}
	h.ing.Catalog, h.svc.Catalog = cat, cat
}

// RFC §20.3 max_cost_usd: once a Work's runs together spent the budget, no
// further run starts.
func TestWorkCostBudget(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.useTemplates(work.Template{ID: "costly", Version: 1, Kind: work.KindIncident, Limits: work.Limits{MaxCostUSD: 0.2},
		Stages: []work.Stage{
			{ID: "first", Type: work.StageRun, Run: &work.RunSpec{Goal: "look"}},
			{ID: "second", Type: work.StageRun, Run: &work.RunSpec{Goal: "look again"}},
		}})
	w := h.create("costly", nil)
	h.settle(w.ID, work.StatusActive)
	h.runs.finishAll(rcaResult, true) // $0.25
	w = h.settle(w.ID, work.StatusFailed, work.StatusSucceeded)
	if w.Status != work.StatusFailed || !strings.HasPrefix(w.Reason, "max_cost_exceeded") {
		t.Fatalf("status %s (%s)", w.Status, w.Reason)
	}
	if h.runs.started() != 1 {
		t.Fatalf("runs started: %d", h.runs.started())
	}
}

// RFC §20.3 max_watches: a step the bound refuses blocks the Work with the
// reason instead of retrying forever.
func TestWatchLimitBlocksWork(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	spec := continuous.Spec{Mode: continuous.OnEither, Timeout: cpkit.Duration(time.Hour),
		Predicate: &eventing.Predicate{EventTypes: []string{"deploy.finished"}}}
	h.useTemplates(work.Template{ID: "waiter", Version: 1, Kind: work.KindIncident, Limits: work.Limits{MaxWatches: 1},
		Stages: []work.Stage{{ID: "wait", Type: work.StageWait, Wait: &spec}, {ID: "confirm", Type: work.StageConfirm}}})
	w := h.create("waiter", nil)
	wt, err := continuous.Build(spec, "t1", w.ID, w.Generation, w.Goal.SubjectRefs, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.ApplyWork(h.ctx, work.Mutation{TenantID: "t1", WorkID: w.ID, ExpectedVersion: w.Version,
		ActorType: "user", ActorID: "op", Now: h.clock.Now(), Watches: []continuous.Watch{wt}}); err != nil {
		t.Fatal(err)
	}
	w = h.settle(w.ID, work.StatusBlocked)
	if w.Status != work.StatusBlocked || !strings.Contains(w.Reason, "max_watches") {
		t.Fatalf("status %s (%s)", w.Status, w.Reason)
	}
}

// RFC §20.3 max_evidence: a spent evidence budget drops the ledger copy of
// a run result; the Work itself keeps going.
func TestEvidenceBudgetDoesNotStopWork(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	h.useTemplates(work.Template{ID: "noted", Version: 1, Kind: work.KindIncident, Limits: work.Limits{MaxEvidence: 1},
		Stages: []work.Stage{{ID: "look", Type: work.StageRun, Run: &work.RunSpec{Goal: "look"}}, {ID: "confirm", Type: work.StageConfirm}}})
	w := h.create("noted", nil)
	if _, _, err := h.store.AddEvidence(h.ctx, evidence.Record{TenantID: "t1", WorkID: w.ID, SourceType: evidence.SourceHuman,
		SourceRef: "api:op", Summary: "operator note", TrustTier: "T3"}); err != nil {
		t.Fatal(err)
	}
	h.settle(w.ID, work.StatusActive)
	h.runs.finishAll(rcaResult, true)
	w = h.settle(w.ID, work.StatusWaiting, work.StatusBlocked, work.StatusFailed)
	if w.Status != work.StatusWaiting {
		t.Fatalf("status %s (%s)", w.Status, w.Reason)
	}
	if list, _ := h.store.ListEvidence(h.ctx, "t1", w.ID, 10); len(list) != 1 {
		t.Fatalf("evidence: %d", len(list))
	}
}
