package orchestration

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

func TestContinuousWorkReturnsToDurableWait(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	var tpl work.Template
	if err := json.Unmarshal([]byte(`{"id":"continuous","version":1,"repeat_from":"observe","limits":{"max_runs":2},"stages":[
		{"id":"observe","type":"wait","wait":{"mode":"event","predicate":{"event_types":["ttft.anomaly"]}}},
		{"id":"diagnose","type":"run","run":{"goal":"Investigate ${subjects}"}},
		{"id":"verify","type":"verify"}]}`), &tpl); err != nil {
		t.Fatal(err)
	}
	cat, err := NewCatalog([]work.Template{tpl})
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Catalog = cat
	w := h.create("continuous", &outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}})
	w = h.settle(w.ID, work.StatusWaiting)
	for cycle := 1; cycle <= 2; cycle++ {
		for delivery := 0; delivery < 2; delivery++ {
			_, _, err := h.ing.Ingest(h.ctx, eventing.Envelope{ID: fmt.Sprintf("anomaly-%d", cycle), TenantID: "t1", Source: "metrics", Type: "ttft.anomaly", SubjectRef: w.Goal.PrimarySubject(), OccurredAt: h.clock.Now()})
			if err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 10 && h.runs.started() < cycle; i++ {
			h.settle(w.ID, work.StatusActive)
			h.clock.Add(time.Second)
		}
		if h.runs.started() != cycle {
			t.Fatalf("cycle %d runs=%d", cycle, h.runs.started())
		}
		h.runs.finishAll(map[string]any{"summary": "investigated"}, true)
		_, _, err = h.world.Store.AssertFact(h.ctx, world.Fact{TenantID: "t1", SubjectRef: w.Goal.PrimarySubject(), Predicate: "ready", Value: "yes", Source: "probe", TrustTier: world.TrustFirstParty, ValidFrom: h.clock.Now(), ObservedAt: h.clock.Now()})
		if err != nil {
			t.Fatal(err)
		}
		w = h.settle(w.ID, work.StatusWaiting, work.StatusSucceeded, work.StatusFailed)
		if w.Status != work.StatusWaiting || w.Stage != 0 || w.Generation != cycle+1 || w.Replans != 0 || w.RunsStarted != cycle {
			t.Fatalf("cycle did not return to wait: %+v", w)
		}
		outs, err := h.store.ListOutcomes(h.ctx, "t1", w.ID, 50)
		if err != nil {
			t.Fatal(err)
		}
		if len(outs) == 0 || outs[0].Generation != cycle || outs[0].Verdict != outcome.Satisfied {
			t.Fatalf("outcome relabeled: %+v", outs)
		}
		// A replacement reconciler/driver uses the same durable watch and state.
		replacement := *h.orch
		replacement.Owner = fmt.Sprintf("replacement-%d", cycle)
		h.orch = &replacement
		h.clock.Add(2 * time.Minute)
	}
	_, _, err = h.ing.Ingest(h.ctx, eventing.Envelope{ID: "over-budget", TenantID: "t1", Source: "metrics", Type: "ttft.anomaly", SubjectRef: w.Goal.PrimarySubject(), OccurredAt: h.clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	w = h.settle(w.ID, work.StatusFailed)
	if w.Status != work.StatusFailed || h.runs.started() != 2 {
		t.Fatalf("lifetime budget reset: %+v runs=%d", w, h.runs.started())
	}
}

func TestWaitingWorkExpiresWithoutExternalWake(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	tpl := work.Template{ID: "wait-deadline", Limits: work.Limits{MaxDuration: cpkit.Duration(time.Minute)}, Stages: []work.Stage{{ID: "wait", Type: work.StageWait, Wait: &continuous.Spec{Mode: continuous.OnEvent, Predicate: &eventing.Predicate{EventTypes: []string{"never"}}}}}}
	cat, err := NewCatalog([]work.Template{tpl})
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Catalog = cat
	w := h.create(tpl.ID, nil)
	w = h.settle(w.ID, work.StatusWaiting)
	if w.WakeAt == nil || !w.WakeAt.Equal(w.CreatedAt.Add(time.Minute)) {
		t.Fatalf("no durable lifetime deadline: %+v", w)
	}
	h.clock.Add(time.Minute)
	w = h.settle(w.ID, work.StatusFailed)
	if w.Status != work.StatusFailed || w.Reason != "max_duration_exceeded" {
		t.Fatalf("expired Work survived without event: %+v", w)
	}
}

func TestProposalDoesNotCrossGeneration(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	w := h.create("mitigate", nil)
	_, err := h.store.ApplyWork(h.ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ActorType: "system", Now: h.clock.Now(), Events: []work.NewEvent{{Type: "work.stage_completed", Payload: map[string]any{"stage_id": "investigate", "generation": 1, "proposed": []map[string]any{{"name": "deployment.rollback", "target": w.Goal.PrimarySubject()}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	w, err = h.store.GetWork(h.ctx, w.TenantID, w.ID)
	if err != nil {
		t.Fatal(err)
	}
	d := driver{o: h.orch, w: w}
	if _, ok, err := d.proposal(h.ctx, "investigate", []string{"deployment.rollback"}); err != nil || !ok {
		t.Fatalf("current proposal missing: %v %v", ok, err)
	}
	d.w.Generation++
	if _, ok, err := d.proposal(h.ctx, "investigate", []string{"deployment.rollback"}); err != nil || ok {
		t.Fatalf("historical proposal authorized new generation: %v %v", ok, err)
	}
}

func TestInboxRecoveryCannotStartAnotherGeneration(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	var tpl work.Template
	if err := json.Unmarshal([]byte(`{"id":"recovery-repeat","repeat_from":"observe","stages":[{"id":"observe","type":"wait","wait":{"mode":"event","predicate":{"event_types":["ttft.anomaly"]}}},{"id":"diagnose","type":"run","run":{"goal":"Investigate"}},{"id":"verify","type":"verify"}]}`), &tpl); err != nil {
		t.Fatal(err)
	}
	cat, err := NewCatalog([]work.Template{tpl})
	if err != nil {
		t.Fatal(err)
	}
	h.svc.Catalog = cat
	w := h.create(tpl.ID, &outcome.Spec{AllOf: []outcome.Objective{{ID: "healthy", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}})
	w = h.settle(w.ID, work.StatusWaiting)
	rec, _, err := h.store.InsertEvent(h.ctx, eventing.Envelope{ID: "same-inbox", TenantID: "t1", Source: "metrics", Type: "ttft.anomaly", SubjectRef: w.Goal.PrimarySubject(), OccurredAt: h.clock.Now()}, h.clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate persisted state after watch commit but before inbox completion;
	// this does not terminate a process or reopen the database.
	if _, err := h.ing.fireWatches(h.ctx, rec); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10 && h.runs.started() < 1; i++ {
		h.settle(w.ID, work.StatusActive)
		h.clock.Add(time.Second)
	}
	h.runs.finishAll(map[string]any{"summary": "done"}, true)
	_, _, err = h.world.Store.AssertFact(h.ctx, world.Fact{TenantID: "t1", SubjectRef: w.Goal.PrimarySubject(), Predicate: "ready", Value: "yes", Source: "probe", TrustTier: world.TrustFirstParty, ValidFrom: h.clock.Now(), ObservedAt: h.clock.Now()})
	if err != nil {
		t.Fatal(err)
	}
	w = h.settle(w.ID, work.StatusWaiting, work.StatusSucceeded, work.StatusFailed)
	if w.Generation != 2 || w.Status != work.StatusWaiting {
		t.Fatalf("did not complete cycle: %+v", w)
	}
	h.clock.Add(30 * time.Second)
	if recovered, err := h.ing.Sweep(h.ctx, 30*time.Second, 20); err != nil || recovered != 1 {
		t.Fatalf("inbox recovery count=%d err=%v", recovered, err)
	}
	watches, err := h.store.ListWatches(h.ctx, "t1", w.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	currentWatches := 0
	for _, watch := range watches {
		if watch.Generation == 2 {
			currentWatches++
			if watch.Status != continuous.Active {
				t.Fatalf("same inbox replay fired next generation: %+v", watch)
			}
		}
	}
	if currentWatches != 1 {
		t.Fatalf("next cycle watches=%d", currentWatches)
	}
	h.settle(w.ID, work.StatusWaiting)
	if h.runs.started() != 1 {
		t.Fatalf("inbox recovery dispatched extra runs: %d", h.runs.started())
	}
}
