package orchestration

import (
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

func TestCopiedOutcomePreservesObservationTime(t *testing.T) {
	h := newHarness(t, domain.RiskR1)
	w := h.create("incident", nil)
	w.Status = work.StatusVerifying
	d := driver{o: h.orch, w: w}
	at := h.clock.Now().Add(-time.Hour)
	e := outcome.Evaluation{ID: "original", Verdict: outcome.Satisfied, EvaluatedAt: at}
	got := d.succeedFrom(&e)
	if got.m.Outcome == nil || !got.m.Outcome.EvaluatedAt.Equal(at) {
		t.Fatalf("observation time rewritten: %+v", got.m.Outcome)
	}
}

func TestFinishReevaluatesMachineGoal(t *testing.T) {
	for _, hash := range []string{"human", "goal"} {
		t.Run(hash, func(t *testing.T) {
			h := newHarness(t, domain.RiskR1)
			w := h.create("incident", &outcome.Spec{AllOf: []outcome.Objective{{ID: "ready", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}})
			specHash := hash
			if hash == "goal" {
				specHash = w.Goal.Success.Hash()
			}
			_, err := h.store.ApplyWork(h.ctx, work.Mutation{TenantID: w.TenantID, WorkID: w.ID, ActorType: "system", Now: h.clock.Now(), Outcome: &outcome.Evaluation{SpecHash: specHash, Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: h.clock.Now(), Generation: w.Generation}})
			if err != nil {
				t.Fatal(err)
			}
			w.Status = work.StatusVerifying
			w.Stage = len(w.Template.Stages)
			d := driver{o: h.orch, w: w}
			got, err := d.finish(h.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got.m.Status != nil && *got.m.Status == work.StatusSucceeded {
				t.Fatal("old outcome bypassed current machine goal")
			}
			if got.m.Outcome == nil || got.m.Outcome.Verdict != outcome.Inconclusive || got.m.Outcome.SpecHash != w.Goal.Success.Hash() {
				t.Fatalf("did not reevaluate machine goal: %+v", got)
			}
		})
	}
}

func TestLastStageCannotBypassMachineGoal(t *testing.T) {
	for _, kind := range []work.StageType{work.StageConfirm, work.StageVerify} {
		t.Run(string(kind), func(t *testing.T) {
			h := newHarness(t, domain.RiskR1)
			stage := work.Stage{ID: "last", Type: kind}
			if kind == work.StageVerify {
				stage.Verify = &work.VerifySpec{Spec: &outcome.Spec{AllOf: []outcome.Objective{{ID: "quiet", Kind: outcome.KindEvent, Event: &outcome.EventExpr{Type: "alert.firing", Absent: true}, Window: 60000000000}}}}
			}
			cat, err := NewCatalog([]work.Template{{ID: "last", Stages: []work.Stage{stage}}})
			if err != nil {
				t.Fatal(err)
			}
			h.svc.Catalog = cat
			w := h.create("last", &outcome.Spec{AllOf: []outcome.Objective{{ID: "ready", Kind: outcome.KindState, State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}})
			if kind == work.StageConfirm {
				h.settle(w.ID, work.StatusWaiting)
				if _, err := h.command(w.ID, work.CmdConfirm, CommandPayload{Verdict: "satisfied"}); err != nil {
					t.Fatal(err)
				}
			}
			w = h.settle(w.ID, work.StatusSucceeded, work.StatusBlocked, work.StatusFailed)
			if w.Status == work.StatusSucceeded || w.Stage != len(w.Template.Stages) {
				t.Fatalf("stage bypassed goal or failed to advance: %+v", w)
			}
			outs, err := h.store.ListOutcomes(h.ctx, w.TenantID, w.ID, 50)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, e := range outs {
				if e.Stage == "final" && e.SpecHash == w.Goal.Success.Hash() && e.Verdict == outcome.Inconclusive {
					found = true
				}
			}
			if !found {
				t.Fatalf("no final goal evaluation: %+v", outs)
			}
		})
	}
}
