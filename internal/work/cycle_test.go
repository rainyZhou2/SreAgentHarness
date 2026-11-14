package work

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
)

func TestRepeatingTemplateRequiresWaitAndMachineGoal(t *testing.T) {
	base := Template{ID: "repeat", RepeatFrom: "observe", Stages: []Stage{
		{ID: "observe", Type: StageWait, Wait: &continuous.Spec{Mode: continuous.OnEvent, Predicate: &eventing.Predicate{EventTypes: []string{"anomaly"}}}},
		{ID: "verify", Type: StageVerify},
	}}
	if err := base.Normalize(); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"missing", "verify"} {
		bad := base
		bad.RepeatFrom = target
		if err := bad.Normalize(); err == nil {
			t.Fatalf("accepted repeat target %q", target)
		}
	}
	n := NewWork{TenantID: "t", Kind: KindIncident, Goal: GoalSpec{Intent: "monitor"}, Template: base, IdempotencyKey: "create"}
	if err := n.Prepare(); err == nil {
		t.Fatal("continuous Work accepted unverifiable goal")
	}
	n.Goal.Success = &outcome.Spec{AllOf: []outcome.Objective{{ID: "ack", Kind: outcome.KindHuman}}}
	if err := n.Prepare(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("continuous Work accepted human-only goal: %v", err)
	}
}

func validCycleMutation(t *testing.T) (Work, Mutation, time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	spec := outcome.Spec{AllOf: []outcome.Objective{{ID: "ready", Kind: outcome.KindState,
		State: &outcome.StateExpr{Predicate: "ready", Equals: "yes"}}}}
	if err := spec.Normalize(); err != nil {
		t.Fatal(err)
	}
	tpl := Template{ID: "repeat", RepeatFrom: "observe", Stages: []Stage{
		{ID: "observe", Type: StageWait, Wait: &continuous.Spec{Mode: continuous.OnEvent,
			Purpose: "observe the next anomaly", Predicate: &eventing.Predicate{EventTypes: []string{"anomaly"}}}},
		{ID: "verify", Type: StageVerify},
	}}
	if err := tpl.Normalize(); err != nil {
		t.Fatal(err)
	}
	w := Work{ID: "work", TenantID: "tenant", Version: 7, Generation: 3, Status: StatusVerifying, Stage: 1,
		Goal: GoalSpec{Success: &spec, SubjectRefs: []string{"service://prod/api"}}, Template: tpl}
	wt, err := continuous.Build(*tpl.Stages[0].Wait, w.TenantID, w.ID, w.Generation+1, w.Goal.SubjectRefs, now)
	if err != nil {
		t.Fatal(err)
	}
	state, err := json.Marshal(map[string]any{"stage_id": "observe", "generation": wt.Generation,
		"watch_id": wt.ID, "started_at": wt.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	status, stage, generation := StatusWaiting, 0, w.Generation+1
	m := Mutation{TenantID: w.TenantID, WorkID: w.ID, ExpectedVersion: w.Version, LeaseOwner: "driver",
		Status: &status, Stage: &stage, Generation: &generation, ClearState: true, StageState: state,
		CompleteCycle: true, CancelAllWatches: true, Watches: []continuous.Watch{wt},
		Outcome: &outcome.Evaluation{TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation,
			SpecHash: spec.Hash(), Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: now,
			Objectives: []outcome.ObjectiveResult{{ID: "ready", Kind: outcome.KindState, Verdict: outcome.Satisfied}}}}
	if err := ValidateCycle(w, m, now); err != nil {
		t.Fatalf("valid cycle fixture: %v", err)
	}
	return w, m, now
}

func TestValidateCycleRejectsInvalidNextWait(t *testing.T) {
	changeState := func(field string, value any) func(*testing.T, *Mutation) {
		return func(t *testing.T, m *Mutation) {
			t.Helper()
			var state map[string]any
			if err := json.Unmarshal(m.StageState, &state); err != nil {
				t.Fatal(err)
			}
			state[field] = value
			var err error
			m.StageState, err = json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Mutation)
	}{
		{"already fired", func(_ *testing.T, m *Mutation) { m.Watches[0].Status = continuous.Fired }},
		{"expired watch", func(_ *testing.T, m *Mutation) { m.Watches[0].Status = continuous.Expired }},
		{"cancelled watch", func(_ *testing.T, m *Mutation) { m.Watches[0].Status = continuous.Cancelled }},
		{"changed mode", func(_ *testing.T, m *Mutation) { m.Watches[0].Mode = continuous.OnTime }},
		{"changed predicate", func(_ *testing.T, m *Mutation) {
			m.Watches[0].Predicate = &eventing.Predicate{EventTypes: []string{"unrelated"}}
		}},
		{"changed purpose", func(_ *testing.T, m *Mutation) { m.Watches[0].Purpose = "different purpose" }},
		{"changed subjects", func(_ *testing.T, m *Mutation) { m.Watches[0].Subjects = []string{"service://prod/other"} }},
		{"changed cursor", func(_ *testing.T, m *Mutation) { m.Watches[0].Cursor = "old-event" }},
		{"fired metadata", func(_ *testing.T, m *Mutation) { m.Watches[0].FiredBy = "old-trigger" }},
		{"nonzero watch checks", func(_ *testing.T, m *Mutation) { m.Watches[0].Checks = 1 }},
		{"changed watch version", func(_ *testing.T, m *Mutation) { m.Watches[0].Version++ }},
		{"changed watch update time", func(_ *testing.T, m *Mutation) { m.Watches[0].UpdatedAt = m.Watches[0].UpdatedAt.Add(time.Second) }},
		{"foreign tenant", func(_ *testing.T, m *Mutation) { m.Watches[0].TenantID = "other" }},
		{"foreign work", func(_ *testing.T, m *Mutation) { m.Watches[0].WorkID = "other" }},
		{"wrong watch generation", func(_ *testing.T, m *Mutation) { m.Watches[0].Generation-- }},
		{"wrong mutation generation", func(_ *testing.T, m *Mutation) { *m.Generation++ }},
		{"wrong outcome generation", func(_ *testing.T, m *Mutation) { m.Outcome.Generation++ }},
		{"missing stage state", func(_ *testing.T, m *Mutation) { m.StageState = nil }},
		{"invalid stage state", func(_ *testing.T, m *Mutation) { m.StageState = json.RawMessage(`{`) }},
		{"wrong stage state stage", changeState("stage_id", "verify")},
		{"wrong stage state generation", changeState("generation", 99)},
		{"wrong stage state watch", changeState("watch_id", "old-watch")},
		{"wrong stage state start", changeState("started_at", "2000-01-01T00:00:00Z")},
		{"add runs", func(_ *testing.T, m *Mutation) { m.AddRuns = 1 }},
		{"reset runs", func(_ *testing.T, m *Mutation) { m.AddRuns = -1 }},
		{"add replans", func(_ *testing.T, m *Mutation) { m.AddReplans = 1 }},
		{"reset replans", func(_ *testing.T, m *Mutation) { m.AddReplans = -1 }},
		{"add cost", func(_ *testing.T, m *Mutation) { m.AddCost = 1 }},
		{"reset cost", func(_ *testing.T, m *Mutation) { m.AddCost = -1 }},
		{"expired outcome", func(_ *testing.T, m *Mutation) { m.Outcome.EvaluatedAt = m.Outcome.EvaluatedAt.Add(-2 * time.Minute) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, m, now := validCycleMutation(t)
			tc.mutate(t, &m)
			if err := ValidateCycle(w, m, now); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid cycle accepted or returned unrelated error: %v", err)
			}
		})
	}
}
