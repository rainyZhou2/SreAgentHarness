package cpsql

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
	"go-sre-harness/migrations"
)

// §17.4: every generation keeps its goal; a replan may amend it; assignment
// results land on the owner links.
func TestGoalHistoryAndAssignments(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		goals, err := s.ListGoals(ctx, tn, w.ID)
		if err != nil || len(goals) != 1 || goals[0].Generation != 1 || goals[0].GoalHash != w.GoalHash {
			t.Fatalf("initial goal: %v %+v", err, goals)
		}
		run := cpkit.NewID()
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0,
			Status: st(work.StatusActive), Links: []work.RunLink{{RunID: run, Role: work.RoleOwner, Phase: "investigate",
				StageID: "investigate", AgentRole: "metrics", Required: true}}})
		if err != nil {
			t.Fatal(err)
		}
		fin := t0.Add(3 * time.Minute)
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: fin,
			LinkStatus:  map[string]string{run: "COMPLETED"},
			LinkResults: map[string]work.LinkResult{run: {Outcome: "completed", CostMicros: 1250, FinishedAt: fin, EvidenceRefs: []string{"evidence:e1"}}}})
		if err != nil {
			t.Fatal(err)
		}
		links, err := s.ListRunLinks(ctx, tn, w.ID)
		if err != nil || len(links) != 1 {
			t.Fatalf("links: %v %d", err, len(links))
		}
		l := links[0]
		if l.Outcome != "completed" || l.CostMicros != 1250 || l.FinishedAt == nil || !l.FinishedAt.Equal(fin) ||
			len(l.EvidenceRefs) != 1 || l.Status != "COMPLETED" {
			t.Fatalf("assignment result: %+v", l)
		}
		var n int
		if err := s.db.QueryRowContext(ctx, s.q(`SELECT count(*) FROM work_assignments WHERE tenant_id=? AND work_id=? AND role='metrics'`),
			tn, w.ID).Scan(&n); err != nil || n != 1 {
			t.Fatalf("assignments view: %v %d", err, n)
		}

		amended := w.Goal
		amended.Description = "also check the payments dependency"
		amended.SubjectRefs = append(append([]string(nil), amended.SubjectRefs...), "service://prod/payments")
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "user", Now: fin,
			Goal: &amended}); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("goal without a new generation: %v", err)
		}
		gen := w.Generation + 1
		reason := "scope widened"
		w2, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "user", ActorID: "alice", Now: fin,
			Generation: &gen, Goal: &amended, Reason: &reason})
		if err != nil {
			t.Fatal(err)
		}
		if w2.GoalHash == w.GoalHash || w2.Goal.Description != amended.Description {
			t.Fatalf("goal not amended: %+v", w2.Goal)
		}
		got, err := s.GetWork(ctx, tn, w.ID)
		if err != nil || got.GoalHash != w2.GoalHash {
			t.Fatalf("stored goal: %v %s/%s", err, got.GoalHash, w2.GoalHash)
		}
		goals, err = s.ListGoals(ctx, tn, w.ID)
		if err != nil || len(goals) != 2 || goals[0].GoalHash != w.GoalHash || goals[1].GoalHash != w2.GoalHash ||
			goals[1].ActorID != "alice" || goals[1].Reason != reason {
			t.Fatalf("goal history: %v %+v", err, goals)
		}
		subs, _ := s.ListSubjects(ctx, tn, w.ID)
		found := false
		for _, sl := range subs {
			found = found || sl.ID == "service://prod/payments"
		}
		if !found {
			t.Fatalf("amended subject not linked: %+v", subs)
		}
		evs, _ := s.ListWorkEvents(ctx, tn, w.ID, 0, 100)
		amendedEv := false
		for _, e := range evs {
			amendedEv = amendedEv || e.Type == "work.goal_amended"
		}
		if !amendedEv {
			t.Fatal("no work.goal_amended event")
		}
		// A new generation without a goal change still records its goal.
		gen3 := gen + 1
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: fin, Generation: &gen3}); err != nil {
			t.Fatal(err)
		}
		goals, _ = s.ListGoals(ctx, tn, w.ID)
		if len(goals) != 3 || goals[2].GoalHash != w2.GoalHash {
			t.Fatalf("generation 3 goal: %+v", goals)
		}
	})
}

// §17.4: terminal inbox events lose their payload after the payload
// retention and the row after the record retention; NEW/FAILED never.
func TestInboxRetention(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		insert := func(id string) eventing.Record {
			env := eventing.Envelope{ID: id, TenantID: tn, Source: "alertmanager", Type: "alert.firing",
				SubjectRef: "service://prod/checkout", Payload: json.RawMessage(`{"secret":"x"}`)}
			if err := env.Normalize(t0); err != nil {
				t.Fatal(err)
			}
			r, _, err := s.InsertEvent(ctx, env, t0)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		done, pending := insert("e-done"), insert("e-pending")
		if err := s.MarkEvent(ctx, tn, done.InboxID, eventing.StateProcessed, "", "ignored", "", 5); err != nil {
			t.Fatal(err)
		}
		ret := InboxRetention{Payload: 14 * 24 * time.Hour, Record: 90 * 24 * time.Hour}
		if _, err := s.PurgeEvents(ctx, t0, InboxRetention{Payload: 2 * time.Hour, Record: time.Hour}); err == nil {
			t.Fatal("payload retention longer than record retention must be refused")
		}
		// The PostgreSQL database is shared between tests: other tenants'
		// events may be purged too, so per-tenant state is checked below.
		if _, err := s.PurgeEvents(ctx, t0.Add(10*24*time.Hour), ret); err != nil {
			t.Fatal(err)
		}
		if got, _ := s.GetEvent(ctx, tn, done.InboxID); len(got.Envelope.Payload) == 0 {
			t.Fatal("payload stripped before the retention")
		}
		res, err := s.PurgeEvents(ctx, t0.Add(20*24*time.Hour), ret)
		if err != nil || res.PayloadsStripped < 1 {
			t.Fatalf("payload pass: %v %+v", err, res)
		}
		got, err := s.GetEvent(ctx, tn, done.InboxID)
		if err != nil || len(got.Envelope.Payload) != 0 || got.Envelope.SubjectRef != "service://prod/checkout" || got.Decision != "ignored" {
			t.Fatalf("stripped event: %v %+v", err, got)
		}
		if p, _ := s.GetEvent(ctx, tn, pending.InboxID); len(p.Envelope.Payload) == 0 {
			t.Fatal("a NEW event lost its payload")
		}
		res, err = s.PurgeEvents(ctx, t0.Add(100*24*time.Hour), ret)
		if err != nil || res.Deleted < 1 {
			t.Fatalf("record pass: %v %+v", err, res)
		}
		if _, err := s.GetEvent(ctx, tn, done.InboxID); err == nil {
			t.Fatal("expired event still present")
		}
		if _, err := s.GetEvent(ctx, tn, pending.InboxID); err != nil {
			t.Fatalf("a NEW event was purged: %v", err)
		}
	})
}

// pgFixture opens the PostgreSQL test database with a raw connection that
// skips foreign keys and triggers (session_replication_role=replica) to seed
// kernel rows without a full run.
func pgFixture(t *testing.T) (*Store, func(sql string, args ...any)) {
	t.Helper()
	url := os.Getenv("HARNESS_PG_TEST_URL")
	if url == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Release)
	if _, err := conn.Exec(ctx, `SET session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `SET session_replication_role = DEFAULT`) })
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	s := OpenPostgres(pool)
	t.Cleanup(func() { s.Close() })
	return s, exec
}

func seedRun(exec func(string, ...any), tenantID string) string {
	id := cpkit.NewID()
	exec(`INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status, create_idempotency_key, goal_ref,
		initiator_type) VALUES ($1,$2,'p',1,'RUNNING',$3,'goal','system')`, id, tenantID, "k-"+id)
	return id
}

// The two ledgers: an open kernel effect on the action's target is visible
// and blocks nothing else.
func TestKernelEffectsPG(t *testing.T) {
	s, exec := pgFixture(t)
	ctx, tn := context.Background(), tenant()
	run := seedRun(exec, tn)
	for i, st := range []string{"UNKNOWN", "CONFIRMED"} {
		exec(`INSERT INTO external_tool_effects (id, tenant_id, run_id, step_id, idempotency_key, target, state, last_check_at)
			VALUES ($1,$2,$3,$4,$5,'prod/checkout',$6, now())`, cpkit.NewID(), tn, run, cpkit.NewID(), "ik-"+cpkit.NewID(), st)
		_ = i
	}
	exec(`INSERT INTO external_tool_effects (id, tenant_id, run_id, step_id, idempotency_key, target, state)
		VALUES ($1,$2,$3,$4,$5,'prod/search','DISPATCHED')`, cpkit.NewID(), tn, run, cpkit.NewID(), "ik-"+cpkit.NewID())
	targets := action.EffectTargets("deployment://prod/checkout")
	open, err := s.KernelEffects(ctx, tn, targets, true, 10)
	if err != nil || len(open) != 1 || open[0].State != "UNKNOWN" || open[0].RunID != run || !open[0].Open() {
		t.Fatalf("open effects: %v %+v", err, open)
	}
	all, err := s.KernelEffects(ctx, tn, targets, false, 10)
	if err != nil || len(all) != 2 {
		t.Fatalf("all effects: %v %+v", err, all)
	}
	if other, _ := s.KernelEffects(ctx, tenant(), targets, true, 10); len(other) != 0 {
		t.Fatalf("effects leaked across tenants: %+v", other)
	}
}

// A Work cannot reference a run of another tenant (trigger, PostgreSQL).
func TestRunTenantConsistencyPG(t *testing.T) {
	s, exec := pgFixture(t)
	ctx, tn := context.Background(), tenant()
	foreign := seedRun(exec, tenant())
	w := mustCreate(t, s, tn)
	_, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0,
		Links: []work.RunLink{{RunID: foreign, Role: work.RoleEvidence, Phase: "investigate"}}})
	if err == nil || !strings.Contains(err.Error(), "another tenant") {
		t.Fatalf("cross-tenant run link accepted: %v", err)
	}
	own := seedRun(exec, tn)
	if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0,
		Links: []work.RunLink{{RunID: own, Role: work.RoleEvidence, Phase: "investigate"}}}); err != nil {
		t.Fatalf("own run link: %v", err)
	}
}

// §21.3: the backlog view counts what waits and checks the approval
// invariant.
func TestBacklog(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		env := eventing.Envelope{ID: "bl-1", TenantID: tn, Source: "alertmanager", Type: "alert.firing",
			SubjectRef: "service://prod/checkout"}
		if err := env.Normalize(t0); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.InsertEvent(ctx, env, t0); err != nil {
			t.Fatal(err)
		}
		w := mustCreate(t, s, tn)
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0,
			Status: st(work.StatusActive)}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, ActorType: "system", Now: t0,
			Status: st(work.StatusWaiting)}); err != nil {
			t.Fatal(err)
		}
		b, err := s.Backlog(ctx, t0.Add(10*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if b.Inbox["NEW"].Count < 1 || b.Inbox["NEW"].Oldest < 10*time.Minute {
			t.Fatalf("inbox backlog: %+v", b.Inbox)
		}
		if b.Works["WAITING"].Count < 1 {
			t.Fatalf("waiting works: %+v", b.Works)
		}
		// Approval invariant: an R2 action dispatched without an approval
		// is counted; one with an approval or a break-glass grant is not.
		mk := func(approve, breakGlass bool) {
			p := action.Plan{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, Generation: 1, ActionName: "deployment.rollback",
				ActionVersion: "1", TargetRef: "deployment://prod/" + cpkit.NewID()[24:], Args: json.RawMessage(`{"to":"v1"}`),
				Risk: "R2", Approvals: 1, ProposedBy: "run-1", CreatedAt: t0, ExpiresAt: t0.Add(time.Hour)}
			p.Seal()
			d := t0.Add(time.Minute)
			r := action.Record{Plan: p, State: action.StateDispatched, DispatchedAt: &d, UpdatedAt: d}
			if approve {
				r.ApprovalLog = []action.Approval{{ApproverID: "bob", Decision: "approve", IntentHash: p.IntentHash, At: d}}
			}
			if breakGlass {
				r.BreakGlass = &action.BreakGlassGrant{Actor: "oncall", Justification: "sev1 outage", IntentHash: p.IntentHash, At: d}
			}
			if _, _, err := s.CreateAction(ctx, r); err != nil {
				t.Fatal(err)
			}
		}
		before, _ := s.Backlog(ctx, t0.Add(10*time.Minute))
		mk(true, false)
		mk(false, true)
		after, _ := s.Backlog(ctx, t0.Add(10*time.Minute))
		if after.UnapprovedDispatches != before.UnapprovedDispatches {
			t.Fatalf("approved or break-glass dispatches counted: %d -> %d", before.UnapprovedDispatches, after.UnapprovedDispatches)
		}
		mk(false, false)
		after, _ = s.Backlog(ctx, t0.Add(10*time.Minute))
		if after.UnapprovedDispatches != before.UnapprovedDispatches+1 {
			t.Fatalf("unapproved dispatch not counted: %d -> %d", before.UnapprovedDispatches, after.UnapprovedDispatches)
		}
		if after.Actions["DISPATCHED"].Count < 3 {
			t.Fatalf("action backlog: %+v", after.Actions)
		}
	})
}
