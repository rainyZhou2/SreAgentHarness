package cpsql

// Contract tests: every test runs against SQLite, and also against
// PostgreSQL when HARNESS_PG_TEST_URL is set. Tests use a fresh tenant id so
// they can share one PostgreSQL database.

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
	"go-sre-harness/migrations"
)

var t0 = time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)

func forEachStore(t *testing.T, fn func(t *testing.T, s *Store)) {
	t.Run("sqlite", func(t *testing.T) {
		s, err := OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		fn(t, s)
	})
	t.Run("postgres", func(t *testing.T) {
		url := os.Getenv("HARNESS_PG_TEST_URL")
		if url == "" {
			t.Skip("HARNESS_PG_TEST_URL not set")
		}
		ctx := context.Background()
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if err := migrations.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}
		s := OpenPostgres(pool)
		defer s.Close()
		fn(t, s)
	})
}

func tenant() string { return "t-" + cpkit.NewID()[24:] }

func newWork(tn, key string) work.NewWork {
	var tpl work.Template
	for _, x := range work.Builtin() {
		if x.ID == "incident" {
			tpl = x
		}
	}
	return work.NewWork{TenantID: tn, Kind: work.KindIncident, Title: "checkout 5xx",
		Goal:     work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{"service://prod/checkout"}},
		Template: tpl, IdempotencyKey: key, Now: t0,
		Subjects: []work.SubjectLink{{Kind: "subject", ID: "service://prod/checkout"}}}
}

func mustCreate(t *testing.T, s *Store, tn string) work.Work {
	t.Helper()
	w, created, err := s.CreateWork(context.Background(), newWork(tn, cpkit.NewID()))
	if err != nil || !created {
		t.Fatalf("create: %v created=%v", err, created)
	}
	return w
}

func st(v work.Status) *work.Status { return &v }

// T01: concurrent creates with one idempotency key converge on one Work; a
// different body under the same key is a conflict.
func TestIdempotentCreate(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		var wg sync.WaitGroup
		ids := make([]string, 8)
		errs := make([]error, 8)
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				w, _, err := s.CreateWork(ctx, newWork(tn, "k1"))
				ids[i], errs[i] = w.ID, err
			}(i)
		}
		wg.Wait()
		for i := range ids {
			if errs[i] != nil || ids[i] != ids[0] {
				t.Fatalf("create %d: id=%s err=%v (want %s)", i, ids[i], errs[i], ids[0])
			}
		}
		n := newWork(tn, "k1")
		n.Title = "other"
		if _, _, err := s.CreateWork(ctx, n); !errors.Is(err, work.ErrIdempotencyConflict) {
			t.Fatalf("want idempotency conflict, got %v", err)
		}
		evs, err := s.ListWorkEvents(ctx, tn, ids[0], 0, 100)
		if err != nil || len(evs) == 0 || evs[0].Seq != 1 {
			t.Fatalf("events: %v %+v", err, evs)
		}
	})
}

// T02: version CAS, gap-free sequence and transition validation.
func TestApplyWorkCAS(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		w2, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
			Status: st(work.StatusActive)})
		if err != nil || w2.Status != work.StatusActive || w2.Version != w.Version+1 {
			t.Fatalf("apply: %v %+v", err, w2)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
			Status: st(work.StatusWaiting)}); !errors.Is(err, work.ErrVersionConflict) {
			t.Fatalf("stale version: %v", err)
		}
		// SUCCEEDED is only reachable from VERIFYING with a satisfied outcome.
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w2.Version, Now: t0,
			Status: st(work.StatusSucceeded)}); err == nil {
			t.Fatal("ACTIVE -> SUCCEEDED must be rejected")
		}
		w3, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w2.Version, Now: t0,
			Status: st(work.StatusVerifying)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w3.Version, Now: t0,
			Status: st(work.StatusSucceeded)}); err == nil {
			t.Fatal("SUCCEEDED without an outcome must be rejected")
		}
		ev := &outcome.Evaluation{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, SpecHash: "sha256:x",
			Verdict: outcome.Satisfied, RawVerdict: outcome.Satisfied, EvaluatedAt: t0}
		w4, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w3.Version, Now: t0,
			Status: st(work.StatusSucceeded), Outcome: ev})
		if err != nil || w4.Status != work.StatusSucceeded || w4.ClosedAt == nil {
			t.Fatalf("succeed: %v %+v", err, w4)
		}
		evs, _ := s.ListWorkEvents(ctx, tn, w.ID, 0, 1000)
		for i, e := range evs {
			if e.Seq != int64(i+1) {
				t.Fatalf("sequence gap at %d: %d", i, e.Seq)
			}
		}
		outs, _ := s.ListOutcomes(ctx, tn, w.ID, 10)
		if len(outs) != 1 || outs[0].Verdict != outcome.Satisfied {
			t.Fatalf("outcomes: %+v", outs)
		}
	})
}

// T02b: concurrent mutations at one version: exactly one wins.
func TestApplyWorkConcurrent(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		var wg sync.WaitGroup
		var mu sync.Mutex
		ok := 0
		for i := 0; i < 6; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
					Status: st(work.StatusActive), AddRuns: 1})
				if err == nil {
					mu.Lock()
					ok++
					mu.Unlock()
				} else if !errors.Is(err, work.ErrVersionConflict) {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		got, _ := s.GetWork(ctx, tn, w.ID)
		if ok != 1 || got.RunsStarted != 1 {
			t.Fatalf("winners=%d runs=%d", ok, got.RunsStarted)
		}
	})
}

// T03: a run is owned by exactly one Work (it may also be evidence elsewhere);
// operator commands are idempotent.
func TestRunOwnership(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		runA := cpkit.NewID()
		other := mustCreate(t, s, tn)
		link := func(run string) work.RunLink {
			return work.RunLink{RunID: run, Role: work.RoleOwner, Phase: "investigate", StageID: "investigate",
				Generation: w.Generation, Required: true}
		}
		w1, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
			Status: st(work.StatusActive), Links: []work.RunLink{link(runA)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: other.ID, ExpectedVersion: other.Version, Now: t0,
			Links: []work.RunLink{link(runA)}}); !errors.Is(err, work.ErrOwnerConflict) {
			t.Fatalf("run owned by two works: %v", err)
		}
		l, found, err := s.WorkForRun(ctx, tn, runA)
		if err != nil || !found || l.WorkID != w.ID {
			t.Fatalf("work for run: %v %v %+v", err, found, l)
		}
		// Command idempotency: replay returns the stored result, a different
		// body conflicts.
		cmd := &work.CommandRecord{Key: "c1", Type: work.CmdPause, RequestHash: "h1"}
		w2, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w1.Version, Now: t0,
			Command: cmd, Status: st(work.StatusSuspended)})
		if err != nil {
			t.Fatal(err)
		}
		w3, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w1.Version, Now: t0,
			Command: cmd, Status: st(work.StatusSuspended)})
		if err != nil || w3.Version != w2.Version {
			t.Fatalf("replay: %v %d/%d", err, w3.Version, w2.Version)
		}
		bad := *cmd
		bad.RequestHash = "h2"
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w2.Version, Now: t0,
			Command: &bad, Status: st(work.StatusActive)}); !errors.Is(err, work.ErrIdempotencyConflict) {
			t.Fatalf("command conflict: %v", err)
		}
	})
}

// T04/T05: inbox dedup and correlation into one Work.
func TestInboxAndCorrelation(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		env := eventing.Envelope{ID: "alert-1", TenantID: tn, Source: "alertmanager", Type: "alert.firing",
			SubjectRef: "service://prod/checkout", DedupKey: "fp-1"}
		if err := env.Normalize(t0); err != nil {
			t.Fatal(err)
		}
		r1, created, err := s.InsertEvent(ctx, env, t0)
		if err != nil || !created {
			t.Fatal(err, created)
		}
		r2, created, err := s.InsertEvent(ctx, env, t0)
		if err != nil || created || r2.InboxID != r1.InboxID {
			t.Fatalf("dedup: %v %v %s/%s", err, created, r2.InboxID, r1.InboxID)
		}
		var wg sync.WaitGroup
		ids := make([]string, 6)
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				n := newWork(tn, "corr:"+cpkit.NewID())
				w, _, err := s.Correlate(ctx, CorrelateRequest{TenantID: tn, GroupKey: "fp-1", Rule: "r", Window: time.Hour,
					Now: t0, Create: &n, EventID: r1.InboxID})
				if err != nil {
					t.Error(err)
				}
				ids[i] = w.ID
			}(i)
		}
		wg.Wait()
		for _, id := range ids {
			if id != ids[0] {
				t.Fatalf("correlation split: %v", ids)
			}
		}
		works, _ := s.ListWorks(ctx, work.Filter{TenantID: tn})
		if len(works) != 1 {
			t.Fatalf("want one work, got %d", len(works))
		}
		if err := s.MarkEvent(ctx, tn, r1.InboxID, eventing.StateProcessed, ids[0], "attached", "", 5); err != nil {
			t.Fatal(err)
		}
		got, _ := s.GetEvent(ctx, tn, r1.InboxID)
		if got.State != eventing.StateProcessed || got.WorkID != ids[0] {
			t.Fatalf("mark: %+v", got)
		}
		if n, err := s.CountEvents(ctx, tn, "alert.firing", "service://prod/checkout", t0.Add(-time.Hour), t0.Add(time.Hour)); err != nil || n != 1 {
			t.Fatalf("count: %d %v", n, err)
		}
	})
}

// T06: a timer and an event racing for one watch wake the Work once.
func TestWatchFiresOnce(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		pred := &eventing.Predicate{EventTypes: []string{"deploy.finished"}}
		wt, err := continuous.Build(continuous.Spec{Mode: continuous.OnEither, Predicate: pred, Timeout: cpkit.Duration(time.Minute)},
			tn, w.ID, w.Generation, []string{"service://prod/checkout"}, t0)
		if err != nil {
			t.Fatal(err)
		}
		w1, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
			Status: st(work.StatusWaiting), Watches: []continuous.Watch{wt}})
		if err == nil {
			t.Fatal("NEW -> WAITING is not an edge; expected rejection")
		}
		w1, err = s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w.Version, Now: t0,
			Status: st(work.StatusActive)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: w1.Version, Now: t0,
			Status: st(work.StatusWaiting), Watches: []continuous.Watch{wt}}); err != nil {
			t.Fatal(err)
		}
		active, _ := s.ActiveEventWatches(ctx, tn, 10)
		if len(active) != 1 {
			t.Fatalf("active watches: %d", len(active))
		}
		due, _ := s.DueWatches(ctx, t0.Add(2*time.Minute), 10)
		var wg sync.WaitGroup
		fired := make([]bool, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			fired[0], _ = s.FireWatch(ctx, continuous.Firing{TenantID: tn, WatchID: wt.ID, FiringID: "event:e1",
				Kind: "event", FiredAt: t0.Add(30 * time.Second)})
		}()
		go func() {
			defer wg.Done()
			if len(due) > 0 {
				fired[1], _ = s.FireWatch(ctx, continuous.Firing{TenantID: tn, WatchID: wt.ID,
					FiringID: continuous.TimerFiringID(wt.ID, 0), Kind: string(continuous.DueTimeout), FiredAt: t0.Add(2 * time.Minute)})
			}
		}()
		wg.Wait()
		if fired[0] == fired[1] {
			t.Fatalf("exactly one firing must win: %v", fired)
		}
		evs, _ := s.ListWorkEvents(ctx, tn, w.ID, 0, 100)
		n := 0
		for _, e := range evs {
			if e.Type == "work.watch_fired" {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("watch_fired events: %d", n)
		}
		// Replaying the winning firing is a no-op.
		if again, _ := s.FireWatch(ctx, continuous.Firing{TenantID: tn, WatchID: wt.ID, FiringID: "event:e1", Kind: "event"}); again {
			t.Fatal("replayed firing fired again")
		}
	})
}

// T07: due works are leased to one owner; an expired lease can be claimed;
// a stale owner cannot write.
func TestClaimDueWorks(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		a, err := s.ClaimDueWorks(ctx, "owner-a", t0.Add(time.Second), time.Minute, 100)
		if err != nil {
			t.Fatal(err)
		}
		if !containsWork(a, w.ID) {
			t.Fatalf("owner-a did not claim the new work")
		}
		b, _ := s.ClaimDueWorks(ctx, "owner-b", t0.Add(2*time.Second), time.Minute, 100)
		if containsWork(b, w.ID) {
			t.Fatal("lease not exclusive")
		}
		c, _ := s.ClaimDueWorks(ctx, "owner-b", t0.Add(2*time.Minute), time.Minute, 100)
		if !containsWork(c, w.ID) {
			t.Fatal("expired lease not reclaimed")
		}
		cur, _ := s.GetWork(ctx, tn, w.ID)
		if _, err := s.ApplyWork(ctx, work.Mutation{ActorType: "system", ActorID: "test", TenantID: tn, WorkID: w.ID, ExpectedVersion: cur.Version,
			Now: t0.Add(2 * time.Minute), LeaseOwner: "owner-a", Status: st(work.StatusActive)}); !errors.Is(err, work.ErrLeaseLost) {
			t.Fatalf("stale owner: %v", err)
		}
	})
}

func containsWork(ws []work.Work, id string) bool {
	for _, w := range ws {
		if w.ID == id {
			return true
		}
	}
	return false
}

// T08: bitemporal world facts.
func TestWorldAsOf(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		subj := "deployment://prod/checkout"
		put := func(v string, at time.Time) {
			t.Helper()
			if _, _, err := s.AssertFact(ctx, world.Fact{TenantID: tn, SubjectRef: subj, Predicate: "image", Value: v,
				Source: "k8s", TrustTier: world.TrustFirstParty, ValidFrom: at, ObservedAt: at}); err != nil {
				t.Fatal(err)
			}
		}
		put("v1", t0)
		put("v2", t0.Add(10*time.Minute))
		if _, created, _ := s.AssertFact(ctx, world.Fact{TenantID: tn, SubjectRef: subj, Predicate: "image", Value: "v2",
			Source: "k8s", TrustTier: world.TrustFirstParty, ValidFrom: t0.Add(11 * time.Minute), ObservedAt: t0.Add(11 * time.Minute)}); created {
			t.Fatal("re-asserting the current value must be a no-op")
		}
		if fs, _ := s.FactsAt(ctx, tn, []string{subj}, nil, t0.Add(12*time.Minute), t0.Add(12*time.Minute)); len(fs) != 1 ||
			fs[0].ConfirmedAt == nil || !fs[0].SeenBy(t0.Add(12*time.Minute)).Equal(t0.Add(11*time.Minute)) {
			t.Fatalf("re-observation must refresh confirmed_at: %+v", fs)
		}
		val := func(validAt, knownAt time.Time) string {
			fs, err := s.FactsAt(ctx, tn, []string{subj}, nil, validAt, knownAt)
			if err != nil {
				t.Fatal(err)
			}
			if len(fs) != 1 {
				t.Fatalf("facts at %s/%s: %+v", validAt, knownAt, fs)
			}
			return fs[0].Value
		}
		if v := val(t0.Add(5*time.Minute), t0.Add(time.Hour)); v != "v1" {
			t.Fatalf("as of +5m: %s", v)
		}
		if v := val(t0.Add(20*time.Minute), t0.Add(time.Hour)); v != "v2" {
			t.Fatalf("as of +20m: %s", v)
		}
		// What we believed at +5m about +20m: still v1 (v2 unknown yet).
		if v := val(t0.Add(20*time.Minute), t0.Add(5*time.Minute)); v != "v1" {
			t.Fatalf("as known at +5m: %s", v)
		}
		// Late, out-of-order observation bounded by the later interval.
		put("v1.5", t0.Add(5*time.Minute))
		if v := val(t0.Add(7*time.Minute), t0.Add(time.Hour)); v != "v1.5" {
			t.Fatalf("backfill: %s", v)
		}
		if v := val(t0.Add(20*time.Minute), t0.Add(time.Hour)); v != "v2" {
			t.Fatalf("backfill must not override later: %s", v)
		}

		if _, err := s.UpsertEntity(ctx, world.Entity{TenantID: tn, Ref: subj, Kind: "deployment", Source: "k8s",
			Attributes: map[string]string{"name": "checkout", "env": "prod"}}, t0); err != nil {
			t.Fatal(err)
		}
		es, err := s.FindEntities(ctx, tn, "name", "checkout", 10)
		if err != nil || len(es) != 1 {
			t.Fatalf("find: %v %+v", err, es)
		}
		if _, err := s.FindEntities(ctx, tn, "name') OR 1=1 --", "x", 10); err == nil {
			t.Fatal("attribute key must be validated")
		}
		if _, err := s.GetEntity(ctx, "other-"+tn, subj); !errors.Is(err, world.ErrNotFound) {
			t.Fatalf("tenant isolation: %v", err)
		}

		svc := &world.Service{Store: s, Clock: func() time.Time { return t0.Add(time.Hour) }}
		snap, err := svc.Snapshot(ctx, world.SnapshotRequest{TenantID: tn, Subjects: []string{subj}, Persist: true,
			Required: []string{"image", "replicas"}})
		if err != nil {
			t.Fatal(err)
		}
		if len(snap.Missing) != 1 || snap.Digest == "" {
			t.Fatalf("snapshot: %+v", snap)
		}
		back, err := s.GetSnapshot(ctx, tn, snap.ID)
		if err != nil || back.Digest != snap.Digest {
			t.Fatalf("snapshot roundtrip: %v", err)
		}
	})
}

// T10/T24: action records, plan dedup, CAS, target locks.
func TestActionStore(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		p := action.Plan{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, Generation: 1, ActionName: "deployment.rollback",
			ActionVersion: "1", TargetRef: "deployment://prod/checkout", Args: json.RawMessage(`{"to":"v1"}`),
			ProposedBy: "run-1", CreatedAt: t0, ExpiresAt: t0.Add(time.Hour)}
		p.Seal()
		r := action.Record{Plan: p, State: action.StateProposed, UpdatedAt: t0}
		got, created, err := s.CreateAction(ctx, r)
		if err != nil || !created {
			t.Fatal(err, created)
		}
		r2 := r
		r2.ID = cpkit.NewID()
		dup, created, err := s.CreateAction(ctx, r2)
		if err != nil || created || dup.ID != got.ID {
			t.Fatalf("plan dedup: %v %v", err, created)
		}
		got.State = action.StateReady
		upd, err := s.UpdateAction(ctx, got, got.Version)
		if err != nil || upd.Version != 2 {
			t.Fatal(err)
		}
		if _, err := s.UpdateAction(ctx, got, 1); !errors.Is(err, action.ErrVersionConflict) {
			t.Fatalf("stale update: %v", err)
		}
		other := cpkit.NewID()
		ok, _, err := s.LockTarget(ctx, tn, p.TargetRef, p.ID, t0.Add(time.Minute), t0)
		if err != nil || !ok {
			t.Fatal(err)
		}
		ok, holder, _ := s.LockTarget(ctx, tn, p.TargetRef, other, t0.Add(time.Minute), t0.Add(time.Second))
		if ok || holder != p.ID {
			t.Fatalf("lock must be exclusive: %v %s", ok, holder)
		}
		ok, _, _ = s.LockTarget(ctx, tn, p.TargetRef, other, t0.Add(3*time.Minute), t0.Add(2*time.Minute))
		if !ok {
			t.Fatal("expired lock must be taken over")
		}
		if err := s.UnlockTarget(ctx, tn, p.TargetRef, other); err != nil {
			t.Fatal(err)
		}
		d := t0
		upd.State, upd.DispatchedAt, upd.MayHaveDispatched = action.StateDispatched, &d, true
		if _, err := s.UpdateAction(ctx, upd, upd.Version); err != nil {
			t.Fatal(err)
		}
		stale, err := s.ListStaleDispatched(ctx, t0.Add(time.Minute), 10)
		if err != nil || !containsAction(stale, p.ID) {
			t.Fatalf("stale dispatched: %v", err)
		}
		if _, err := s.GetAction(ctx, "other-"+tn, p.ID); !errors.Is(err, action.ErrNotFound) {
			t.Fatalf("tenant isolation: %v", err)
		}
	})
}

func containsAction(rs []action.Record, id string) bool {
	for _, r := range rs {
		if r.ID == id {
			return true
		}
	}
	return false
}

func TestEvidenceStore(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		e := evidence.Record{TenantID: tn, WorkID: w.ID, SourceType: evidence.SourceTool, SourceRef: "tool://metrics/q1",
			Summary: "5xx rate 12%"}
		a, created, err := s.AddEvidence(ctx, e)
		if err != nil || !created {
			t.Fatal(err)
		}
		b, created, err := s.AddEvidence(ctx, e)
		if err != nil || created || b.ID != a.ID {
			t.Fatalf("evidence dedup: %v %v", err, created)
		}
		list, _ := s.ListEvidence(ctx, tn, w.ID, 10)
		if len(list) != 1 {
			t.Fatalf("list: %d", len(list))
		}
		h := evidence.Hypothesis{ID: cpkit.NewID(), TenantID: tn, WorkID: w.ID, Statement: "bad deploy v2",
			Supporting: []string{a.ID}}
		h1, err := s.UpsertHypothesis(ctx, h, 0)
		if err != nil || h1.Status != evidence.HypSupported {
			t.Fatalf("hyp: %v %+v", err, h1)
		}
		if _, err := s.UpsertHypothesis(ctx, h, 0); !errors.Is(err, evidence.ErrVersionConflict) {
			t.Fatalf("dup insert: %v", err)
		}
		h1.Refuting = []string{"ev-2"}
		h2, err := s.UpsertHypothesis(ctx, h1, h1.Version)
		if err != nil || h2.Status != evidence.HypChallenged {
			t.Fatalf("update: %v %+v", err, h2)
		}
		hs, _ := s.ListHypotheses(ctx, tn, w.ID)
		if len(hs) != 1 || hs[0].Version != 2 {
			t.Fatalf("list hyps: %+v", hs)
		}
	})
}
