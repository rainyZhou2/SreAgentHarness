package orchestration

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/work"
)

func newCauseBackfillTarget(t *testing.T, f *secondaryCreateFixture) work.Work {
	t.Helper()
	req := f.request()
	req.CausedBy = nil
	target, created, err := f.h.svc.Create(f.h.ctx, req)
	if err != nil || !created {
		t.Fatalf("create target: created=%v err=%v", created, err)
	}
	return target
}

func causeBackfillRequest(f *secondaryCreateFixture, target work.Work) LinkCauseRequest {
	return LinkCauseRequest{TenantID: target.TenantID, WorkID: target.ID, ActorID: "alice", IdempotencyKey: "backfill-1",
		ExpectedVersion: target.Version, IncidentID: f.source.ID, ActionID: f.action.ID, Reason: "postmortem confirmed"}
}

func incidentCauseEvents(t *testing.T, f *secondaryCreateFixture, workID string) []work.Event {
	t.Helper()
	events, err := f.h.store.ListWorkEvents(f.h.ctx, "t1", workID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []work.Event
	for _, event := range events {
		if event.Type == "work.incident_cause_linked" {
			out = append(out, event)
		}
	}
	return out
}

func TestIncidentCauseBackfillPersistsLinksAndAuditEvent(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	got, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, causeBackfillRequest(f, target))
	if err != nil || !changed {
		t.Fatalf("link cause: changed=%v err=%v", changed, err)
	}
	if got.Version != target.Version+1 || got.Status != target.Status || got.Generation != target.Generation ||
		got.Stage != target.Stage || !reflect.DeepEqual(got.Goal, target.Goal) ||
		!reflect.DeepEqual(got.WakeAt, target.WakeAt) || !reflect.DeepEqual(got.ClosedAt, target.ClosedAt) {
		t.Fatalf("unexpected Work changes: before=%+v after=%+v", target, got)
	}
	subjects, err := f.h.store.ListSubjects(f.h.ctx, target.TenantID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	links, err := work.CausalLinks(subjects)
	if err != nil || len(links) != 2 || links[0].ID != f.source.ID || links[1].ID != f.action.ID {
		t.Fatalf("causal links=%+v err=%v", links, err)
	}
	events := incidentCauseEvents(t, f, target.ID)
	if len(events) != 1 || events[0].ActorType != "user" || events[0].ActorID != "alice" {
		t.Fatalf("cause events=%+v", events)
	}
	var payload map[string]any
	if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"incident_id": f.source.ID, "action_id": f.action.ID,
		"mitigation_work_id": f.mitigation.ID, "reason": "postmortem confirmed", "idempotency_key": "backfill-1"}
	if !reflect.DeepEqual(payload, want) {
		t.Fatalf("payload=%+v want=%+v", payload, want)
	}
}

func TestIncidentCauseBackfillNoOpConflictAndReplay(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	req := causeBackfillRequest(f, target)
	first, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, req)
	if err != nil || !changed {
		t.Fatalf("first link: changed=%v err=%v", changed, err)
	}
	eventsBefore := len(incidentCauseEvents(t, f, target.ID))

	noOp := req
	noOp.IdempotencyKey = "backfill-noop"
	noOp.ExpectedVersion = first.Version
	unchanged, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, noOp)
	if err != nil || changed || unchanged.Version != first.Version || len(incidentCauseEvents(t, f, target.ID)) != eventsBefore {
		t.Fatalf("no-op: work=%+v changed=%v err=%v", unchanged, changed, err)
	}

	secondAction := createSecondaryAction(t, f.h, f.mitigation, true)
	conflict := noOp
	conflict.IdempotencyKey = "backfill-conflict"
	conflict.ActionID = secondAction.ID
	if _, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, conflict); !errors.Is(err, work.ErrCauseConflict) {
		t.Fatalf("different cause error=%v", err)
	}

	if _, err := f.h.command(target.ID, work.CmdWake, nil); err != nil {
		t.Fatal(err)
	}
	replay, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, req)
	if err != nil || changed || replay.Version != first.Version || replay.LastSeq != first.LastSeq {
		t.Fatalf("replay=%+v changed=%v err=%v first=%+v", replay, changed, err, first)
	}
	for _, mutate := range []func(*LinkCauseRequest){
		func(r *LinkCauseRequest) { r.Reason = "different reason" },
		func(r *LinkCauseRequest) { r.ActionID = secondAction.ID },
		func(r *LinkCauseRequest) { r.IncidentID = cpkit.NewID() },
		func(r *LinkCauseRequest) { r.ExpectedVersion++ },
	} {
		changedReq := req
		mutate(&changedReq)
		if _, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, changedReq); !errors.Is(err, work.ErrIdempotencyConflict) {
			t.Fatalf("changed replay=%+v error=%v", changedReq, err)
		}
	}
}

func TestIncidentCauseBackfillChecksVersionBeforeCauseConflict(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	req := causeBackfillRequest(f, target)
	first, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, req)
	if err != nil || !changed {
		t.Fatalf("first link: changed=%v err=%v", changed, err)
	}
	stale := req
	stale.IdempotencyKey = "backfill-stale"
	stale.ActionID = createSecondaryAction(t, f.h, f.mitigation, true).ID
	if _, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, stale); !errors.Is(err, work.ErrVersionConflict) {
		t.Fatalf("stale error=%v", err)
	}
	subjects, err := f.h.store.ListSubjects(f.h.ctx, target.TenantID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	links, err := work.CausalLinks(subjects)
	if err != nil || len(links) != 2 || links[1].ID != f.action.ID || len(incidentCauseEvents(t, f, target.ID)) != 1 || first.Version != target.Version+1 {
		t.Fatalf("stale request changed state: links=%+v err=%v", links, err)
	}
}

func TestIncidentCauseBackfillRejectsInvalidRequests(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*LinkCauseRequest)
	}{
		{name: "tenant", mutate: func(r *LinkCauseRequest) { r.TenantID = "" }},
		{name: "work", mutate: func(r *LinkCauseRequest) { r.WorkID = "" }},
		{name: "actor", mutate: func(r *LinkCauseRequest) { r.ActorID = "" }},
		{name: "idempotency key", mutate: func(r *LinkCauseRequest) { r.IdempotencyKey = "" }},
		{name: "version", mutate: func(r *LinkCauseRequest) { r.ExpectedVersion = 0 }},
		{name: "source", mutate: func(r *LinkCauseRequest) { r.IncidentID = " " }},
		{name: "action", mutate: func(r *LinkCauseRequest) { r.ActionID = "" }},
		{name: "reason", mutate: func(r *LinkCauseRequest) { r.Reason = " \n" }},
		{name: "reason length", mutate: func(r *LinkCauseRequest) { r.Reason = strings.Repeat("x", 501) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSecondaryCreateFixture(t)
			target := newCauseBackfillTarget(t, f)
			req := causeBackfillRequest(f, target)
			tc.mutate(&req)
			if _, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, req); !errors.Is(err, work.ErrInvalid) {
				t.Fatalf("error=%v", err)
			}
			if len(incidentCauseEvents(t, f, target.ID)) != 0 {
				t.Fatal("invalid request wrote an audit event")
			}
		})
	}
}

func TestIncidentCauseBackfillRejectsInvalidEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *secondaryCreateFixture, *work.Work, *LinkCauseRequest)
		want   error
	}{
		{name: "target not incident", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, target *work.Work, req *LinkCauseRequest) {
			routine, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindRoutine, Goal: work.GoalSpec{Intent: "routine", SubjectRefs: []string{f.action.TargetRef}}})
			if err != nil {
				t.Fatal(err)
			}
			*target = routine
			req.WorkID, req.ExpectedVersion = routine.ID, routine.Version
		}},
		{name: "self cause", want: work.ErrInvalid, mutate: func(_ *testing.T, _ *secondaryCreateFixture, target *work.Work, req *LinkCauseRequest) {
			req.IncidentID = target.ID
		}},
		{name: "source not incident", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			routine, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindRoutine, Goal: work.GoalSpec{Intent: "routine"}})
			if err != nil {
				t.Fatal(err)
			}
			req.IncidentID = routine.ID
		}},
		{name: "not dispatched", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			req.ActionID = createSecondaryAction(t, f.h, f.mitigation, false).ID
		}},
		{name: "action outside source", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			other := f.h.create("incident", nil)
			req.ActionID = createSecondaryAction(t, f.h, other, true).ID
		}},
		{name: "not mitigation child", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			rca := createSecondaryChild(t, f.h, f.source, "rca")
			req.ActionID = createSecondaryAction(t, f.h, rca, true).ID
		}},
		{name: "indirect child", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			nested := createSecondaryChild(t, f.h, f.mitigation, "mitigation")
			req.ActionID = createSecondaryAction(t, f.h, nested, true).ID
		}},
		{name: "target mismatch", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, target *work.Work, req *LinkCauseRequest) {
			other, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindIncident, Goal: work.GoalSpec{Intent: "other", SubjectRefs: []string{"deployment://prod/other"}}})
			if err != nil {
				t.Fatal(err)
			}
			*target = other
			req.WorkID, req.ExpectedVersion = other.ID, other.Version
		}},
		{name: "cross tenant source", want: work.ErrNotFound, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			other, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t2", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindIncident, Goal: work.GoalSpec{Intent: "other tenant"}})
			if err != nil {
				t.Fatal(err)
			}
			req.IncidentID = other.ID
		}},
		{name: "cross tenant action", want: action.ErrNotFound, mutate: func(t *testing.T, f *secondaryCreateFixture, _ *work.Work, req *LinkCauseRequest) {
			other, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t2", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindIncident, Goal: work.GoalSpec{Intent: "other tenant", SubjectRefs: []string{"deployment://prod/checkout"}}})
			if err != nil {
				t.Fatal(err)
			}
			req.ActionID = createSecondaryAction(t, f.h, other, true).ID
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSecondaryCreateFixture(t)
			target := newCauseBackfillTarget(t, f)
			req := causeBackfillRequest(f, target)
			tc.mutate(t, f, &target, &req)
			if _, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, req); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			subjects, err := f.h.store.ListSubjects(f.h.ctx, target.TenantID, target.ID)
			if err != nil {
				t.Fatal(err)
			}
			links, err := work.CausalLinks(subjects)
			if err != nil || len(links) != 0 || len(incidentCauseEvents(t, f, target.ID)) != 0 {
				t.Fatalf("rejected request wrote cause: links=%+v err=%v", links, err)
			}
		})
	}
}

func TestIncidentCauseBackfillAllowsTerminalIncidentWithoutLifecycleChanges(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	closed, err := f.h.command(target.ID, work.CmdCancel, nil)
	if err != nil || closed.ClosedAt == nil {
		t.Fatalf("close target: work=%+v err=%v", closed, err)
	}
	req := causeBackfillRequest(f, closed)
	got, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, req)
	if err != nil || !changed {
		t.Fatalf("link closed Incident: changed=%v err=%v", changed, err)
	}
	if got.Status != closed.Status || got.Generation != closed.Generation || got.Stage != closed.Stage ||
		!reflect.DeepEqual(got.Goal, closed.Goal) || !reflect.DeepEqual(got.WakeAt, closed.WakeAt) ||
		!reflect.DeepEqual(got.ClosedAt, closed.ClosedAt) {
		t.Fatalf("terminal lifecycle changed: before=%+v after=%+v", closed, got)
	}
}

func TestIncidentCauseBackfillRejectsCorruptStoredLinks(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	if _, err := f.h.store.DB().ExecContext(f.h.ctx, `INSERT INTO work_subject_links
		(tenant_id, work_id, subject_kind, subject_id, created_at) VALUES (?, ?, ?, ?, ?)`,
		target.TenantID, target.ID, work.SubjectCausedByWork, f.source.ID, f.h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	_, _, err := f.h.svc.LinkIncidentCause(f.h.ctx, causeBackfillRequest(f, target))
	if err == nil || errors.Is(err, work.ErrInvalid) || errors.Is(err, work.ErrCauseConflict) {
		t.Fatalf("corrupt stored links error=%v", err)
	}
}

func TestIncidentCauseBackfillConcurrentCASAllowsOneWriter(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	target := newCauseBackfillTarget(t, f)
	first := causeBackfillRequest(f, target)
	second := first
	second.IdempotencyKey = "backfill-2"
	second.ActionID = createSecondaryAction(t, f.h, f.mitigation, true).ID

	type result struct {
		actionID string
		changed  bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, req := range []LinkCauseRequest{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, changed, err := f.h.svc.LinkIncidentCause(f.h.ctx, req)
			results <- result{actionID: req.ActionID, changed: changed, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded := 0
	winningAction := ""
	for result := range results {
		switch {
		case result.err == nil && result.changed:
			succeeded++
			winningAction = result.actionID
		case errors.Is(result.err, work.ErrVersionConflict) && !result.changed:
		default:
			t.Fatalf("unexpected concurrent result=%+v", result)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful writers=%d", succeeded)
	}
	subjects, err := f.h.store.ListSubjects(f.h.ctx, target.TenantID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	links, err := work.CausalLinks(subjects)
	if err != nil || len(links) != 2 || links[0].ID != f.source.ID || links[1].ID != winningAction {
		t.Fatalf("final links=%+v err=%v winning action=%s", links, err, winningAction)
	}
	if events := incidentCauseEvents(t, f, target.ID); len(events) != 1 {
		t.Fatalf("cause events=%+v", events)
	}
}
