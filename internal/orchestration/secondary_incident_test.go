package orchestration

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

type secondaryCreateFixture struct {
	h          *harness
	source     work.Work
	mitigation work.Work
	action     action.Record
}

func newSecondaryCreateFixture(t *testing.T) *secondaryCreateFixture {
	t.Helper()
	h := newHarness(t, domain.RiskR1)
	source := h.create("incident", nil)
	mitigation := createSecondaryChild(t, h, source, "mitigation")
	return &secondaryCreateFixture{h: h, source: source, mitigation: mitigation, action: createSecondaryAction(t, h, mitigation, true)}
}

func createSecondaryChild(t *testing.T, h *harness, parent work.Work, role string) work.Work {
	t.Helper()
	child, created, err := h.svc.CreateChild(h.ctx, ChildRequest{TenantID: parent.TenantID, ParentID: parent.ID,
		ActorID: "alice", ActorType: "user", IdempotencyKey: cpkit.NewID(), Kind: work.KindIncident, TemplateID: "mitigate",
		Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{"deployment://prod/checkout"},
			Labels: map[string]string{"child_key": role}}})
	if err != nil || !created {
		t.Fatalf("create child: created=%v err=%v", created, err)
	}
	return child
}

func createSecondaryAction(t *testing.T, h *harness, w work.Work, dispatched bool) action.Record {
	t.Helper()
	now := h.clock.Now()
	p := action.Plan{ID: cpkit.NewID(), TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation,
		ActionName: "deployment.rollback", ActionVersion: "1", Risk: domain.RiskR1,
		TargetRef: "deployment://prod/checkout", Args: json.RawMessage(`{"request":"` + cpkit.NewID() + `"}`),
		CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	p.Seal()
	r := action.Record{Plan: p, State: action.StateReady, UpdatedAt: now}
	if dispatched {
		r.State, r.MayHaveDispatched, r.DispatchedAt = action.StateDispatched, true, &now
	}
	stored, _, err := h.store.CreateAction(h.ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	return stored
}

func (f *secondaryCreateFixture) request() CreateRequest {
	return CreateRequest{TenantID: f.source.TenantID, ActorID: "alice", IdempotencyKey: cpkit.NewID(),
		Kind: work.KindIncident, Goal: work.GoalSpec{Intent: "secondary impact", SubjectRefs: []string{f.action.TargetRef}},
		CausedBy: &CreateCause{IncidentID: f.source.ID, ActionID: f.action.ID}}
}

func TestSecondaryIncidentCreatePersistsAndBindsIdempotency(t *testing.T) {
	f := newSecondaryCreateFixture(t)
	req := f.request()
	secondary, created, err := f.h.svc.Create(f.h.ctx, req)
	if err != nil || !created {
		t.Fatalf("create secondary: created=%v err=%v", created, err)
	}
	links, err := f.h.store.ListSubjects(f.h.ctx, secondary.TenantID, secondary.ID)
	if err != nil {
		t.Fatal(err)
	}
	causal, err := work.CausalLinks(links)
	if err != nil || len(causal) != 2 || causal[0].ID != f.source.ID || causal[1].ID != f.action.ID {
		t.Fatalf("causal links=%+v err=%v", causal, err)
	}
	replay, created, err := f.h.svc.Create(f.h.ctx, req)
	if err != nil || created || replay.ID != secondary.ID {
		t.Fatalf("replay=%+v created=%v err=%v", replay, created, err)
	}
	secondAction := createSecondaryAction(t, f.h, f.mitigation, true)
	req.CausedBy.ActionID = secondAction.ID
	if _, _, err := f.h.svc.Create(f.h.ctx, req); !errors.Is(err, work.ErrIdempotencyConflict) {
		t.Fatalf("different cause reused idempotency key: %v", err)
	}
}

func TestSecondaryIncidentCreateRejectsInvalidEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *secondaryCreateFixture, *CreateRequest)
		want   error
	}{
		{name: "non incident", want: work.ErrInvalid, mutate: func(_ *testing.T, _ *secondaryCreateFixture, req *CreateRequest) {
			req.Kind = work.KindRoutine
		}},
		{name: "empty source", want: work.ErrInvalid, mutate: func(_ *testing.T, _ *secondaryCreateFixture, req *CreateRequest) {
			req.CausedBy.IncidentID = ""
		}},
		{name: "source missing", want: work.ErrNotFound, mutate: func(_ *testing.T, _ *secondaryCreateFixture, req *CreateRequest) {
			req.CausedBy.IncidentID = cpkit.NewID()
		}},
		{name: "source not incident", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			routine, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t1", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindRoutine, Goal: work.GoalSpec{Intent: "routine"}})
			if err != nil {
				t.Fatal(err)
			}
			req.CausedBy.IncidentID = routine.ID
		}},
		{name: "action missing", want: action.ErrNotFound, mutate: func(_ *testing.T, _ *secondaryCreateFixture, req *CreateRequest) {
			req.CausedBy.ActionID = cpkit.NewID()
		}},
		{name: "not dispatched", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			pending := createSecondaryAction(t, f.h, f.mitigation, false)
			req.CausedBy.ActionID = pending.ID
		}},
		{name: "action outside source", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			other := f.h.create("incident", nil)
			outside := createSecondaryAction(t, f.h, other, true)
			req.CausedBy.ActionID = outside.ID
		}},
		{name: "not mitigation child", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			rca := createSecondaryChild(t, f.h, f.source, "rca")
			wrongRole := createSecondaryAction(t, f.h, rca, true)
			req.CausedBy.ActionID = wrongRole.ID
		}},
		{name: "indirect child", want: work.ErrInvalid, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			nested := createSecondaryChild(t, f.h, f.mitigation, "mitigation")
			indirect := createSecondaryAction(t, f.h, nested, true)
			req.CausedBy.ActionID = indirect.ID
		}},
		{name: "target mismatch", want: work.ErrInvalid, mutate: func(_ *testing.T, _ *secondaryCreateFixture, req *CreateRequest) {
			req.Goal.SubjectRefs = []string{"deployment://prod/other"}
		}},
		{name: "cross tenant action", want: action.ErrNotFound, mutate: func(t *testing.T, f *secondaryCreateFixture, req *CreateRequest) {
			other, _, err := f.h.svc.Create(f.h.ctx, CreateRequest{TenantID: "t2", ActorID: "alice", IdempotencyKey: cpkit.NewID(),
				Kind: work.KindIncident, Goal: work.GoalSpec{Intent: "other tenant", SubjectRefs: []string{"deployment://prod/checkout"}}})
			if err != nil {
				t.Fatal(err)
			}
			cross := createSecondaryAction(t, f.h, other, true)
			req.CausedBy.ActionID = cross.ID
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSecondaryCreateFixture(t)
			req := f.request()
			tc.mutate(t, f, &req)
			if _, _, err := f.h.svc.Create(f.h.ctx, req); !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want %v", err, tc.want)
			}
			if _, err := f.h.store.WorkByIdempotencyKey(f.h.ctx, req.TenantID, req.IdempotencyKey); !errors.Is(err, work.ErrNotFound) {
				t.Fatalf("rejected request created a Work: %v", err)
			}
		})
	}
}
