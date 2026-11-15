package cpsql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

func secondaryTestAction(t *testing.T, s *Store, w work.Work) action.Record {
	t.Helper()
	p := action.Plan{ID: cpkit.NewID(), TenantID: w.TenantID, WorkID: w.ID, Generation: w.Generation,
		ActionName: "deployment.rollback", ActionVersion: "1", Risk: domain.RiskR1,
		TargetRef: "deployment://prod/checkout", Args: json.RawMessage(`{}`),
		CreatedAt: t0, ExpiresAt: t0.Add(time.Hour)}
	p.Seal()
	r, _, err := s.CreateAction(context.Background(), action.Record{Plan: p, State: action.StateDispatched,
		MayHaveDispatched: true, DispatchedAt: &t0, UpdatedAt: t0})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func createSecondaryTestWork(t *testing.T, s *Store, tenantID, sourceID, actionID, key string, at time.Time) work.Work {
	t.Helper()
	n := newWork(tenantID, key)
	n.Now = at
	n.Subjects = append(n.Subjects,
		work.SubjectLink{Kind: work.SubjectCausedByWork, ID: sourceID},
		work.SubjectLink{Kind: work.SubjectCausedByAction, ID: actionID})
	w, created, err := s.CreateWork(context.Background(), n)
	if err != nil || !created {
		t.Fatalf("create secondary: created=%v err=%v", created, err)
	}
	return w
}

func TestSecondaryIncidentsForScopesAndOrdersResults(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		source := mustCreate(t, s, tn)
		mitigation := mustCreate(t, s, tn)
		act := secondaryTestAction(t, s, mitigation)
		first := createSecondaryTestWork(t, s, tn, source.ID, act.ID, "secondary-first", t0)
		second := createSecondaryTestWork(t, s, tn, source.ID, act.ID, "secondary-second", t0.Add(time.Minute))
		otherSource := mustCreate(t, s, tn)
		createSecondaryTestWork(t, s, tn, otherSource.ID, act.ID, "secondary-other-source", t0.Add(2*time.Minute))
		otherTenant := tenant()
		otherTenantSource := mustCreate(t, s, otherTenant)
		createSecondaryTestWork(t, s, otherTenant, otherTenantSource.ID, act.ID, "secondary-other-tenant", t0.Add(3*time.Minute))

		got, err := s.SecondaryIncidentsFor(ctx, tn, source.ID)
		if err != nil || len(got) != 2 {
			t.Fatalf("secondary=%+v err=%v", got, err)
		}
		if got[0].Work.ID != second.ID || got[1].Work.ID != first.ID {
			t.Fatalf("wrong order: %+v", got)
		}
		for _, item := range got {
			if item.ActionID != act.ID || item.MitigationWorkID != mitigation.ID {
				t.Fatalf("wrong causal IDs: %+v", item)
			}
		}
		if cross, err := s.SecondaryIncidentsFor(ctx, otherTenant, source.ID); err != nil || len(cross) != 0 {
			t.Fatalf("cross-tenant result=%+v err=%v", cross, err)
		}
		links, err := s.ListSubjects(ctx, tn, first.ID)
		if err != nil {
			t.Fatal(err)
		}
		if causal, err := work.CausalLinks(links); err != nil || len(causal) != 2 {
			t.Fatalf("persisted causal links=%+v err=%v", causal, err)
		}
		events, err := s.ListWorkEvents(ctx, tn, first.ID, 0, 10)
		if err != nil || len(events) != 1 {
			t.Fatalf("events=%+v err=%v", events, err)
		}
		var payload struct {
			CausedBy struct {
				IncidentID string `json:"incident_id"`
				ActionID   string `json:"action_id"`
			} `json:"caused_by"`
		}
		if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.CausedBy.IncidentID != source.ID || payload.CausedBy.ActionID != act.ID {
			t.Fatalf("creation event omitted cause: %s", events[0].Payload)
		}
		if _, err := s.SecondaryIncidentsFor(ctx, tn, ""); !errors.Is(err, work.ErrInvalid) {
			t.Fatalf("empty source accepted: %v", err)
		}
	})
}

func TestSecondaryIncidentsForRejectsInconsistentLinks(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		for _, tc := range []struct {
			name    string
			corrupt func(t *testing.T, tenantID, secondaryID, actionID string)
		}{
			{name: "missing action link", corrupt: func(t *testing.T, tenantID, secondaryID, _ string) {
				_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM work_subject_links WHERE tenant_id=? AND work_id=? AND subject_kind='caused_by_action'`), tenantID, secondaryID)
				if err != nil {
					t.Fatal(err)
				}
			}},
			{name: "missing action", corrupt: func(t *testing.T, tenantID, _, actionID string) {
				_, err := s.db.ExecContext(ctx, s.q(`DELETE FROM work_actions WHERE tenant_id=? AND id=?`), tenantID, actionID)
				if err != nil {
					t.Fatal(err)
				}
			}},
			{name: "duplicate action link", corrupt: func(t *testing.T, tenantID, secondaryID, _ string) {
				_, err := s.db.ExecContext(ctx, s.q(`INSERT INTO work_subject_links (tenant_id, work_id, subject_kind, subject_id, created_at)
					VALUES (?,?,?,?,?)`), tenantID, secondaryID, work.SubjectCausedByAction, cpkit.NewID(), t0)
				if err != nil {
					t.Fatal(err)
				}
			}},
			{name: "non incident", corrupt: func(t *testing.T, tenantID, secondaryID, _ string) {
				_, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET kind='routine' WHERE tenant_id=? AND id=?`), tenantID, secondaryID)
				if err != nil {
					t.Fatal(err)
				}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tn := tenant()
				source := mustCreate(t, s, tn)
				mitigation := mustCreate(t, s, tn)
				act := secondaryTestAction(t, s, mitigation)
				secondary := createSecondaryTestWork(t, s, tn, source.ID, act.ID, cpkit.NewID(), t0)
				tc.corrupt(t, tn, secondary.ID, act.ID)
				if got, err := s.SecondaryIncidentsFor(ctx, tn, source.ID); err == nil {
					t.Fatalf("inconsistent causal data returned success: %+v", got)
				}
			})
		}
	})
}

func TestCausalCreateRejectsInvalidStructure(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		for _, tc := range []struct {
			name     string
			kind     work.Kind
			subjects []work.SubjectLink
		}{
			{name: "single link", kind: work.KindIncident, subjects: []work.SubjectLink{{Kind: work.SubjectCausedByWork, ID: "source"}}},
			{name: "non incident", kind: work.KindRoutine, subjects: []work.SubjectLink{{Kind: work.SubjectCausedByWork, ID: "source"}, {Kind: work.SubjectCausedByAction, ID: "action"}}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				n := newWork(tenant(), cpkit.NewID())
				n.Kind = tc.kind
				n.Subjects = append(n.Subjects, tc.subjects...)
				if _, _, err := s.CreateWork(context.Background(), n); !errors.Is(err, work.ErrInvalid) {
					t.Fatalf("invalid causal create accepted: %v", err)
				}
			})
		}
	})
}
