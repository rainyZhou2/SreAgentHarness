package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func TestRunReadersEnforceTenantAfterReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	run, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "owner", GoalRef: "secret", GoalHash: "hash", InitiatorID: "operator", InitialSteps: []domain.PlannedStep{
		{Kind: domain.StepReason, Executor: "reasoner", Status: domain.StepPending},
		{Kind: domain.StepTool, Executor: "tool", Status: domain.StepWaitingApproval, Intent: &domain.ActionIntent{ToolName: "secret-tool", Target: "secret-target", ActionHash: "secret-hash"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimStep(ctx, run.ID, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitSuccess(ctx, lease, domain.StepSuccess{Thoughts: []domain.Thought{{Kind: domain.ThoughtObservation, Summary: "private trace"}}, Capabilities: []domain.CapabilityRef{{Ref: "secret:capability", Role: "used"}}}); err != nil {
		t.Fatal(err)
	}
	for _, reopen := range []bool{false, true} {
		if reopen {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, tenant := range []string{"owner", "foreign", ""} {
			for _, id := range []string{run.ID, newID()} {
				allowed := tenant == "owner" && id == run.ID
				events, err := s.ListEvents(ctx, tenant, id, 0, 100)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					t.Fatal(err)
				}
				if allowed && len(events) == 0 || !allowed && len(events) != 0 {
					t.Errorf("events tenant=%q id=%s reopen=%v count=%d", tenant, id, reopen, len(events))
				}
				approvals, err := s.ListApprovals(ctx, tenant, id)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					t.Fatal(err)
				}
				if allowed && len(approvals) != 1 || !allowed && len(approvals) != 0 {
					t.Errorf("approvals tenant=%q reopen=%v count=%d", tenant, reopen, len(approvals))
				}
				thoughts, caps, err := s.RunTrace(ctx, tenant, id)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					t.Fatal(err)
				}
				if allowed {
					if len(thoughts[lease.StepID]) != 1 || len(caps[lease.StepID]) != 1 {
						t.Errorf("owner trace missing after reopen=%v", reopen)
					}
				} else if len(thoughts) != 0 || len(caps) != 0 {
					t.Errorf("trace leak tenant=%q reopen=%v", tenant, reopen)
				}
				got, err := s.GetRun(ctx, tenant, id)
				if allowed && (err != nil || got.ID != run.ID) || !allowed && (err == nil || got.ID != "") {
					t.Errorf("GetRun tenant=%q reopen=%v: %s %v", tenant, reopen, got.ID, err)
				}
				steps, err := s.ListSteps(ctx, tenant, id)
				if allowed && (err != nil || len(steps) != 2) || !allowed && len(steps) != 0 {
					t.Errorf("ListSteps tenant=%q reopen=%v: %d %v", tenant, reopen, len(steps), err)
				}
			}
			runs, err := s.ListRuns(ctx, tenant, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if tenant == "owner" {
				want = 1
			}
			if len(runs) != want {
				t.Errorf("ListRuns tenant=%q: got %d", tenant, len(runs))
			}
			runs, err = s.ListRunsFor(ctx, tenant, "", 100, "operator")
			if err != nil || len(runs) != want {
				t.Errorf("ListRunsFor tenant=%q: got %d %v", tenant, len(runs), err)
			}
		}
	}
}

func TestListEventsBoundsAndCursor(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	run, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "goal", GoalHash: "hash"})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1005; i++ {
		s.appendEvent("t", run.ID, "", "", "test.event", "system", "", nil)
	}
	for _, tc := range []struct {
		limit int
		after int64
		want  int
	}{{0, 0, 1000}, {-1, 0, 1000}, {1001, 0, 1000}, {7, 3, 7}, {1000, 1000, 8}} {
		events, err := s.ListEvents(ctx, "t", run.ID, tc.after, tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != tc.want {
			t.Errorf("limit=%d after=%d: got %d want %d", tc.limit, tc.after, len(events), tc.want)
		}
		for i, e := range events {
			if e.Seq != tc.after+int64(i)+1 {
				t.Fatalf("unordered or repeated cursor: %d", e.Seq)
			}
		}
	}
}
