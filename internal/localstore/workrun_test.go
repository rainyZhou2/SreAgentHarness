package localstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func workReservation(key string) domain.CreateRunRequest {
	return domain.CreateRunRequest{TenantID: "t", IdempotencyKey: key, InitiatorType: "work", InitiatorID: "work:w", PendingActivation: true, WorkOwner: &domain.WorkRunOwner{WorkID: "w", Generation: 1, StageID: "s"}, InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", Status: domain.StepPending}}}
}

func TestWorkRunReservationActivation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	req := workReservation("k")
	r, created, err := s.CreateRun(ctx, req)
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	owner := *req.WorkOwner
	req.WorkOwner.StageID = "mutated"
	r.WorkOwner.StageID = "mutated-result"
	r, err = s.GetRun(ctx, "t", r.ID)
	if err != nil || *r.WorkOwner != owner {
		t.Fatalf("owner aliases caller: %+v %v", r.WorkOwner, err)
	}
	r.WorkOwner.StageID = "mutated-read"
	req.WorkOwner = &owner
	if retry, new, err := s.CreateRun(ctx, req); err != nil || new || retry.ID != r.ID {
		t.Fatalf("retry: %v %v", new, err)
	}
	changed := owner
	changed.Generation++
	req.WorkOwner = &changed
	if _, _, err := s.CreateRun(ctx, req); err == nil {
		t.Fatal("accepted changed reservation owner")
	}
	req.WorkOwner = nil
	if _, _, err := s.CreateRun(ctx, req); err == nil {
		t.Fatal("accepted missing reservation owner")
	}
	if _, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("reserved run claim: %v", err)
	}
	if err := s.ActivateWorkRun(ctx, "foreign", r.ID, owner); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("foreign tenant: %v", err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, changed); err == nil {
		t.Fatal("accepted changed owner")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, owner); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, owner); err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(ctx, "t", r.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range events {
		if e.Type == "run.activated" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("activation events: %d", count)
	}
	lease, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitSuccess(ctx, lease, domain.StepSuccess{Complete: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, owner); err == nil {
		t.Fatal("reactivated terminal run")
	}
}

func TestWorkRunValidationAndPause(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for name, mutate := range map[string]func(*domain.CreateRunRequest){
		"unreserved":    func(r *domain.CreateRunRequest) { r.PendingActivation = false },
		"invalid owner": func(r *domain.CreateRunRequest) { r.WorkOwner.Generation = 0 },
		"session":       func(r *domain.CreateRunRequest) { r.SessionID = strptr("x") },
		"branch":        func(r *domain.CreateRunRequest) { r.BranchID = strptr("x") },
		"turn":          func(r *domain.CreateRunRequest) { r.TurnID = strptr("x") },
		"parent":        func(r *domain.CreateRunRequest) { r.ParentRunID = strptr("x") },
		"parent step":   func(r *domain.CreateRunRequest) { r.ParentStepID = strptr("x") },
		"initiator":     func(r *domain.CreateRunRequest) { r.InitiatorID = "x" },
	} {
		t.Run(name, func(t *testing.T) {
			req := workReservation(name)
			mutate(&req)
			if _, _, err := s.CreateRun(ctx, req); err == nil {
				t.Fatal("invalid reservation accepted")
			}
		})
	}
	for _, cancel := range []bool{false, true} {
		req := workReservation(fmt.Sprint(cancel))
		r, _, err := s.CreateRun(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdPause}); err != nil {
			t.Fatal(err)
		}
		r, _ = s.GetRun(ctx, "t", r.ID)
		if r.Status != domain.RunPendingActivation || r.DesiredState != domain.DesiredPaused {
			t.Fatal("pause escaped activation barrier")
		}
		if cancel {
			if _, err := s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdCancel}); err != nil {
				t.Fatal(err)
			}
			if err := s.ActivateWorkRun(ctx, "t", r.ID, *req.WorkOwner); err == nil {
				t.Fatal("canceled reservation activated")
			}
			continue
		}
		if err := s.ActivateWorkRun(ctx, "t", r.ID, *req.WorkOwner); err != nil {
			t.Fatal(err)
		}
		r, _ = s.GetRun(ctx, "t", r.ID)
		if r.Status != domain.RunPaused {
			t.Fatalf("pause lost: %s", r.Status)
		}
		if _, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
			t.Fatalf("paused claim: %v", err)
		}
	}
	// An ordinary pending conversation cannot be activated by the Work API.
	req := workReservation("conversation")
	owner := *req.WorkOwner
	req.WorkOwner = nil
	req.SessionID = strptr("session")
	r, _, err := s.CreateRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, owner); err == nil {
		t.Fatal("conversation activated")
	}
}

func TestWorkRunActivationRollbackAndInventory(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	req := workReservation("rollback")
	r, _, err := s.CreateRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	before := eventFiles(t, s)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_activation BEFORE INSERT ON objects WHEN NEW.kind='run' BEGIN SELECT RAISE(ABORT, 'injected activation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.ActivateWorkRun(ctx, "t", r.ID, *req.WorkOwner); err == nil {
		t.Fatal("write failure swallowed")
	}
	got, _ := s.GetRun(ctx, "t", r.ID)
	if got.Status != domain.RunPendingActivation || got.Version != r.Version {
		t.Fatal("failed activation changed state")
	}
	if fmt.Sprint(before) != fmt.Sprint(eventFiles(t, s)) {
		t.Fatal("failed activation emitted event")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_activation`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		req := workReservation(fmt.Sprint(i))
		req.TenantID = fmt.Sprint(i)
		if _, _, err := s.CreateRun(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	req = workReservation("legacy")
	req.WorkOwner = nil
	if _, _, err := s.CreateRun(ctx, req); err != nil {
		t.Fatal(err)
	}
	after := ""
	count := 0
	for {
		runs, err := s.ListWorkRuns(ctx, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) > 2 {
			t.Fatal("limit ignored")
		}
		if len(runs) == 0 {
			break
		}
		for _, run := range runs {
			if run.ID <= after || run.WorkOwner == nil {
				t.Fatal("invalid inventory")
			}
			after = run.ID
			count++
		}
	}
	if count != 5 {
		t.Fatalf("inventory count %d", count)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ListWorkRuns(canceled, "", 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("context ignored: %v", err)
	}
}

func TestWorkRunInventoryCapAndTerminalExclusion(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 205; i++ {
		req := workReservation(fmt.Sprint(i))
		r, _, err := s.CreateRun(ctx, req)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if _, err := s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdCancel}); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, limit := range []int{0, 500} {
		runs, err := s.ListWorkRuns(ctx, "", limit)
		if err != nil || len(runs) != 200 {
			t.Fatalf("limit %d: %d %v", limit, len(runs), err)
		}
		tail, err := s.ListWorkRuns(ctx, runs[199].ID, 200)
		if err != nil || len(tail) != 4 {
			t.Fatalf("tail: %d %v", len(tail), err)
		}
		for _, r := range append(runs, tail...) {
			if r.Status.Terminal() {
				t.Fatal("terminal inventory entry")
			}
		}
	}
}
