package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func TestPendingActivationPauseCommands(t *testing.T) {
	for _, action := range []string{"resume before activation", "activate while paused", "cancel while paused"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "h.db")
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			session, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t", ActorID: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			submit := func(key string) domain.SubmittedTurn {
				t.Helper()
				turn, err := s.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: "t", SessionID: session.ID, ActorID: "operator", ClientRequestID: key, InputRef: key, InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", Status: domain.StepPending}}}, domain.BranchQueue, 10)
				if err != nil {
					t.Fatal(err)
				}
				return turn
			}
			first, second := submit("first"), submit("second")
			if first.Queued || !second.Queued {
				t.Fatal("expected second turn to queue behind first")
			}
			command := func(typ domain.CommandType) error {
				t.Helper()
				run, err := s.GetRun(ctx, "t", second.RunID)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.ApplyCommand(ctx, "t", second.RunID, domain.CommandRequest{Type: typ, ActorID: "operator", ExpectedVersion: &run.Version})
				return err
			}
			assertPending := func(desired domain.DesiredState) {
				t.Helper()
				run, err := s.GetRun(ctx, "t", second.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if run.Status != domain.RunPendingActivation || run.DesiredState != desired {
					t.Errorf("pending run escaped activation barrier: status=%s desired=%s", run.Status, run.DesiredState)
				}
				turn := s.turns[second.TurnID]
				if turn.Status != domain.TurnQueued || turn.ActivatedAt != nil || turn.InputMessageCursor != nil {
					t.Error("pending turn was activated")
				}
				if deref(s.branches[session.DefaultBranchID].ActiveTurnID) != first.TurnID || len(s.messages[session.DefaultBranchID]) != 1 {
					t.Error("pending commands changed branch messages or active turn")
				}
				if _, err := s.ClaimStep(ctx, second.RunID, "early-worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
					t.Errorf("preactivation claim: %v", err)
				}
			}
			assertPending(domain.DesiredRunning)
			if err := command(domain.CmdPause); err != nil {
				t.Fatal(err)
			}
			assertPending(domain.DesiredPaused)
			version := s.runs[second.RunID].Version
			if err := command(domain.CmdPause); err != nil {
				t.Fatal(err)
			}
			if s.runs[second.RunID].Version != version {
				t.Error("repeated pending pause changed version")
			}
			assertPending(domain.DesiredPaused)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			assertPending(domain.DesiredPaused)
			switch action {
			case "resume before activation":
				if err := command(domain.CmdResume); err != nil {
					t.Fatal(err)
				}
				assertPending(domain.DesiredRunning)
				version = s.runs[second.RunID].Version
				if err := command(domain.CmdResume); err == nil {
					t.Error("repeated pending resume accepted without a pause")
				}
				if s.runs[second.RunID].Version != version {
					t.Error("rejected resume changed version")
				}
				assertPending(domain.DesiredRunning)
			case "cancel while paused":
				if err := command(domain.CmdCancel); err != nil {
					t.Fatal(err)
				}
				if s.runs[second.RunID].Status != domain.RunCancelled || s.turns[second.TurnID].Status != domain.TurnCancelled {
					t.Fatal("pending cancellation did not settle run and turn")
				}
			}
			lease, err := s.ClaimStep(ctx, first.RunID, "worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.CommitSuccess(ctx, lease, domain.StepSuccess{Complete: true, ResultRef: "first-result"}); err != nil {
				t.Fatal(err)
			}
			run := s.runs[second.RunID]
			turn := s.turns[second.TurnID]
			if action == "cancel while paused" {
				if run.Status != domain.RunCancelled || turn.ActivatedAt != nil || s.branches[session.DefaultBranchID].ActiveTurnID != nil {
					t.Fatal("canceled queued turn was activated")
				}
				if _, err := s.ClaimStep(ctx, second.RunID, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
					t.Fatalf("canceled claim: %v", err)
				}
				return
			}
			if turn.Status != domain.TurnRunning || turn.ActivatedAt == nil || turn.InputMessageCursor == nil || *turn.InputMessageCursor != 3 {
				t.Fatal("activation did not establish conversation cursor")
			}
			if action == "activate while paused" {
				if run.Status != domain.RunPaused || run.DesiredState != domain.DesiredPaused {
					t.Fatalf("activation ignored pause: %s %s", run.Status, run.DesiredState)
				}
				if _, err := s.ClaimStep(ctx, second.RunID, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
					t.Fatalf("paused activation claim: %v", err)
				}
				if err := command(domain.CmdResume); err != nil {
					t.Fatal(err)
				}
			}
			lease, err = s.ClaimStep(ctx, second.RunID, "worker", time.Minute)
			if err != nil {
				t.Fatalf("activated run not claimable: %v", err)
			}
			if lease.RunID != second.RunID {
				t.Fatal("claimed wrong run")
			}
		})
	}
}
