package localstore

import (
	"context"
	"errors"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

func TestEventFailureRollsBackCommandAndFailure(t *testing.T) {
	for _, op := range []string{"pause", "reject", "fail"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			s, err := Open(filepath.Join(t.TempDir(), "h.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
			if err != nil {
				t.Fatal(err)
			}
			before, objects := s.snapshotStateLocked(), objectRows(t, s)
			if _, err := s.db.Exec(`CREATE TRIGGER fail_event BEFORE INSERT ON objects WHEN NEW.kind='event' BEGIN SELECT RAISE(ABORT,'injected event failure'); END`); err != nil {
				t.Fatal(err)
			}
			switch op {
			case "pause":
				_, err = s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "pause"})
			case "reject":
				v := r.Version + 1
				_, err = s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdPause, ExpectedVersion: &v, IdempotencyKey: "reject"})
			case "fail":
				err = s.FailRun(ctx, "t", r.ID, "failed")
			}
			var rejected *domain.RejectedError
			if err == nil || errors.As(err, &rejected) {
				t.Errorf("want persistence error, got %v", err)
			}
			if !reflect.DeepEqual(before, s.snapshotStateLocked()) || !reflect.DeepEqual(objects, objectRows(t, s)) {
				t.Fatal("event failure left state or idempotency key committed")
			}
		})
	}
}

func TestDatabaseSnapshotContainsCompleteEventHistory(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
	if err != nil {
		t.Fatal(err)
	}
	events, err := s.ListEvents(ctx, "t", r.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := s.Snapshot(path); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, err := restored.ListEvents(ctx, "t", r.ID, 0, 100)
	if err != nil || !reflect.DeepEqual(events, got) {
		t.Fatalf("snapshot lost events: want %d got %d err=%v", len(events), len(got), err)
	}
	if err := restored.FailRun(ctx, "t", r.ID, "stop"); err != nil {
		t.Fatal(err)
	}
	next, err := restored.ListEvents(ctx, "t", r.ID, r.LastEventSeq, 100)
	if err != nil || len(next) != 1 || next[0].Seq != r.LastEventSeq+1 {
		t.Fatalf("restored cursor: %+v %v", next, err)
	}
}

func TestLifecycleEventFailureRollsBack(t *testing.T) {
	for _, op := range []string{"submit", "stalled branch", "fork", "expired lease", "expired approval", "claim", "success", "failure", "effect result"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "h.db")
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { s.Close() })
			r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
			if err != nil {
				t.Fatal(err)
			}
			var runOperation func() error
			switch op {
			case "submit", "stalled branch":
				sess, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t"})
				if err != nil {
					t.Fatal(err)
				}
				req := domain.SubmitTurnRequest{TenantID: "t", SessionID: sess.ID, ClientRequestID: "turn", InputRef: "input"}
				runOperation = func() error { _, err := s.SubmitTurn(ctx, req, domain.BranchQueue, 10); return err }
				if op == "stalled branch" {
					if err := runOperation(); err != nil {
						t.Fatal(err)
					}
					req.ClientRequestID = "second"
					if err := runOperation(); err != nil {
						t.Fatal(err)
					}
					br := s.branches[sess.DefaultBranchID]
					br.ActiveTurnID = nil
					s.saveBranch(br)
					runOperation = func() error {
						n, err := s.ActivateStalledBranches(ctx, 10)
						if err != nil && n != 0 {
							t.Errorf("failed batch reported %d activations", n)
						}
						return err
					}
				}
			case "claim":
				runOperation = func() error { _, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute); return err }
			case "expired approval":
				steps, err := s.ListSteps(ctx, "t", r.ID)
				if err != nil {
					t.Fatal(err)
				}
				a := approvalRec{Request: domain.ApprovalRequest{ID: "approval", Status: "PENDING", ExpiresAt: time.Now().Add(-time.Minute)}, RunID: r.ID, TenantID: "t", StepID: steps[0].ID}
				s.saveApproval(a)
				runOperation = func() error {
					n, err := s.ExpireApprovals(ctx, 10)
					if err != nil && n != 0 {
						t.Errorf("failed batch reported %d expirations", n)
					}
					return err
				}
			default:
				l, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				switch op {
				case "fork":
					if err := s.CommitSuccess(ctx, l, domain.StepSuccess{CheckpointRef: "manifest", CheckpointHash: "hash"}); err != nil {
						t.Fatal(err)
					}
					runOperation = func() error {
						_, _, err := s.ForkRun(ctx, pg.ForkRunRequest{TenantID: "t", SourceRunID: r.ID, IdempotencyKey: "fork"})
						return err
					}
				case "expired lease":
					current := s.runs[r.ID]
					expired := time.Now().Add(-time.Minute)
					current.LeaseUntil = &expired
					s.saveRun(current)
					runOperation = func() error {
						n, err := s.RecoverExpiredLeases(ctx, 10)
						if err != nil && n != 0 {
							t.Errorf("failed batch reported %d recoveries", n)
						}
						return err
					}
				case "success":
					runOperation = func() error {
						return s.CommitSuccess(ctx, l, domain.StepSuccess{Complete: true, CheckpointRef: "manifest"})
					}
				case "failure":
					runOperation = func() error {
						return s.CommitFailure(ctx, l, domain.StepFailure{Class: domain.FailPermanent, Message: "failure"})
					}
				case "effect result":
					if _, _, err := s.PrepareEffect(ctx, l, "target"); err != nil {
						t.Fatal(err)
					}
					runOperation = func() error { return s.RecordEffectResult(ctx, l, "target", "result") }
				}
			}
			before, objects := s.snapshotStateLocked(), objectRows(t, s)
			if _, err := s.db.Exec(`CREATE TRIGGER fail_event BEFORE INSERT ON objects WHEN NEW.kind='event' BEGIN SELECT RAISE(ABORT,'injected event failure'); END`); err != nil {
				t.Fatal(err)
			}
			if err := runOperation(); err == nil {
				t.Fatal("event persistence failure swallowed")
			}
			if !reflect.DeepEqual(before, s.snapshotStateLocked()) || !reflect.DeepEqual(objects, objectRows(t, s)) {
				t.Fatal("failed lifecycle operation partially committed")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(objects, objectRows(t, s)) {
				t.Fatal("partial commit survived restart")
			}
		})
	}
}

func TestEventSerializationFailureRollsBackStep(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before, objects := s.snapshotStateLocked(), objectRows(t, s)
	err = s.CommitSuccess(ctx, l, domain.StepSuccess{Complete: true, Events: []domain.EmittedEvent{{Type: "invalid", Payload: math.NaN()}}})
	if err == nil {
		t.Fatal("invalid event payload committed")
	}
	if !reflect.DeepEqual(before, s.snapshotStateLocked()) || !reflect.DeepEqual(objects, objectRows(t, s)) {
		t.Fatal("serialization failure partially committed")
	}
}

func TestSubmitTurnRequiresSessionOwnedBranch(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	own, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t"})
	if err != nil {
		t.Fatal(err)
	}
	foreign, _, err := s.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	before, objects := s.snapshotStateLocked(), objectRows(t, s)
	_, err = s.SubmitTurn(ctx, domain.SubmitTurnRequest{TenantID: "t", SessionID: own.ID, BranchID: foreign.DefaultBranchID, ClientRequestID: "request", InputRef: "input"}, domain.BranchQueue, 10)
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("foreign branch accepted: %v", err)
	}
	if !reflect.DeepEqual(before, s.snapshotStateLocked()) || !reflect.DeepEqual(objects, objectRows(t, s)) {
		t.Fatal("foreign branch mutated")
	}
}

func TestEventIndexRebuildDoesNotReplayLegacyArchive(t *testing.T) {
	path, file, _ := legacyEventFixture(t)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	objects := objectRows(t, s)
	if err := os.WriteFile(file, []byte("broken archive"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.RebuildEventIndex(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(objects, objectRows(t, s)) {
		t.Fatal("derived index rebuild changed authoritative objects")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListEvents(ctx, "t", "run", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled event query: %v", err)
	}
}

func TestEventCommitSurvivesProcessExit(t *testing.T) {
	const childDB = "GO_SRE_TEST_EVENT_CRASH_DB"
	if path := os.Getenv(childDB); path != "" {
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		r, _, err := s.CreateRun(context.Background(), domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h", IdempotencyKey: "crash"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.ApplyCommand(context.Background(), "t", r.ID, domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "pause"}); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Simulate process loss without Store.Close or a WAL checkpoint.
	}
	path := filepath.Join(t.TempDir(), "h.db")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestEventCommitSurvivesProcessExit$")
	cmd.Env = append(os.Environ(), childDB+"="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process: %v %s", err, output)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, created, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h", IdempotencyKey: "crash"})
	if err != nil || created || r.Status != domain.RunPaused {
		t.Fatalf("lost committed run: %+v %v %v", r, created, err)
	}
	events, err := s.ListEvents(ctx, "t", r.ID, 0, 100)
	if err != nil || int64(len(events)) != r.LastEventSeq || len(events) <= 3 {
		t.Fatalf("lost committed events: %d cursor=%d %v", len(events), r.LastEventSeq, err)
	}
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatal("non-contiguous timeline")
		}
	}
	before := r.LastEventSeq
	if _, err := s.ApplyCommand(ctx, "t", r.ID, domain.CommandRequest{Type: domain.CmdPause, IdempotencyKey: "pause"}); err != nil {
		t.Fatal(err)
	}
	after, err := s.GetRun(ctx, "t", r.ID)
	if err != nil || after.LastEventSeq != before {
		t.Fatal("retry duplicated committed command/events")
	}
}
