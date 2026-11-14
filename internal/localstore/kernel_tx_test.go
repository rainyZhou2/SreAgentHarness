package localstore

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func objectRows(t *testing.T, s *Store) map[string]string {
	t.Helper()
	rows, err := s.db.Query(`SELECT kind, id, data FROM objects ORDER BY kind, id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var kind, id, data string
		if err := rows.Scan(&kind, &id, &data); err != nil {
			t.Fatal(err)
		}
		out[objectKey(kind, id)] = data
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestKernelSQLFailureRollsBackState(t *testing.T) {
	for _, op := range []string{"claim", "renew", "success", "failure", "prepare", "record"} {
		t.Run(op, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "h.db")
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			parent, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "p"})
			if err != nil {
				t.Fatal(err)
			}
			r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h", ParentRunID: &parent.ID})
			if err != nil {
				t.Fatal(err)
			}
			s.saveFlow(domain.FlowSnapshot{ID: parent.ID, RunID: parent.ID, Nodes: []domain.FlowNodeRun{{ChildRunID: r.ID, Status: "RUNNING"}}})
			var l domain.Lease
			if op != "claim" {
				l, err = s.ClaimStep(ctx, r.ID, "worker", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
			}
			if op == "record" {
				if _, _, err := s.PrepareEffect(ctx, l, "target"); err != nil {
					t.Fatal(err)
				}
			}
			before, objects := s.snapshotStateLocked(), objectRows(t, s)
			flowBefore, _ := json.Marshal(s.flows[parent.ID])
			snapshots := cloneMap(s.snapshots)
			if _, err := s.db.Exec(`CREATE TRIGGER fail_kernel BEFORE INSERT ON objects BEGIN SELECT RAISE(ABORT, 'injected kernel write failure'); END`); err != nil {
				t.Fatal(err)
			}
			switch op {
			case "claim":
				_, err = s.ClaimStep(ctx, r.ID, "worker", time.Minute)
			case "renew":
				_, err = s.RenewLease(ctx, l, 2*time.Minute)
			case "success":
				err = s.CommitSuccess(ctx, l, domain.StepSuccess{Complete: true, ResultRef: "result"})
			case "failure":
				err = s.CommitFailure(ctx, l, domain.StepFailure{Class: domain.FailPermanent, Message: "failure"})
			case "prepare":
				_, _, err = s.PrepareEffect(ctx, l, "target")
			case "record":
				err = s.RecordEffectResult(ctx, l, "target", "result")
			}
			if err == nil {
				t.Error("kernel swallowed persistence failure")
			}
			if !reflect.DeepEqual(before, s.snapshotStateLocked()) {
				t.Error("failed kernel write changed memory")
			}
			if !reflect.DeepEqual(snapshots, s.snapshots) {
				t.Error("failed kernel write changed context snapshots")
			}
			flowAfter, _ := json.Marshal(s.flows[parent.ID])
			if string(flowAfter) != string(flowBefore) {
				t.Error("failed kernel write changed parent flow nodes")
			}
			if !reflect.DeepEqual(objects, objectRows(t, s)) {
				t.Error("failed kernel write partially persisted objects")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if !reflect.DeepEqual(objects, objectRows(t, reopened)) {
				t.Error("reopened database differs after rejected write")
			}
			if op == "renew" {
				// The persisted lease survives the rejected heartbeat and can be
				// renewed after the fault is removed, even after reopening.
				if _, err := reopened.db.Exec(`DROP TRIGGER fail_kernel`); err != nil {
					t.Fatal(err)
				}
				previous := reopened.runs[r.ID]
				if _, err := reopened.RenewLease(ctx, l, 2*time.Minute); err != nil {
					t.Fatalf("renewal retry: %v", err)
				}
				next := reopened.runs[r.ID]
				if !next.LeaseUntil.After(*previous.LeaseUntil) || next.Version != previous.Version || next.FenceToken != previous.FenceToken {
					t.Fatal("renewal retry must extend only the lease deadline")
				}
			}
		})
	}
}

func TestBufferedCheckpointPreservesEventBoundary(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CommitSuccess(ctx, l, domain.StepSuccess{CheckpointRef: "manifest", Complete: true}); err != nil {
		t.Fatal(err)
	}
	events, err := s.listEventsLocked(ctx, r.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	var succeeded int64
	for _, event := range events {
		if event.Type == "step.succeeded" {
			succeeded = event.Seq
		}
	}
	if succeeded == 0 {
		t.Fatal("missing success event")
	}
	for _, cp := range s.checkpoints {
		if cp.RunID == r.ID && cp.LastEventSeq != succeeded {
			t.Fatalf("checkpoint cursor %d, want committed step event %d", cp.LastEventSeq, succeeded)
		}
	}
}

// A heartbeat updates one leased Run. Its allocation cost must not grow
// with unrelated completed history kept by the local process.
func TestRenewLeaseAllocationDoesNotScaleWithHistory(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	r, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
	if err != nil {
		t.Fatal(err)
	}
	l, err := s.ClaimStep(ctx, r.ID, "worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	measure := func() float64 {
		return testing.AllocsPerRun(5, func() {
			if _, err := s.RenewLease(ctx, l, time.Minute); err != nil {
				t.Fatal(err)
			}
		})
	}
	small := measure()
	for i := 0; i < 2000; i++ {
		id := fmt.Sprintf("history-%d", i)
		s.runs[id] = domain.Run{ID: id, TenantID: "t", Status: domain.RunCompleted}
	}
	large := measure()
	// Allow ordinary driver/runtime variance without accepting one allocation
	// per historical Run from copying the complete working set.
	if large > small+64 {
		t.Fatalf("renewal allocations grew with history: small %.0f, large %.0f", small, large)
	}
}
