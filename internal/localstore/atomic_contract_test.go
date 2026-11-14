package localstore

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

func TestCommitSuccessRejectsKVBeforeMutating(t *testing.T) {
	for _, bad := range []string{"foreign scope", "CAS conflict", "repeated key conflict"} {
		t.Run(bad, func(t *testing.T) {
			s, err := Open(filepath.Join(t.TempDir(), "h.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			r, _, err := s.CreateRun(context.Background(), domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
			if err != nil {
				t.Fatal(err)
			}
			l, err := s.ClaimStep(context.Background(), r.ID, "worker", time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			key := domain.KVKey{TenantID: "t", Scope: "run", OwnerID: r.ID, Name: "result"}
			zero, wrong := int64(0), int64(10)
			writes := []domain.KVWrite{{Key: key, Data: []byte(`{"ok":true}`), ExpectedVersion: &zero}}
			invalid := domain.KVWrite{Key: key, Data: []byte(`{}`), ExpectedVersion: &zero}
			switch bad {
			case "foreign scope":
				invalid.Key.OwnerID = "other-run"
			case "CAS conflict":
				invalid.Key.Name = "other"
				invalid.ExpectedVersion = &wrong
			}
			writes = append(writes, invalid)
			before := s.snapshotStateLocked()
			if err := s.CommitSuccess(context.Background(), l, domain.StepSuccess{KVWrites: writes, Complete: true, SideEffect: true}); err == nil {
				t.Fatal("invalid KV commit accepted")
			}
			if !reflect.DeepEqual(before, s.snapshotStateLocked()) {
				t.Error("rejected commit changed state")
			}
			if _, ok := s.snapshots[l.ContextSnapshotID]; !ok {
				t.Error("rejected commit discarded snapshot")
			}
			// The same lease must still be usable after correcting the payload.
			if err := s.CommitSuccess(context.Background(), l, domain.StepSuccess{KVWrites: writes[:1], Complete: true}); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

func TestCommandSQLFailureRollsBackObjects(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	r, _, err := s.CreateRun(context.Background(), domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "h"})
	if err != nil {
		t.Fatal(err)
	}
	before := s.snapshotStateLocked()
	var persistedBefore []byte
	if err := s.db.QueryRow(`SELECT data FROM objects WHERE kind='run' AND id=?`, r.ID).Scan(&persistedBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_command BEFORE INSERT ON objects WHEN NEW.kind='command' BEGIN SELECT RAISE(ABORT, 'injected write failure'); END`); err != nil {
		t.Fatal(err)
	}
	request := domain.CommandRequest{Type: domain.CmdPause, ActorID: "operator", IdempotencyKey: "pause"}
	if _, err := s.ApplyCommand(context.Background(), "t", r.ID, request); err == nil {
		t.Error("SQL failure was swallowed")
	}
	if !reflect.DeepEqual(before, s.snapshotStateLocked()) {
		t.Error("failed command changed in-memory state")
	}
	var persistedAfter []byte
	if err := s.db.QueryRow(`SELECT data FROM objects WHERE kind='run' AND id=?`, r.ID).Scan(&persistedAfter); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(persistedBefore, persistedAfter) {
		t.Error("failed command persisted partial run state")
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_command`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ApplyCommand(context.Background(), "t", r.ID, request); err != nil {
		t.Fatalf("retry: %v", err)
	}
}
