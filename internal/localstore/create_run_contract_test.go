package localstore

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"go-sre-harness/internal/domain"
)

func eventFiles(t *testing.T, s *Store) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(s.eventDir, func(path string, entry fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err == nil {
			out[path] = string(data)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCreateRunRollsBackFailure(t *testing.T) {
	for _, failure := range []string{"run", "runkey", "step", "cancelled context"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "h.db")
			s, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			before, objects, files := s.snapshotStateLocked(), objectRows(t, s), eventFiles(t, s)
			ctx := context.Background()
			if failure == "cancelled context" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				if _, err := s.db.Exec(`CREATE TRIGGER fail_create BEFORE INSERT ON objects WHEN NEW.kind='` + failure + `' BEGIN SELECT RAISE(ABORT, 'injected create failure'); END`); err != nil {
					t.Fatal(err)
				}
			}
			req := domain.CreateRunRequest{TenantID: "t", GoalRef: "goal", GoalHash: "hash", IdempotencyKey: "create"}
			run, created, err := s.CreateRun(ctx, req)
			if err == nil {
				t.Error("CreateRun swallowed failure")
			}
			if failure == "cancelled context" && !errors.Is(err, context.Canceled) {
				t.Errorf("got %v, want context.Canceled", err)
			}
			if run.ID != "" || created {
				t.Errorf("failed create returned successful outputs: %s %v", run.ID, created)
			}
			if !reflect.DeepEqual(before, s.snapshotStateLocked()) {
				t.Error("failed create changed in-memory state")
			}
			if !reflect.DeepEqual(objects, objectRows(t, s)) {
				t.Error("failed create persisted partial objects")
			}
			if !reflect.DeepEqual(files, eventFiles(t, s)) {
				t.Error("failed create emitted events")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(objects, objectRows(t, s)) || len(s.runs) != 0 || len(s.steps) != 0 || len(s.runByKey) != 0 || len(s.events) != 0 {
				t.Error("failed create survived reopen")
			}
			if failure != "cancelled context" {
				if _, err := s.db.Exec(`DROP TRIGGER fail_create`); err != nil {
					t.Fatal(err)
				}
			}
			run, created, err = s.CreateRun(context.Background(), req)
			if err != nil || !created || run.ID == "" {
				t.Fatalf("retry: run=%s created=%v err=%v", run.ID, created, err)
			}
			if len(s.runs) != 1 || len(s.steps) != 2 || len(s.runByKey) != 1 {
				t.Fatal("retry did not create exactly one run and its steps/key")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
			replay, created, err := s.CreateRun(context.Background(), req)
			if err != nil || created || replay.ID != run.ID {
				t.Fatalf("reopened idempotent retry: %s %v %v", replay.ID, created, err)
			}
			events, err := s.ListEvents(context.Background(), "t", run.ID, 0, 100)
			if err != nil || len(events) != 3 {
				t.Fatalf("retry events: %d %v", len(events), err)
			}
		})
	}
}

func TestCreateRunReturnsCommittedEventCursor(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "h.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	req := domain.CreateRunRequest{TenantID: "t", GoalRef: "goal", GoalHash: "hash", IdempotencyKey: "create"}
	run, created, err := s.CreateRun(ctx, req)
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	stored, err := s.GetRun(ctx, "t", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LastEventSeq != 3 || !reflect.DeepEqual(run, stored) {
		t.Errorf("CreateRun returned stale run: event cursor=%d stored=%d", run.LastEventSeq, stored.LastEventSeq)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := s.GetRun(ctx, "t", run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.LastEventSeq != reopened.LastEventSeq {
		t.Errorf("returned cursor=%d reopened=%d", run.LastEventSeq, reopened.LastEventSeq)
	}
	replay, created, err := s.CreateRun(ctx, req)
	if err != nil || created || !reflect.DeepEqual(replay, reopened) {
		t.Errorf("idempotent create did not return committed run: %v %v", created, err)
	}
}
