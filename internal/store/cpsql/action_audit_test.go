package cpsql

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/work"
)

func TestActionStateAndAuditCommitTogether(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		w, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, Status: st(work.StatusActive), ActorType: "system", Now: t0})
		if err != nil {
			t.Fatal(err)
		}
		a := readyAction(t, s, w)
		check := func(want int, state action.State) {
			t.Helper()
			events, err := s.ListWorkEvents(ctx, tn, w.ID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			var count int
			var last struct {
				ActionID   string       `json:"action_id"`
				State      action.State `json:"state"`
				Version    int64        `json:"action_version"`
				Generation int          `json:"generation"`
			}
			for _, e := range events {
				if e.Type != "action.state_changed" {
					continue
				}
				count++
				if err := json.Unmarshal(e.Payload, &last); err != nil {
					t.Fatal(err)
				}
			}
			if count != want || last.ActionID != a.ID || last.State != state || last.Version != int64(want) || last.Generation != w.Generation {
				t.Fatalf("audit count=%d last=%+v expected %d/%s", count, last, want, state)
			}
		}
		check(1, action.StateReady)
		a, err = dispatchAction(s, a)
		if err != nil {
			t.Fatal(err)
		}
		check(2, action.StateDispatched)
		if _, err := s.UpdateAction(ctx, a, 1); !errors.Is(err, action.ErrVersionConflict) {
			t.Fatalf("stale writer: %v", err)
		}
		check(2, action.StateDispatched)
		// Reject only this Work's audit insert to fail after the action row update.
		if s.d == Postgres {
			_, err = s.db.ExecContext(ctx, `ALTER TABLE work_events ADD CONSTRAINT test_action_audit CHECK (work_id <> '`+w.ID+`' OR type <> 'action.state_changed') NOT VALID`)
			defer func() {
				_, e := s.db.ExecContext(ctx, `ALTER TABLE work_events DROP CONSTRAINT IF EXISTS test_action_audit`)
				if e != nil {
					t.Error(e)
				}
			}()
		} else {
			_, err = s.db.ExecContext(ctx, `CREATE TRIGGER test_action_audit BEFORE INSERT ON work_events WHEN NEW.work_id='`+w.ID+`' AND NEW.type='action.state_changed' BEGIN SELECT RAISE(ABORT, 'audit fault'); END`)
			defer func() {
				_, e := s.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS test_action_audit`)
				if e != nil {
					t.Error(e)
				}
			}()
		}
		if err != nil {
			t.Fatal(err)
		}
		updated := a
		updated.State = action.StateApplied
		if _, err := s.UpdateAction(ctx, updated, a.Version); err == nil {
			t.Fatal("action committed without its audit")
		}
		got, err := s.GetAction(ctx, tn, a.ID)
		if err != nil || got.State != a.State || got.Version != a.Version {
			t.Fatalf("rollback lost: %+v %v", got, err)
		}
		check(2, action.StateDispatched)
		if s.d == Postgres {
			_, err = s.db.ExecContext(ctx, `ALTER TABLE work_events DROP CONSTRAINT test_action_audit`)
		} else {
			_, err = s.db.ExecContext(ctx, `DROP TRIGGER test_action_audit`)
		}
		if err != nil {
			t.Fatal(err)
		}
		w, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: w.ID, Status: st(work.StatusCancelled), ActorType: "system", Now: t0})
		if err != nil {
			t.Fatal(err)
		}
		a, err = s.UpdateAction(ctx, updated, a.Version)
		if err != nil {
			t.Fatal(err)
		}
		check(3, action.StateApplied)
		before, err := s.GetWork(ctx, tn, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		start, results := make(chan struct{}), make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				<-start
				next := a
				next.State = action.StateVerifying
				_, err := s.UpdateAction(ctx, next, a.Version)
				results <- err
			}()
		}
		close(start)
		successes := 0
		for i := 0; i < 2; i++ {
			if err := <-results; err == nil {
				successes++
			} else if !errors.Is(err, action.ErrVersionConflict) {
				t.Fatal(err)
			}
		}
		if successes != 1 {
			t.Fatalf("successful concurrent writers=%d", successes)
		}
		check(4, action.StateVerifying)
		after, err := s.GetWork(ctx, tn, w.ID)
		if err != nil || after.Status != work.StatusCancelled || after.Version != before.Version || after.LastSeq != before.LastSeq+1 {
			t.Fatalf("audit corrupted Work state/cursor: before=%+v after=%+v err=%v", before, after, err)
		}
	})
}
