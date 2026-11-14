package localstore

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

// A fence is necessary but insufficient: the holder must still have a live
// lease when it renews, commits, or records an external dispatch/result.
func TestWorkerWritesRequireLiveLease(t *testing.T) {
	operations := map[string]func(*Store, domain.Lease) error{
		"renew": func(s *Store, l domain.Lease) error {
			_, err := s.RenewLease(context.Background(), l, time.Minute)
			return err
		},
		"success": func(s *Store, l domain.Lease) error {
			return s.CommitSuccess(context.Background(), l, domain.StepSuccess{Complete: true})
		},
		"failure": func(s *Store, l domain.Lease) error {
			return s.CommitFailure(context.Background(), l, domain.StepFailure{Class: domain.FailPermanent})
		},
		"dispatch": func(s *Store, l domain.Lease) error {
			_, _, err := s.PrepareEffect(context.Background(), l, "target")
			return err
		},
		"effect result": func(s *Store, l domain.Lease) error {
			return s.RecordEffectResult(context.Background(), l, "target", "result")
		},
	}
	invalid := map[string]func(*domain.Run, *domain.Lease){
		"expired":      func(r *domain.Run, _ *domain.Lease) { past := time.Now().Add(-time.Second); r.LeaseUntil = &past },
		"no deadline":  func(r *domain.Run, _ *domain.Lease) { r.LeaseUntil = nil },
		"wrong owner":  func(_ *domain.Run, l *domain.Lease) { l.WorkerID = "other-worker" },
		"no owner":     func(r *domain.Run, _ *domain.Lease) { r.LeaseOwner = nil },
		"stale fence":  func(_ *domain.Run, l *domain.Lease) { l.FenceToken-- },
		"not running":  func(r *domain.Run, _ *domain.Lease) { r.Status = domain.RunPaused },
		"wrong tenant": func(_ *domain.Run, l *domain.Lease) { l.TenantID = "other-tenant" },
	}
	for state, invalidate := range invalid {
		for name, operation := range operations {
			t.Run(state+"/"+name, func(t *testing.T) {
				s, err := Open(filepath.Join(t.TempDir(), "h.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				r, _, err := s.CreateRun(context.Background(), domain.CreateRunRequest{TenantID: "t", GoalRef: "g", GoalHash: "hash"})
				if err != nil {
					t.Fatal(err)
				}
				l, err := s.ClaimStep(context.Background(), r.ID, "worker", time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				s.mu.Lock()
				r = s.runs[r.ID]
				invalidate(&r, &l)
				s.saveRun(r)
				effectState := "PREPARED"
				if name == "effect result" {
					effectState = "DISPATCHED"
				}
				s.saveEffect(effectRec{Key: l.IdempotencyKey, State: effectState})
				before := s.snapshotStateLocked()
				s.mu.Unlock()
				if err := operation(s, l); !errors.Is(err, domain.ErrLeaseLost) {
					t.Errorf("invalid worker write returned %v, want ErrLeaseLost", err)
				}
				s.mu.Lock()
				after := s.snapshotStateLocked()
				s.mu.Unlock()
				if !reflect.DeepEqual(before, after) {
					t.Error("rejected worker write changed durable state")
				}
			})
		}
	}
}
