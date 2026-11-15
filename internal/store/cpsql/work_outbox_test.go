package cpsql

import (
	"context"
	"testing"
	"time"
)

func TestWorkOutboxLifecycle(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		tn := tenant()
		w, created, err := s.CreateWork(ctx, newWork(tn, "outbox-create"))
		if err != nil || !created {
			t.Fatalf("create work: created=%v err=%v", created, err)
		}
		if _, again, err := s.CreateWork(ctx, newWork(tn, "outbox-create")); err != nil || again {
			t.Fatalf("idempotent create: again=%v err=%v", again, err)
		}
		msgs, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(msgs) != 1 {
			t.Fatalf("claim create: messages=%v err=%v", msgs, err)
		}
		if msgs[0].Subject != "harness.work.wake" || msgs[0].TenantID != tn || string(msgs[0].Payload) != `{"work_id":"`+w.ID+`"}` {
			t.Fatalf("message=%+v payload=%s", msgs[0], msgs[0].Payload)
		}
		concurrent, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(concurrent) != 0 {
			t.Fatalf("leased message was claimed twice: %v %v", concurrent, err)
		}
		if err := s.MarkPublishFailed(ctx, msgs[0].ID, 30*time.Millisecond); err != nil {
			t.Fatal(err)
		}
		beforeRetry, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(beforeRetry) != 0 {
			t.Fatalf("message ignored backoff: %v %v", beforeRetry, err)
		}
		time.Sleep(40 * time.Millisecond)
		retry, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(retry) != 1 || retry[0].ID != msgs[0].ID {
			t.Fatalf("retry=%v err=%v", retry, err)
		}
		if err := s.MarkPublished(ctx, retry[0].ID); err != nil {
			t.Fatal(err)
		}
		published, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(published) != 0 {
			t.Fatalf("published message returned: %v %v", published, err)
		}
		if err := s.WakeWork(ctx, tn, w.ID, "test", "system", "test", nil); err != nil {
			t.Fatal(err)
		}
		wake, err := s.ClaimOutbox(ctx, 10, time.Second)
		if err != nil || len(wake) != 1 || wake[0].ID == msgs[0].ID {
			t.Fatalf("wake message=%v err=%v", wake, err)
		}
	})
}
