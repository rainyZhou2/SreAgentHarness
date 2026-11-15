package cpsql

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/work"
)

func publishPendingWorkMessages(t *testing.T, s *Store) []domain.OutboxMessage {
	t.Helper()
	var published []domain.OutboxMessage
	for {
		msgs, err := s.ClaimOutbox(context.Background(), 100, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 0 {
			return published
		}
		for _, msg := range msgs {
			if err := s.MarkPublished(context.Background(), msg.ID); err != nil {
				t.Fatal(err)
			}
			published = append(published, msg)
		}
	}
}

func TestReleaseWorkPublishesImmediateRetry(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		w := mustCreate(t, s, tn)
		publishPendingWorkMessages(t, s)
		now := s.now()
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET lease_owner=?, lease_until=? WHERE tenant_id=? AND id=?`),
			"owner", s.t(now.Add(time.Minute)), tn, w.ID); err != nil {
			t.Fatal(err)
		}
		if err := s.ReleaseWork(ctx, tn, w.ID, "owner", now); err != nil {
			t.Fatal(err)
		}
		msgs := publishPendingWorkMessages(t, s)
		if len(msgs) != 1 || workMessageID(t, msgs[0].Payload) != w.ID {
			t.Fatalf("messages=%v", msgs)
		}
	})
}

func TestWakeDeferredPublishesEachWork(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		first, second := mustCreate(t, s, tn), mustCreate(t, s, tn)
		publishPendingWorkMessages(t, s)
		later := s.now().Add(time.Hour)
		for _, w := range []work.Work{first, second} {
			if _, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET status='WAITING', reason='queued: test', wake_at=? WHERE tenant_id=? AND id=?`),
				s.t(later), tn, w.ID); err != nil {
				t.Fatal(err)
			}
		}
		n, err := s.WakeDeferred(ctx, tn, s.now(), 2)
		if err != nil || n != 2 {
			t.Fatalf("woken=%d err=%v", n, err)
		}
		msgs := publishPendingWorkMessages(t, s)
		if len(msgs) != 2 {
			t.Fatalf("messages=%v", msgs)
		}
		seen := map[string]bool{}
		for _, msg := range msgs {
			seen[workMessageID(t, msg.Payload)] = true
		}
		for _, w := range []work.Work{first, second} {
			if !seen[w.ID] {
				t.Errorf("missing wake for %s: %v", w.ID, seen)
			}
		}
	})
}

func TestCompleteChildPublishesEligibleParentWake(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		parent := mustCreate(t, s, tn)
		child, created, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, nil)})
		if err != nil || !created {
			t.Fatalf("create child: created=%v err=%v", created, err)
		}
		publishPendingWorkMessages(t, s)
		cur, err := s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: child.ID, ExpectedVersion: child.Version,
			ActorType: "system", Status: st(work.StatusActive), Now: s.now()})
		if err != nil {
			t.Fatal(err)
		}
		cur, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: child.ID, ExpectedVersion: cur.Version,
			ActorType: "system", Status: st(work.StatusFailed), Now: s.now()})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteChild(ctx, tn, child.ID, cur.Version); err != nil {
			t.Fatal(err)
		}
		msgs := publishPendingWorkMessages(t, s)
		found := false
		for _, msg := range msgs {
			if workMessageID(t, msg.Payload) == parent.ID {
				found = true
			}
		}
		if !found {
			t.Fatalf("parent wake missing: %v", msgs)
		}

		blockedChild, created, err := s.CreateChild(ctx, work.NewChild{ParentID: parent.ID, Work: childOf(tn, nil)})
		if err != nil || !created {
			t.Fatalf("create blocked child: created=%v err=%v", created, err)
		}
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET status='BLOCKED', wake_at=NULL WHERE tenant_id=? AND id=?`), tn, parent.ID); err != nil {
			t.Fatal(err)
		}
		publishPendingWorkMessages(t, s)
		blockedChild, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: blockedChild.ID, ExpectedVersion: blockedChild.Version,
			ActorType: "system", Status: st(work.StatusActive), Now: s.now()})
		if err != nil {
			t.Fatal(err)
		}
		blockedChild, err = s.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: blockedChild.ID, ExpectedVersion: blockedChild.Version,
			ActorType: "system", Status: st(work.StatusFailed), Now: s.now()})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.CompleteChild(ctx, tn, blockedChild.ID, blockedChild.Version); err != nil {
			t.Fatal(err)
		}
		for _, msg := range publishPendingWorkMessages(t, s) {
			if workMessageID(t, msg.Payload) == parent.ID {
				t.Fatalf("blocked parent was woken: %v", msg)
			}
		}
	})
}
