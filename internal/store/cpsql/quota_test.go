package cpsql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/quota"
)

func lease(tn, workID, key string, units, capacity int64) quota.LeaseRequest {
	return quota.LeaseRequest{TenantID: tn, WorkID: workID, Queue: "routine-work", Resource: quota.ResourceModelRun,
		Units: units, DedupKey: key, TTL: time.Minute, Owner: "r1", Capacity: capacity}
}

// The same dedup key never holds units twice, a full budget refuses, and a
// release frees exactly what it held.
func TestQuotaReserveIsIdempotentAndBounded(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "q-"+cpkit.NewID()[24:]
		w := cpkit.NewID()
		a, err := s.Reserve(ctx, lease(tn, w, "k1", 2, 3))
		if err != nil {
			t.Fatal(err)
		}
		again, err := s.Reserve(ctx, lease(tn, w, "k1", 2, 3))
		if err != nil || again.ID != a.ID || !again.Existing {
			t.Fatalf("replay must return the held reservation: %+v %v", again, err)
		}
		var ex *quota.ExhaustedError
		if _, err := s.Reserve(ctx, lease(tn, w, "k2", 2, 3)); !errors.As(err, &ex) || ex.Budget.Reserved != 2 {
			t.Fatalf("want exhausted with 2 reserved, got %v", err)
		}
		if _, err := s.Reserve(ctx, lease(tn, w, "k3", 1, 3)); err != nil {
			t.Fatal(err)
		}
		if err := s.Release(ctx, tn, a.ID, a.Token); err != nil {
			t.Fatal(err)
		}
		// A second (late) release is fenced and frees nothing more.
		if err := s.Release(ctx, tn, a.ID, a.Token); !errors.Is(err, quota.ErrFenced) {
			t.Fatalf("double release: %v", err)
		}
		bs, err := s.Budgets(ctx, tn)
		if err != nil || len(bs) != 1 || bs[0].Reserved != 1 {
			t.Fatalf("budget after release: %+v %v", bs, err)
		}
	})
}

// A stale owner cannot release or renew the units of the owner that took
// the key after it (F02 for reservations).
func TestQuotaReleaseIsFenced(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "q-"+cpkit.NewID()[24:]
		w := cpkit.NewID()
		old, err := s.Reserve(ctx, lease(tn, w, "k", 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		// The reaper is global (other tests share the PostgreSQL database):
		// assert on this tenant's state, not on the count.
		if _, err := s.Reap(ctx, s.now().Add(2*time.Minute), 1000); err != nil {
			t.Fatalf("reap: %v", err)
		}
		if held, _ := s.ActiveReservations(ctx, tn, 10); len(held) != 0 {
			t.Fatalf("expired reservation still held: %+v", held)
		}
		cur, err := s.Reserve(ctx, lease(tn, w, "k", 1, 1))
		if err != nil || cur.Token <= old.Token {
			t.Fatalf("re-reserve must fence the old holder: %+v %v", cur, err)
		}
		if err := s.Release(ctx, tn, old.ID, old.Token); !errors.Is(err, quota.ErrFenced) {
			t.Fatalf("stale release must be fenced: %v", err)
		}
		if err := s.Renew(ctx, tn, old.ID, old.Token, time.Minute); !errors.Is(err, quota.ErrFenced) {
			t.Fatalf("stale renew must be fenced: %v", err)
		}
		if bs, _ := s.Budgets(ctx, tn); bs[0].Reserved != 1 {
			t.Fatalf("the new owner's unit was freed: %+v", bs)
		}
	})
}

// The reaper frees a reservation whose owner died before creating the run,
// and keeps one whose run is really in flight.
func TestQuotaReapReconcilesWithRuns(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "q-"+cpkit.NewID()[24:]
		dead, live := cpkit.NewID(), cpkit.NewID()
		if _, err := s.Reserve(ctx, lease(tn, dead, "dead", 1, 4)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Reserve(ctx, lease(tn, live, "live", 1, 4)); err != nil {
			t.Fatal(err)
		}
		_, err := s.ReapReservations(ctx, s.now().Add(2*time.Minute), 1000, func(_ context.Context, r quota.Reservation) (bool, error) {
			return r.TenantID != tn || r.WorkID == live, nil
		}, 5*time.Minute)
		if err != nil {
			t.Fatalf("reap: %v", err)
		}
		held, _ := s.ActiveReservations(ctx, tn, 10)
		if len(held) != 1 || held[0].WorkID != live || !held[0].ExpiresAt.After(s.now().Add(2*time.Minute)) {
			t.Fatalf("the in-flight reservation must be renewed: %+v", held)
		}
		if bs, _ := s.Budgets(ctx, tn); bs[0].Reserved != 1 {
			t.Fatalf("budget after reap: %+v", bs)
		}
	})
}

// F07: many replicas reserving at once never admit more than the capacity.
func TestQuotaNeverOversubscribes(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "q-"+cpkit.NewID()[24:]
		const capacity, clients = 7, 60
		var admitted atomic.Int64
		var wg sync.WaitGroup
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := s.Reserve(ctx, lease(tn, cpkit.NewID(), cpkit.NewID(), 1, capacity))
				switch {
				case err == nil:
					admitted.Add(1)
				case !errors.Is(err, quota.ErrExhausted):
					t.Errorf("reserve: %v", err)
				}
			}()
		}
		wg.Wait()
		if got := admitted.Load(); got != capacity {
			t.Fatalf("admitted %d of capacity %d", got, capacity)
		}
		if bs, _ := s.Budgets(ctx, tn); bs[0].Reserved != capacity {
			t.Fatalf("budget: %+v", bs)
		}
	})
}

// An oversize request is admitted only while nothing else is reserved.
func TestQuotaOversize(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "q-"+cpkit.NewID()[24:]
		big := lease(tn, cpkit.NewID(), "big", 5, 2)
		if _, err := s.Reserve(ctx, big); !errors.Is(err, quota.ErrExhausted) {
			t.Fatalf("without Oversize a wide request is refused: %v", err)
		}
		big.Oversize = true
		r, err := s.Reserve(ctx, big)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Reserve(ctx, lease(tn, cpkit.NewID(), "small", 1, 2)); !errors.Is(err, quota.ErrExhausted) {
			t.Fatalf("an over-reserved budget admits nothing: %v", err)
		}
		if err := s.Release(ctx, tn, r.ID, r.Token); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Reserve(ctx, lease(tn, cpkit.NewID(), "small", 1, 2)); err != nil {
			t.Fatal(err)
		}
	})
}

// ReleaseWork must drop the lease and set the retry time on both dialects:
// a release that fails leaves the Work leased until the lease expires.
func TestReleaseWorkDropsLease(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), "rel-"+cpkit.NewID()[24:]
		w := mustCreate(t, s, tn)
		now := s.now()
		if _, err := s.db.ExecContext(ctx, s.q(`UPDATE agent_works SET lease_owner=?, lease_until=? WHERE tenant_id=? AND id=?`),
			"o1", s.t(now.Add(time.Minute)), tn, w.ID); err != nil {
			t.Fatal(err)
		}
		retry := now.Add(10 * time.Second).Truncate(time.Microsecond)
		if err := s.ReleaseWork(ctx, tn, w.ID, "o1", retry); err != nil {
			t.Fatalf("release: %v", err)
		}
		got, err := s.GetWork(ctx, tn, w.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.LeaseOwner != "" || got.LeaseUntil != nil || got.WakeAt == nil || !got.WakeAt.Equal(retry) {
			t.Fatalf("lease=%q until=%v wake=%v, want released and wake %v", got.LeaseOwner, got.LeaseUntil, got.WakeAt, retry)
		}
	})
}
