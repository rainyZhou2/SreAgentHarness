package revocation_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
)

type fakeStore struct {
	mu     sync.Mutex
	stamps []security.RevocationStamp
	calls  int
}

func (s *fakeStore) Revoke(context.Context, revocation.Event) (revocation.Event, error) {
	return revocation.Event{}, nil
}

func (s *fakeStore) Stamps(context.Context, revocation.Query) ([]security.RevocationStamp, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	return append([]security.RevocationStamp(nil), s.stamps...), nil
}

func (s *fakeStore) setVersion(version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stamps[0].Version = version
}

func (s *fakeStore) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestRevocationCacheInvalidation(t *testing.T) {
	now := time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)
	store := &fakeStore{stamps: []security.RevocationStamp{{Scope: "work", ScopeID: "w", Version: 1}}}
	cache := revocation.NewCache(store, func() time.Time { return now })
	query := revocation.Query{TenantID: "t", WorkID: "w"}
	first, err := cache.Stamps(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	store.setVersion(2)
	second, err := cache.Stamps(context.Background(), query)
	if err != nil || len(first) != 1 || len(second) != 1 || second[0].Version != 1 {
		t.Fatalf("cache snapshot: first=%+v second=%+v err=%v", first, second, err)
	}
	first[0].Version = 99
	cache.Invalidate(query.TenantID, query.WorkID)
	third, err := cache.Stamps(context.Background(), query)
	if err != nil || third[0].Version != 2 || store.callCount() != 2 {
		t.Fatalf("invalidation: %+v calls=%d err=%v", third, store.callCount(), err)
	}
	now = now.Add(5 * time.Second)
	if _, err = cache.Stamps(context.Background(), query); err != nil || store.callCount() != 3 {
		t.Fatalf("TTL: calls=%d err=%v", store.callCount(), err)
	}
}

func TestRevocationCacheScopesAndConcurrentSnapshots(t *testing.T) {
	now := time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)
	store := &fakeStore{}
	cache := revocation.NewCache(store, func() time.Time { return now })
	query := revocation.Query{TenantID: "t", Subject: "agent:1", WorkID: "w", RunID: "r"}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cache.Stamps(context.Background(), query); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	baseline := store.callCount()
	cache.Invalidate("other", "w")
	cache.Invalidate("t", "other")
	if _, err := cache.Stamps(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if got := store.callCount(); got != baseline {
		t.Fatalf("unrelated invalidations must retain cache: calls=%d", got)
	}
	cache.Invalidate("t", "t")
	if _, err := cache.Stamps(context.Background(), query); err != nil {
		t.Fatal(err)
	}
	if got := store.callCount(); got != baseline+1 {
		t.Fatalf("tenant invalidation must clear every scope: calls=%d", got)
	}
}
