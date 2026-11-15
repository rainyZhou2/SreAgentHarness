package retention

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type artifactLeaseProbe struct {
	mu             sync.Mutex
	claims         []ArtifactClaim
	confirmed      int
	retried        int
	retryAt        time.Time
	failConfirm    bool
	claimCalls     int
	lastLease      time.Duration
	lastClaimBatch int
}

func (s *artifactLeaseProbe) ClaimArtifacts(_ context.Context, _ time.Time, batch int, lease time.Duration) ([]ArtifactClaim, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimCalls++
	s.lastLease = lease
	s.lastClaimBatch = batch
	claims := append([]ArtifactClaim(nil), s.claims...)
	s.claims = nil
	return claims, nil
}

func (s *artifactLeaseProbe) ConfirmArtifactDeleted(_ context.Context, _ ArtifactClaim) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failConfirm {
		return errors.New("confirmation unavailable")
	}
	s.confirmed++
	return nil
}

func (s *artifactLeaseProbe) RetryArtifact(_ context.Context, _ ArtifactClaim, at time.Time, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retried++
	s.retryAt = at
	return nil
}

type artifactDeleteProbe struct {
	mu      sync.Mutex
	deletes int
	err     error
}

func (p *artifactDeleteProbe) Delete(context.Context, string, string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes++
	return p.err
}

func (p *artifactDeleteProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.deletes
}

func TestArtifactCleanerRecoversAfterObjectDelete(t *testing.T) {
	store := &artifactLeaseProbe{claims: []ArtifactClaim{{TenantID: "t", Ref: "mem://t/kind/hash", LeaseToken: 1}}, failConfirm: true}
	objects := &artifactDeleteProbe{}
	cleaner := ArtifactCleaner{Store: store, Objects: objects}
	if err := cleaner.Tick(context.Background()); err == nil {
		t.Fatal("confirmation failure must be returned")
	}
	if objects.count() != 1 || store.confirmed != 0 {
		t.Fatal("object delete must precede database confirmation")
	}
	store.claims = []ArtifactClaim{{TenantID: "t", Ref: "mem://t/kind/hash", LeaseToken: 2}}
	store.failConfirm = false
	if err := cleaner.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if objects.count() != 2 || store.confirmed != 1 {
		t.Fatal("retry must idempotently delete and confirm exactly once")
	}
}

func TestArtifactClaimCompetition(t *testing.T) {
	store := &artifactLeaseProbe{claims: []ArtifactClaim{{TenantID: "t", Ref: "mem://t/kind/hash", LeaseToken: 1}}}
	objects := &artifactDeleteProbe{}
	first := ArtifactCleaner{Store: store, Objects: objects}
	second := ArtifactCleaner{Store: store, Objects: objects}
	var wg sync.WaitGroup
	wg.Add(2)
	for _, cleaner := range []*ArtifactCleaner{&first, &second} {
		go func() {
			defer wg.Done()
			if err := cleaner.Tick(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if objects.count() != 1 || store.confirmed != 1 {
		t.Fatalf("only the lease holder may delete: deletes=%d confirmations=%d", objects.count(), store.confirmed)
	}
}

func TestArtifactCleanerBackoffAfterDeleteFailure(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	store := &artifactLeaseProbe{claims: []ArtifactClaim{{TenantID: "t", Ref: "mem://t/kind/hash", LeaseToken: 7}}}
	objects := &artifactDeleteProbe{err: errors.New("object store unavailable")}
	cleaner := ArtifactCleaner{Store: store, Objects: objects, Backoff: 2 * time.Minute, Batch: 9, Lease: 3 * time.Minute,
		Now: func() time.Time { return now }}
	if err := cleaner.Tick(context.Background()); err == nil {
		t.Fatal("object deletion failure must be returned")
	}
	if store.confirmed != 0 || store.retried != 1 || !store.retryAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("failed deletion must be retried with backoff: %+v", store)
	}
	if store.lastClaimBatch != 9 || store.lastLease != 3*time.Minute {
		t.Fatal("cleaner must forward bounded claim configuration")
	}
}

func TestArtifactCleanerDoesNotDeleteUnclaimableArtifact(t *testing.T) {
	store := &artifactLeaseProbe{}
	objects := &artifactDeleteProbe{}
	cleaner := ArtifactCleaner{Store: store, Objects: objects}
	if err := cleaner.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if objects.count() != 0 || store.confirmed != 0 {
		t.Fatal("an artifact blocked at claim-time must not reach object deletion")
	}
}
