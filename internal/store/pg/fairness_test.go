package pg_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

func TestTenantRunningCapDefersClaims(t *testing.T) {
	cfg := pg.DefaultConfig()
	cfg.MaxRunningPerTenant = 1
	cfg.TenantDeferDelay = 500 * time.Millisecond
	s := testutil.Store(t, cfg)
	a1, a2 := newRun(t, s, "fair-a1"), newRun(t, s, "fair-a2")

	if err := s.EnsureProfile(ctx, "tenant-b", "sre-diagnosis", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]string{"goal": "diagnose service=cart"})
	ref, h, _ := artifact.NewMemory().Put(ctx, "tenant-b", "goal", data)
	b1, _, err := s.CreateRun(ctx, domain.CreateRunRequest{TenantID: "tenant-b", ProfileID: "sre-diagnosis",
		ProfileVersion: 1, GoalRef: ref, GoalHash: h, IdempotencyKey: "fair-b1", InitiatorID: "bob"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.ClaimStep(ctx, a1.ID, "w1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimStep(ctx, a2.ID, "w1", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("second run of a capped tenant must be deferred: %v", err)
	}
	if r := mustRun(t, s, a2.ID); r.Status != domain.RunQueued || r.NotBefore == nil {
		t.Fatalf("deferred run: %+v", r)
	}
	if _, err := s.ClaimStep(ctx, b1.ID, "w1", time.Minute); err != nil {
		t.Fatalf("another tenant must not be blocked: %v", err)
	}
	// Once the deferral passes and a slot frees up, the run is claimable.
	time.Sleep(600 * time.Millisecond)
	if _, err := s.ClaimStep(ctx, a2.ID, "w1", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("still capped while a1 holds its lease: %v", err)
	}
}
