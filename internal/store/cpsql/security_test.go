package cpsql

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
	"go-sre-harness/migrations"
)

func TestSecuritySchema(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		for _, table := range []string{"security_revocation_scope", "security_revocation_request", "security_revocation_event", "security_decision_audit"} {
			var n int
			if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
				t.Fatalf("%s: %v", table, err)
			}
		}
	})
}

func TestSecurityTablesUseRLS(t *testing.T) {
	raw := os.Getenv("HARNESS_PG_TEST_URL")
	if raw == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"security_revocation_scope", "security_revocation_request", "security_revocation_event", "security_decision_audit"} {
		var enabled, forced bool
		err := pool.QueryRow(ctx, `SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE relname=$1`, table).Scan(&enabled, &forced)
		if err != nil || !enabled || !forced {
			t.Fatalf("%s RLS=%v forced=%v err=%v", table, enabled, forced, err)
		}
	}
}

func TestRevocationConcurrentIdempotency(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		const workers = 16
		var wg sync.WaitGroup
		results := make([]revocation.Event, workers)
		errs := make([]error, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i], errs[i] = s.RevokeSecurity(ctx, revocation.Event{TenantID: tn, Scope: "work", ScopeID: "w-1", IdempotencyKey: "revoke-1", ActorID: "admin", Reason: "incident", CreatedAt: t0})
			}(i)
		}
		wg.Wait()
		for i := range results {
			if errs[i] != nil || results[i].Version != 1 || results[i].ID != results[0].ID {
				t.Fatalf("result %d: %+v %v", i, results[i], errs[i])
			}
		}
		got, err := s.SecurityStamps(ctx, revocation.Query{TenantID: tn, Subject: "agent:1", WorkID: "w-1", RunID: "r-1"})
		if err != nil || !hasStamp(got, "work", "w-1", 1) {
			t.Fatalf("stamps: %+v %v", got, err)
		}
	})
}

func TestRevocationConflictAndValidation(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx, tn := context.Background(), tenant()
		original := revocation.Event{TenantID: tn, Scope: "tenant", IdempotencyKey: "revoke-tenant", ActorID: "admin", Reason: "stop", CreatedAt: t0}
		first, err := s.RevokeSecurity(ctx, original)
		if err != nil || first.ScopeID != tn || first.Version != 1 {
			t.Fatalf("tenant revocation: %+v %v", first, err)
		}
		retry := original
		retry.CreatedAt = t0.Add(time.Hour)
		again, err := s.RevokeSecurity(ctx, retry)
		if err != nil || again.ID != first.ID || again.Version != 1 {
			t.Fatalf("retry: %+v %v", again, err)
		}
		conflict := original
		conflict.Reason = "different"
		if _, err := s.RevokeSecurity(ctx, conflict); !errors.Is(err, revocation.ErrRevocationConflict) {
			t.Fatalf("conflict: %v", err)
		}
		bad := []revocation.Event{
			{TenantID: tn, Scope: "unknown", ScopeID: "x", IdempotencyKey: "a", ActorID: "a", Reason: "r"},
			{TenantID: tn, Scope: "work", IdempotencyKey: "a", ActorID: "a", Reason: "r"},
			{TenantID: tn, Scope: "tenant", ScopeID: "other", IdempotencyKey: "a", ActorID: "a", Reason: "r"},
			{TenantID: tn, Scope: "work", ScopeID: strings.Repeat("x", 201), IdempotencyKey: "a", ActorID: "a", Reason: "r"},
		}
		for _, event := range bad {
			if _, err := s.RevokeSecurity(ctx, event); !errors.Is(err, revocation.ErrInvalidRevocation) {
				t.Fatalf("accepted %+v: %v", event, err)
			}
		}
		got, err := s.SecurityStamps(ctx, revocation.Query{TenantID: tn, Subject: "agent:1"})
		if err != nil || len(got) != 1 || got[0].Scope != "tenant" || got[0].Version != 1 {
			t.Fatalf("tenant stamp: %+v %v", got, err)
		}
		if _, err := s.SecurityStamps(ctx, revocation.Query{}); !errors.Is(err, revocation.ErrInvalidRevocation) {
			t.Fatalf("empty query: %v", err)
		}
	})
}

func hasStamp(stamps []security.RevocationStamp, scope, id string, version int64) bool {
	for _, stamp := range stamps {
		if stamp.Scope == scope && stamp.ScopeID == id && stamp.Version == version {
			return true
		}
	}
	return false
}

func TestSecurityDecisionAudit(t *testing.T) {
	forEachStore(t, func(t *testing.T, s *Store) {
		ctx := context.Background()
		request := security.Request{RequestID: "request-1", Operation: security.ArtifactWrite, TenantID: tenant(), Subject: "agent:1", InputHash: "sha256:" + strings.Repeat("a", 64)}
		decision := security.PolicyDecision{Effect: security.Allow, PolicyVersion: "v1", DecisionID: "sha256:" + strings.Repeat("b", 64), DecidedAt: t0, ExpiresAt: t0.Add(time.Minute)}
		if err := s.RecordSecurityDecision(ctx, request, decision); err != nil {
			t.Fatal(err)
		}
		if err := s.RecordSecurityDecision(ctx, request, decision); err != nil {
			t.Fatalf("idempotent audit: %v", err)
		}
		conflict := decision
		conflict.Effect = security.Deny
		if err := s.RecordSecurityDecision(ctx, request, conflict); !errors.Is(err, security.ErrDecisionConflict) {
			t.Fatalf("conflicting audit: %v", err)
		}
		otherRequest := request
		otherRequest.Subject = "agent:2"
		if err := s.RecordSecurityDecision(ctx, otherRequest, decision); !errors.Is(err, security.ErrDecisionConflict) {
			t.Fatalf("conflicting request audit: %v", err)
		}
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM security_decision_audit WHERE tenant_id=?`, request.TenantID).Scan(&count); err != nil || count != 1 {
			t.Fatalf("audit count=%d err=%v", count, err)
		}
	})
}
