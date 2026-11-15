package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/security/revocation"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/migrations"
)

type securityReplica struct {
	*security.Service
	Revocations *revocation.Service
}

type integrationDecisionRecorder struct{}

func (integrationDecisionRecorder) RecordDecision(context.Context, security.Request, security.PolicyDecision) error {
	return nil
}

type integrationEvaluator struct{}

func (integrationEvaluator) Evaluate(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	return security.PolicyDecision{Effect: security.Allow, PolicyVersion: "integration-v1"}, nil
}

func newSecurityReplica(t *testing.T, name string, store revocation.Store) *securityReplica {
	t.Helper()
	cache := revocation.NewCache(store, time.Now)
	reader := revocation.RequestReader{Reader: cache}
	fresh := revocation.RequestReader{Reader: store}
	return &securityReplica{
		Service: &security.Service{Mode: security.ModeEnforce, Evaluator: integrationEvaluator{}, Revocations: reader,
			FreshRevocations: fresh, Recorder: integrationDecisionRecorder{}},
		Revocations: &revocation.Service{Store: store, Cache: cache},
	}
}

func newPostgresSecurityStore(t *testing.T) *cpsql.Store {
	t.Helper()
	raw := os.Getenv("HARNESS_PG_TEST_URL")
	if raw == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := cpsql.OpenPostgres(pool)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func assertSecurityCode(t *testing.T, err error, code security.ErrorCode) {
	t.Helper()
	var securityErr *security.Error
	if !errors.As(err, &securityErr) || securityErr.Code != code {
		t.Fatalf("got %v, want security code %s", err, code)
	}
}

func TestTwoReplicasObserveRevocation(t *testing.T) {
	if os.Getenv("HARNESS_PG_TEST_URL") == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	a := newSecurityReplica(t, "a", newPostgresSecurityStore(t))
	b := newSecurityReplica(t, "b", newPostgresSecurityStore(t))
	req := security.Request{Operation: security.ArtifactWrite, TenantID: "t-replica-" + strings.ReplaceAll(t.Name(), "/", "-"),
		Subject: "work:w", WorkID: "w", RunID: "r", InputHash: "sha256:" + strings.Repeat("a", 64)}
	decision, err := a.Authorize(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Revocations.Revoke(context.Background(), revocation.Event{TenantID: req.TenantID, Scope: "work", ScopeID: "w",
		IdempotencyKey: "stop-w", ActorID: "admin", Reason: "incident", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	assertSecurityCode(t, a.Recheck(context.Background(), req, decision), security.Revoked)
}

func TestDatabaseUnavailableFailsSideEffectsClosed(t *testing.T) {
	store, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	replica := newSecurityReplica(t, "a", store)
	_, err = replica.Authorize(context.Background(), security.Request{Operation: security.ArtifactWrite, TenantID: "t", Subject: "s",
		InputHash: "sha256:" + strings.Repeat("a", 64)})
	assertSecurityCode(t, err, security.PolicyUnavailable)
}
