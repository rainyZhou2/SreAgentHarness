package cpsql

import (
	"context"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/work"
	"go-sre-harness/migrations"
)

// T18 at the database layer: on the server's pool (pg.ConfigureRLS) a
// request scoped with pg.WithTenant cannot read or write another tenant's
// control-plane rows even if a query names that tenant explicitly. RLS only
// binds roles without BYPASSRLS, so the test connects as an ordinary role.
func TestRowLevelSecurity(t *testing.T) {
	raw := os.Getenv("HARNESS_PG_TEST_URL")
	if raw == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, raw)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := migrations.Apply(ctx, admin); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='harness_rls_test') THEN
			CREATE ROLE harness_rls_test LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD 'rls-test'; END IF; END $$`,
		`GRANT USAGE ON SCHEMA public TO harness_rls_test`,
		`GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO harness_rls_test`,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO harness_rls_test`,
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword("harness_rls_test", "rls-test")
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		t.Fatal(err)
	}
	pg.ConfigureRLS(cfg)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := OpenPostgres(pool)
	defer s.Close()
	s.Clock = func() time.Time { return t0 }

	ta, tb := tenant(), tenant()
	ctxA, ctxB := pg.WithTenant(ctx, ta), pg.WithTenant(ctx, tb)
	w, created, err := s.CreateWork(ctxA, newWork(ta, cpkit.NewID()))
	if err != nil || !created {
		t.Fatalf("create in own tenant: %v", err)
	}
	if _, err := s.GetWork(ctxA, ta, w.ID); err != nil {
		t.Fatalf("own read: %v", err)
	}
	// Tenant B names tenant A explicitly: RLS still hides the row.
	if _, err := s.GetWork(ctxB, ta, w.ID); !errors.Is(err, work.ErrNotFound) {
		t.Fatalf("cross-tenant read: %v", err)
	}
	if ws, err := s.ListWorks(ctxB, work.Filter{TenantID: ta}); err != nil || len(ws) != 0 {
		t.Fatalf("cross-tenant list: %d %v", len(ws), err)
	}
	if evs, err := s.ListWorkEvents(ctxB, ta, w.ID, 0, 10); err != nil || len(evs) != 0 {
		t.Fatalf("cross-tenant timeline: %d %v", len(evs), err)
	}
	if gs, err := s.ListGoals(ctxB, ta, w.ID); err != nil || len(gs) != 0 {
		t.Fatalf("cross-tenant goals: %d %v", len(gs), err)
	}
	var n int
	if err := s.db.QueryRowContext(ctxB, `SELECT count(*) FROM work_assignments WHERE tenant_id=$1`, ta).Scan(&n); err != nil || n != 0 {
		t.Fatalf("assignments view leaks across tenants: %d %v", n, err)
	}
	// The run-tenant trigger works under RLS for an ordinary role.
	run := cpkit.NewID()
	if _, err := s.ApplyWork(ctxA, work.Mutation{TenantID: ta, WorkID: w.ID, ActorType: "system", Now: t0,
		Links: []work.RunLink{{RunID: run, Role: work.RoleOwner, Phase: "investigate", StageID: "investigate"}}}); err != nil {
		t.Fatalf("own run link under RLS: %v", err)
	}
	if err := s.db.QueryRowContext(ctxA, `SELECT count(*) FROM work_assignments WHERE tenant_id=$1`, ta).Scan(&n); err != nil || n != 1 {
		t.Fatalf("own assignments: %d %v", n, err)
	}
	// Writing a row for tenant A from tenant B's scope violates the policy.
	if _, _, err := s.CreateWork(ctxB, newWork(ta, cpkit.NewID())); err == nil {
		t.Fatal("cross-tenant insert must be refused")
	}
	// Background workers (no tenant scope) see every tenant.
	if _, err := s.GetWork(ctx, ta, w.ID); err != nil {
		t.Fatalf("unscoped read: %v", err)
	}
}
