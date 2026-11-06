// Package testutil provisions an isolated PostgreSQL schema per test.
// Set HARNESS_TEST_DATABASE_URL (e.g. postgres://harness@127.0.0.1:54329/harness_test);
// tests that need a database are skipped when it is unset.
package testutil

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/store/pg"
	"go-sre-harness/migrations"
)

const Tenant = "tenant-a"

func Pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("HARNESS_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("HARNESS_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano(), rand.IntN(1<<20))

	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func Store(t *testing.T, cfg pg.Config) *pg.Store {
	t.Helper()
	s := pg.New(Pool(t), cfg)
	if err := s.EnsureProfile(context.Background(), Tenant, "sre-diagnosis", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	return s
}

// Eventually polls cond until it is true or the timeout elapses.
func Eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out: "+msg, args...)
}
