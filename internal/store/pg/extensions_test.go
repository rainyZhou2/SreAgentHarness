package pg_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"go-sre-harness/internal/extensions"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/testutil"
)

func TestExtensionsVersionsAndTenantIsolation(t *testing.T) {
	pool := testutil.Pool(t)
	s := pg.New(pool, pg.DefaultConfig())
	name := fmt.Sprint("x", time.Now().UnixNano())
	other := "other-" + name
	rec := extensions.Record{TenantID: tenant, Kind: extensions.KindSkill, Name: name,
		Spec: json.RawMessage(`{"description":"d","instructions":"i"}`), Enabled: true, UpdatedBy: "alice"}
	zero, one := int64(0), int64(1)

	r1, err := s.UpsertExtension(ctx, rec, &zero)
	if err != nil || r1.Version != 1 {
		t.Fatalf("create: %+v %v", r1, err)
	}
	if _, err := s.UpsertExtension(ctx, rec, &zero); !errors.Is(err, extensions.ErrConflict) {
		t.Fatalf("create-only on existing: %v", err)
	}
	if r2, err := s.UpsertExtension(ctx, rec, &one); err != nil || r2.Version != 2 {
		t.Fatalf("cas v1: %+v %v", r2, err)
	}
	if _, err := s.UpsertExtension(ctx, rec, &one); !errors.Is(err, extensions.ErrConflict) {
		t.Fatalf("stale cas: %v", err)
	}
	if r3, err := s.UpsertExtension(ctx, rec, nil); err != nil || r3.Version != 3 {
		t.Fatalf("upsert: %+v %v", r3, err)
	}
	o := rec
	o.TenantID = other
	if _, err := s.UpsertExtension(ctx, o, nil); err != nil {
		t.Fatal(err)
	}

	has := func(list []extensions.Record, tenantID string) bool {
		for _, r := range list {
			if r.Name == name && r.TenantID == tenantID {
				return true
			}
		}
		return false
	}
	// RLS: the test user is a superuser (which bypasses RLS), so check the
	// policy as an ordinary role with app.tenant_id set, as the pool hook
	// does for tenant-scoped requests.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	for _, q := range []string{
		`DO $$ BEGIN CREATE ROLE harness_rls_probe NOLOGIN NOBYPASSRLS; EXCEPTION WHEN duplicate_object THEN NULL; END $$`,
		`DO $$ BEGIN EXECUTE format('GRANT USAGE ON SCHEMA %I TO harness_rls_probe', current_schema()); END $$`,
		`GRANT SELECT ON tenant_extensions TO harness_rls_probe`,
		`SET LOCAL ROLE harness_rls_probe`,
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	visible := func(scope string) (mine, theirs int) {
		if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, scope); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE tenant_id=$1), count(*) FILTER (WHERE tenant_id=$2)
			FROM tenant_extensions WHERE name=$3`, tenant, other, name).Scan(&mine, &theirs); err != nil {
			t.Fatal(err)
		}
		return
	}
	if mine, theirs := visible(tenant); mine != 1 || theirs != 0 {
		t.Fatalf("tenant-scoped: sees %d own and %d other rows", mine, theirs)
	}
	if mine, theirs := visible(""); mine != 1 || theirs != 1 {
		t.Fatalf("unscoped (background) role: sees %d/%d rows, want both tenants", mine, theirs)
	}
	_ = tx.Rollback(ctx)

	// The runtime sync lists every tenant with an unscoped context.
	all, err := s.ListExtensions(ctx, "")
	if err != nil || !has(all, tenant) || !has(all, other) {
		t.Fatalf("unscoped list: %v", err)
	}
	if err := s.DeleteExtension(ctx, other, extensions.KindSkill, name); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteExtension(ctx, other, extensions.KindSkill, name); !errors.Is(err, extensions.ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
}
