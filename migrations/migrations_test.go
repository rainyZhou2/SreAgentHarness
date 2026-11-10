package migrations_test

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/work"
	"go-sre-harness/migrations"
)

var updateChecksums = flag.Bool("update-checksums", false, "record checksums of new migrations in checksums.sha256")

// Applied migrations are immutable: a deployed database never re-runs a
// changed file, so editing one silently forks the schema. New migrations
// are appended and recorded with -update-checksums.
func TestMigrationsAreAppendOnly(t *testing.T) {
	recorded := map[string]string{}
	var order []string
	f, err := os.Open("checksums.sha256")
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		sum, name, ok := strings.Cut(strings.TrimSpace(sc.Text()), "  ")
		if !ok {
			continue
		}
		recorded[name] = sum
		order = append(order, name)
	}
	f.Close()
	var added []string
	for _, v := range migrations.Versions() {
		name := v + ".sql"
		body, err := migrations.Source(v)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.Sum256(body)
		sum := hex.EncodeToString(h[:])
		want, ok := recorded[name]
		switch {
		case ok && want != sum:
			t.Errorf("%s was changed after it was recorded; add a new migration instead", name)
		case !ok:
			if len(order) > 0 && name < order[len(order)-1] {
				t.Errorf("%s sorts before the last recorded migration %s; migrations are append-only", name, order[len(order)-1])
			}
			added = append(added, fmt.Sprintf("%s  %s", sum, name))
		}
	}
	if len(added) == 0 {
		return
	}
	if !*updateChecksums {
		t.Fatalf("new migrations are not recorded: run go test ./migrations -run AppendOnly -update-checksums\n%s", strings.Join(added, "\n"))
	}
	out, err := os.OpenFile("checksums.sha256", os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	for _, l := range added {
		fmt.Fprintln(out, l)
	}
}

// freshDB creates an empty database next to HARNESS_PG_TEST_URL.
func freshDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("HARNESS_PG_TEST_URL")
	if url == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "compat_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name+" TEMPLATE template0 ENCODING 'UTF8'"); err != nil {
		t.Skipf("cannot create a scratch database: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	return pool
}

// An old database upgrades to the latest schema and its records stay
// readable by today's code (RFC §24.3 migration gate): a v1 run (seeded at
// the very first schema), a memory from before applicability, and a v2 Work
// from before goals, assignments and queues.
func TestUpgradeKeepsOldRecordsReadable(t *testing.T) {
	pool := freshDB(t)
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%v\n%s", err, sql)
		}
	}
	const tn = "compat"
	runID, memID, workID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	t0 := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

	// v1.0 schema: a profile and a completed run.
	if err := migrations.ApplyUntil(ctx, pool, "0001_init"); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1, 'sre-diagnosis', 1, '{"max_steps": 10}', 'sha256:x', 'ACTIVE')`, tn)
	exec(`INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status, create_idempotency_key, goal_ref,
		result_ref, initiator_type, initiator_id, created_at, updated_at)
		VALUES ($1, $2, 'sre-diagnosis', 1, 'COMPLETED', 'k1', 'goal://checkout 5xx', 'res://1', 'user', 'alice', $3, $3)`,
		runID, tn, t0)

	// Last v1 schema: a memory without applicability or outcome counters.
	if err := migrations.ApplyUntil(ctx, pool, "0019_memory_tiers"); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO agent_memories (id, tenant_id, scope, scope_id, memory_type, status, subject, title, content_text,
		content_ref, content_hash, created_by) VALUES ($1, $2, 'organization', '', 'procedural', 'ACTIVE', 'checkout:rollback',
		'Roll back checkout', 'Roll back to the previous image', 'mem://x', 'sha256:m', 'alice')`, memID, tn)

	// Early v2 schema: an incident Work from before goals and queues.
	if err := migrations.ApplyUntil(ctx, pool, "0024_work_evidence"); err != nil {
		t.Fatal(err)
	}
	var tpl work.Template
	for _, x := range work.Builtin() {
		if x.ID == "incident" {
			tpl = x
		}
	}
	if err := tpl.Normalize(); err != nil {
		t.Fatal(err)
	}
	exec(`INSERT INTO agent_works (id, tenant_id, kind, status, title, goal, goal_hash, template, template_id, template_version,
		priority, version, last_event_seq, wake_at, create_idempotency_key, create_request_hash, created_at, updated_at)
		VALUES ($1, $2, 'incident', 'ACTIVE', 'checkout 5xx', $3, 'sha256:g', $4, 'incident', 1, 10, 1, 0, $5, 'w1', 'sha256:r', $5, $5)`,
		workID, tn, `{"intent":"mitigate","subject_refs":["service://prod/checkout"]}`, mustJSON(t, tpl), t0)

	// Upgrade to the latest schema, twice (idempotent).
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatalf("re-apply: %v", err)
	}

	store := pg.New(pool, pg.Config{})
	run, err := store.GetRun(ctx, tn, runID)
	if err != nil || string(run.Status) != "COMPLETED" || run.ProfileID != "sre-diagnosis" {
		t.Fatalf("v1 run after upgrade: %v %+v", err, run)
	}
	m, err := store.GetMemory(ctx, tn, memID)
	if err != nil || m.Title != "Roll back checkout" || m.Applicability != nil || m.Verification.SuccessCount != 0 {
		t.Fatalf("v1 memory after upgrade: %v %+v", err, m)
	}
	cp := cpsql.OpenPostgres(pool)
	w, err := cp.GetWork(ctx, tn, workID)
	if err != nil {
		t.Fatalf("v2 Work after upgrade: %v", err)
	}
	if w.Queue != work.QueueIncident || w.Title != "checkout 5xx" || w.Priority != 10 || len(w.Template.Stages) == 0 {
		t.Fatalf("v2 Work after upgrade: %+v", w)
	}
	// The upgraded Work keeps working with today's code.
	if _, err := cp.ApplyWork(ctx, work.Mutation{TenantID: tn, WorkID: workID, ActorType: "user", ActorID: "op",
		ExpectedVersion: w.Version, Now: time.Now(), Reason: ptr("checked after upgrade")}); err != nil {
		t.Fatalf("mutate upgraded Work: %v", err)
	}
}

func ptr[T any](v T) *T { return &v }

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
