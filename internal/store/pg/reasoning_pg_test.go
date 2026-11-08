package pg

// Integration test against a real Postgres. Set HARNESS_PG_TEST_URL to a
// superuser URL of an empty database; it is skipped otherwise.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/reasoning"
	"go-sre-harness/migrations"
)

func pgTestStores(t *testing.T) (admin *Store, app *Store, url string) {
	url = os.Getenv("HARNESS_PG_TEST_URL")
	if url == "" {
		t.Skip("HARNESS_PG_TEST_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatalf("migrations: %v", err)
	}
	// A non-superuser role, so row level security applies.
	for _, q := range []string{
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='harness_app') THEN
			CREATE ROLE harness_app LOGIN NOSUPERUSER NOBYPASSRLS; END IF; END $$`,
		`GRANT ALL ON ALL TABLES IN SCHEMA public TO harness_app`,
		`GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO harness_app`,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = "harness_app"
	ConfigureRLS(cfg)
	appPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(appPool.Close)
	return New(pool, Config{}), New(appPool, Config{}), url
}

func seedRun(t *testing.T, s *Store, tenant string) (runID string, stepID string) {
	ctx := context.Background()
	runID, stepID = uuid.NewString(), uuid.NewString()
	if _, err := s.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,'p',3,'{}','h','ACTIVE') ON CONFLICT DO NOTHING`, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status,
		create_idempotency_key, goal_ref, initiator_type) VALUES ($1,$2,'p',3,'COMPLETED',$3,'g','user')`, runID, tenant, runID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO agent_steps (id, tenant_id, run_id, seq, kind, executor_name, status,
		input_ref, input_hash) VALUES ($1,$2,$3,1,'tool','x','SUCCEEDED','r','h')`, stepID, tenant, runID); err != nil {
		t.Fatal(err)
	}
	return runID, stepID
}

func TestReasoningScoresPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	runID, stepID := seedRun(t, admin, tenant)
	m, _, err := admin.CreateMemory(ctx, memory.NewMemory{TenantID: tenant, Scope: "service", ScopeID: "api", Type: "procedural",
		Status: "ACTIVE", Subject: "api:" + reasoning.PatternSubject, Title: "p", Content: "c", ContentHash: uuid.NewString(),
		Trust: "T3", Sensitivity: "internal", CreatedBy: "test"})
	if err != nil {
		t.Fatalf("create memory: %v", err)
	}
	for _, sid := range []string{stepID, stepID} { // duplicate hit rows are collapsed by DISTINCT
		_, _ = admin.pool.Exec(ctx, `INSERT INTO step_memory_hits (tenant_id, step_id, memory_id, memory_version, score, rank)
			VALUES ($1,$2,$3,1,0.5,1) ON CONFLICT DO NOTHING`, tenant, sid, m.ID)
	}
	ids, err := app.RunMemoryIDs(WithTenant(ctx, tenant), tenant, runID)
	if err != nil || len(ids) != 1 || ids[0] != m.ID {
		t.Fatalf("run memory ids: %v %v", ids, err)
	}

	b := 0.2
	sc := reasoning.RunScore{TenantID: tenant, RunID: runID, ProfileID: "p", ProfileVersion: 3, Service: "api", Outcome: "COMPLETED",
		Metrics: reasoning.Metrics{Quality: 0.7, Brier: &b, Conclusion: "VERIFIED", Completed: true}, MemoryIDs: ids}
	tctx := WithTenant(ctx, tenant)
	if err := app.PutReasoningScore(tctx, sc); err != nil {
		t.Fatalf("put: %v", err)
	}
	sc.Metrics.Quality = 0.9
	if err := app.PutReasoningScore(tctx, sc); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	sc.Metrics.CitedMemories, sc.Metrics.CalibrationCoverage, sc.Metrics.Structured = []string{m.ID}, 0.5, 1
	if err := app.PutReasoningScore(tctx, sc); err != nil {
		t.Fatalf("upsert with citations: %v", err)
	}
	if got, _ := app.ReasoningScore(tctx, tenant, runID); len(got.Metrics.CitedMemories) != 1 || got.Metrics.CalibrationCoverage != 0.5 {
		t.Fatalf("metric fields round trip: %+v", got.Metrics)
	}
	// Hypothesis ids survive as thought attributes and drive dedup.
	tx, err := admin.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	th := []domain.Thought{{Kind: domain.ThoughtHypothesis, Summary: "x", Status: domain.ThoughtVerified,
		Attrs: map[string]any{reasoning.HypothesisIDAttr: "h1"}}}
	if err := insertThoughts(ctx, tx, tenant, runID, stepID, th); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	trace, _, err := app.RunTrace(tctx, tenant, runID)
	if err != nil || len(trace[stepID]) != 1 || trace[stepID][0].Attrs[reasoning.HypothesisIDAttr] != "h1" {
		t.Fatalf("thought attrs: %+v %v", trace, err)
	}
	got, err := app.ReasoningScore(tctx, tenant, runID)
	if err != nil || got.Metrics.Quality != 0.9 || *got.Metrics.Brier != 0.2 || len(got.MemoryIDs) != 1 || got.ScoredAt.IsZero() {
		t.Fatalf("get: %+v %v", got, err)
	}
	for _, f := range []reasoning.ScoreFilter{{}, {ProfileID: "p", Version: 3, Service: "api", Since: time.Now().Add(-time.Hour)}} {
		list, err := app.ReasoningScores(tctx, tenant, f)
		if err != nil || len(list) != 1 {
			t.Fatalf("list %+v: %v %v", f, list, err)
		}
	}
	if list, _ := app.ReasoningScores(tctx, tenant, reasoning.ScoreFilter{Version: 4}); len(list) != 0 {
		t.Fatal("version filter")
	}
	if _, err := app.ReasoningScore(tctx, tenant, uuid.NewString()); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("missing score: %v", err)
	}
	// Row level security: another tenant's context sees nothing, even when
	// the query names the tenant.
	other := WithTenant(ctx, "other-"+tenant)
	if list, err := app.ReasoningScores(other, tenant, reasoning.ScoreFilter{}); err != nil || len(list) != 0 {
		t.Fatalf("rls leak: %v %v", list, err)
	}
	if err := app.PutReasoningScore(other, sc); err == nil {
		t.Fatal("rls must reject writes for another tenant")
	}
}

func TestRolloutReasoningAndRetentionPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	tctx := WithTenant(ctx, tenant)
	since := time.Now().Add(-time.Minute)
	ctrlRun, _ := seedRun(t, admin, tenant)
	canRun, _ := seedRun(t, admin, tenant)
	rollout := uuid.NewString()
	if _, err := admin.pool.Exec(ctx, `INSERT INTO evolution_rollouts (id, tenant_id, profile_id, from_version, to_version,
		status, guardrails, started_at) VALUES ($1,$2,'p',3,3,'CANARY','{}',now())`, rollout, tenant); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(ctx, `UPDATE agent_runs SET rollout_id=$1 WHERE id=$2`, rollout, canRun); err != nil {
		t.Fatal(err)
	}
	for run, q := range map[string]float64{ctrlRun: 0.9, canRun: 0.4} {
		if err := app.PutReasoningScore(tctx, reasoning.RunScore{TenantID: tenant, RunID: run, ProfileID: "p", ProfileVersion: 3,
			Outcome: "COMPLETED", Metrics: reasoning.Metrics{Quality: q, Completed: true}}); err != nil {
			t.Fatal(err)
		}
	}
	can, err := app.RolloutStats(tctx, tenant, rollout, false, "p", since)
	if err != nil || can.Reasoning == nil || can.Reasoning.N != 1 || can.Reasoning.MeanQuality != 0.4 || can.Finished != 1 {
		t.Fatalf("canary: %+v %v", can, err)
	}
	ctl, err := app.RolloutStats(tctx, tenant, rollout, true, "p", since)
	if err != nil || ctl.Reasoning == nil || ctl.Reasoning.N != 1 || ctl.Reasoning.MeanQuality != 0.9 {
		t.Fatalf("control: %+v %v", ctl, err)
	}
	if empty, err := app.RolloutStats(tctx, tenant, rollout, false, "p", time.Now().Add(time.Hour)); err != nil || empty.Reasoning != nil {
		t.Fatalf("no scored runs must give a nil summary: %+v %v", empty, err)
	}

	if _, err := admin.pool.Exec(ctx, `UPDATE reasoning_scores SET scored_at = now() - interval '100 days'
		WHERE tenant_id=$1 AND run_id=$2`, tenant, ctrlRun); err != nil {
		t.Fatal(err)
	}
	n, err := admin.DeleteOldReasoningScores(ctx, time.Now().Add(-90*24*time.Hour), 1000)
	if err != nil || n < 1 {
		t.Fatalf("retention: %d %v", n, err)
	}
	if _, err := app.ReasoningScore(tctx, tenant, ctrlRun); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("old score must be deleted")
	}
	if _, err := app.ReasoningScore(tctx, tenant, canRun); err != nil {
		t.Fatalf("recent score must stay: %v", err)
	}
	_ = fmt.Sprint()
}
