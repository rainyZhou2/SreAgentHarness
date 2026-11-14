package pg

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"go-sre-harness/internal/domain"
)

func TestWorkRunQuotaRetryPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "work-quota-" + uuid.NewString()
	if err := admin.EnsureProfile(ctx, tenant, "p", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(ctx, `INSERT INTO tenant_quotas (tenant_id,max_active_runs,max_queued_runs) VALUES ($1,10,1)`, tenant); err != nil {
		t.Fatal(err)
	}
	req := domain.CreateRunRequest{TenantID: tenant, ProfileID: "p", ProfileVersion: 1, IdempotencyKey: "k", GoalRef: "g", PendingActivation: true, InitiatorID: "work:w", WorkOwner: &domain.WorkRunOwner{WorkID: "w", Generation: 1, StageID: "s"}}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	ids := make(chan string, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, _, err := admin.CreateRun(ctx, req); errs <- err; ids <- r.ID }()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent create: %v", err)
		}
	}
	id := ""
	for got := range ids {
		if id != "" && id != got {
			t.Fatal("duplicate reservation")
		}
		id = got
	}
	if _, created, err := admin.CreateRun(ctx, req); err != nil || created {
		t.Fatalf("retry at quota: created=%v err=%v", created, err)
	}
	req.IdempotencyKey = "other"
	if _, _, err := admin.CreateRun(ctx, req); !errors.Is(err, domain.ErrQuotaExceeded) {
		t.Fatalf("new run quota: %v", err)
	}
}

func TestWorkRunActivationPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "work-" + uuid.NewString()
	tctx := WithTenant(ctx, tenant)
	if err := admin.EnsureProfile(ctx, tenant, "p", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	owner := domain.WorkRunOwner{WorkID: "w", Generation: 1, StageID: "s"}
	req := domain.CreateRunRequest{TenantID: tenant, ProfileID: "p", ProfileVersion: 1, IdempotencyKey: "k", GoalRef: "g", PendingActivation: true, InitiatorID: "work:w", WorkOwner: &owner}
	r, _, err := app.CreateRun(tctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if r.WorkOwner == nil || *r.WorkOwner != owner {
		t.Fatal("owner lost")
	}
	if _, err := admin.ClaimStep(ctx, r.ID, "worker", time.Minute); !errors.Is(err, domain.ErrNoRunnableStep) {
		t.Fatalf("reserved claim: %v", err)
	}
	if err := app.ActivateWorkRun(tctx, "other", r.ID, owner); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("foreign activation: %v", err)
	}
	changed := owner
	changed.Generation++
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, changed); err == nil {
		t.Fatal("changed owner activated")
	}
	req.WorkOwner = &changed
	if _, _, err := app.CreateRun(tctx, req); err == nil {
		t.Fatal("changed idempotent owner accepted")
	}
	req.WorkOwner = &owner
	if _, err := app.ApplyCommand(tctx, tenant, r.ID, domain.CommandRequest{Type: domain.CmdPause, ActorID: "test", IdempotencyKey: "pause"}); err != nil {
		t.Fatal(err)
	}
	got, _ := app.GetRun(tctx, tenant, r.ID)
	if got.Status != domain.RunPendingActivation {
		t.Fatal("pause escaped barrier")
	}
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, owner); err != nil {
		t.Fatal(err)
	}
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, owner); err != nil {
		t.Fatal(err)
	}
	got, _ = app.GetRun(tctx, tenant, r.ID)
	if got.Status != domain.RunPaused {
		t.Fatal("paused activation lost pause")
	}
	var events, wakes int
	if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1 AND event_type='run.activated'`, r.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE aggregate_id=$1`, r.ID).Scan(&wakes); err != nil {
		t.Fatal(err)
	}
	if events != 1 || wakes != 0 {
		t.Fatalf("paused activation events=%d wakes=%d", events, wakes)
	}
	if _, err := app.ApplyCommand(tctx, tenant, r.ID, domain.CommandRequest{Type: domain.CmdCancel, ActorID: "test", IdempotencyKey: "cancel"}); err != nil {
		t.Fatal(err)
	}
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, owner); err == nil {
		t.Fatal("canceled activated")
	}
	req.IdempotencyKey = "running"
	r, _, err = app.CreateRun(tctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, owner); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ClaimStep(ctx, r.ID, "worker", time.Minute); err != nil {
		t.Fatalf("active claim: %v", err)
	}
	if err := app.ActivateWorkRun(tctx, tenant, r.ID, owner); err != nil {
		t.Fatalf("running idempotent activation: %v", err)
	}
	// Inventory is internal and crosses tenant boundaries; keyset results exclude terminal runs.
	after := ""
	found := false
	for {
		runs, err := admin.ListWorkRuns(ctx, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(runs) == 0 {
			break
		}
		if len(runs) > 1 || runs[0].ID <= after || runs[0].WorkOwner == nil || runs[0].Status.Terminal() {
			t.Fatal("bad inventory page")
		}
		after = runs[0].ID
		found = found || after == r.ID
	}
	if !found {
		t.Fatal("inventory omitted active run")
	}
}

func TestWorkRunActivationRollbackPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "work-rollback-" + uuid.NewString()
	if err := admin.EnsureProfile(ctx, tenant, "p", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	owner := domain.WorkRunOwner{WorkID: "w", Generation: 1, StageID: "s"}
	req := domain.CreateRunRequest{TenantID: tenant, ProfileID: "p", ProfileVersion: 1, IdempotencyKey: "k", GoalRef: "g", PendingActivation: true, InitiatorID: "work:w", WorkOwner: &owner}
	r, _, err := admin.CreateRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// Each possible write failure must roll back status, event and wake together.
	for _, table := range []string{"run_events", "outbox_messages", "agent_runs"} {
		t.Run(table, func(t *testing.T) {
			name := "fail_activation_" + uuid.NewString()[:8]
			if _, err := admin.pool.Exec(ctx, `CREATE FUNCTION `+name+`() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected activation failure'; END $$`); err != nil {
				t.Fatal(err)
			}
			defer admin.pool.Exec(ctx, `DROP FUNCTION `+name+`() CASCADE`)
			if _, err := admin.pool.Exec(ctx, `CREATE TRIGGER `+name+` BEFORE INSERT OR UPDATE ON `+table+` FOR EACH ROW WHEN (NEW.tenant_id='`+tenant+`') EXECUTE FUNCTION `+name+`() `); err != nil {
				t.Fatal(err)
			}
			if err := admin.ActivateWorkRun(ctx, tenant, r.ID, owner); err == nil {
				t.Fatal("activation failure swallowed")
			}
			got, err := admin.GetRun(ctx, tenant, r.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status != r.Status || got.Version != r.Version || got.LastEventSeq != r.LastEventSeq {
				t.Fatal("failed activation changed run")
			}
			var events, wakes int
			if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1 AND event_type='run.activated'`, r.ID).Scan(&events); err != nil {
				t.Fatal(err)
			}
			if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE aggregate_id=$1`, r.ID).Scan(&wakes); err != nil {
				t.Fatal(err)
			}
			if events != 0 || wakes != 0 {
				t.Fatalf("partial activation events=%d wakes=%d", events, wakes)
			}
		})
	}
	if err := admin.ActivateWorkRun(ctx, tenant, r.ID, owner); err != nil {
		t.Fatal(err)
	}
	if err := admin.ActivateWorkRun(ctx, tenant, r.ID, owner); err != nil {
		t.Fatal(err)
	}
	var events, wakes int
	if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM run_events WHERE run_id=$1 AND event_type='run.activated'`, r.ID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := admin.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_messages WHERE aggregate_id=$1`, r.ID).Scan(&wakes); err != nil {
		t.Fatal(err)
	}
	if events != 1 || wakes != 1 {
		t.Fatalf("repeated activation events=%d wakes=%d", events, wakes)
	}
}

func TestWorkRunInventoryCapPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "work-inventory-" + uuid.NewString()
	if err := admin.EnsureProfile(ctx, tenant, "p", 1, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.pool.Exec(ctx, `INSERT INTO agent_runs (id,tenant_id,profile_id,profile_version,status,create_idempotency_key,goal_ref,initiator_type,initiator_id,work_owner)
 SELECT gen_random_uuid(),$1,'p',1,'PENDING_ACTIVATION',n::text,'g','system','work:w','{"work_id":"w","generation":1,"stage_id":"s"}'::jsonb FROM generate_series(1,205) n`, tenant); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, 500} {
		runs, err := admin.ListWorkRuns(ctx, "", limit)
		if err != nil || len(runs) != 200 {
			t.Fatalf("limit %d: %d %v", limit, len(runs), err)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := admin.ListWorkRuns(canceled, "", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("context ignored: %v", err)
	}
}
