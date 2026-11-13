package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/internal/domain"
)

// Triggers, schedules and Works start runs with their own initiator kinds.
// The column only accepts a fixed set, so these must be stored as "system";
// before this was handled, such runs failed on PostgreSQL with a check
// constraint violation.
func TestRunsFromTriggersSchedulesAndWorksPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	tctx := WithTenant(ctx, tenant)
	if _, err := admin.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,'p',3,'{}','h','ACTIVE')`, tenant); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"trigger", "schedule", "work", "user", "service_principal", "system"} {
		run, _, err := app.CreateRun(tctx, domain.CreateRunRequest{TenantID: tenant, ProfileID: "p", ProfileVersion: 3, GoalRef: "g", GoalHash: "h",
			IdempotencyKey: "k-" + kind, InitiatorType: kind, InitiatorID: "who",
			InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: "g", InputHash: "h", Status: domain.StepPending}}})
		if err != nil {
			t.Fatalf("a %q run must be accepted: %v", kind, err)
		}
		shared := kind != "user"
		if domain.SharedInitiator(run.InitiatorType) != shared {
			t.Errorf("%q is stored as %q: shared=%v, want %v", kind, run.InitiatorType, domain.SharedInitiator(run.InitiatorType), shared)
		}
	}
}
