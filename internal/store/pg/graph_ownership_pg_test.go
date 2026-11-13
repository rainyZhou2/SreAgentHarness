package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// Run, step and thought nodes of the provenance graph say who started their
// run, so the API can hide other users' work.
func TestGraphNodesCarryTheirRunsOwnerPostgres(t *testing.T) {
	admin, app, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-" + uuid.NewString()[:8]
	tctx := WithTenant(ctx, tenant)
	if _, err := admin.pool.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
		VALUES ($1,'p',3,'{}','h','ACTIVE')`, tenant); err != nil {
		t.Fatal(err)
	}
	runID, stepID, thoughtID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	for _, q := range [][]any{
		{`INSERT INTO agent_runs (id, tenant_id, profile_id, profile_version, status, create_idempotency_key, goal_ref, initiator_type, initiator_id)
			VALUES ($1,$2,'p',3,'COMPLETED',$3,'g','user','alice')`, runID, tenant, runID},
		{`INSERT INTO agent_steps (id, tenant_id, run_id, seq, kind, executor_name, status, input_ref, input_hash)
			VALUES ($1,$2,$3,1,'tool','x','SUCCEEDED','r','h')`, stepID, tenant, runID},
		{`INSERT INTO run_thoughts (id, tenant_id, run_id, step_id, seq, kind, summary) VALUES ($1,$2,$3,$4,1,'hypothesis','the pool is exhausted')`,
			thoughtID, tenant, runID, stepID},
	} {
		if _, err := admin.pool.Exec(ctx, q[0].(string), q[1:]...); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"run", "step", "thought"} {
		nodes, err := app.GraphSearch(tctx, tenant, "", []string{kind}, 10)
		if err != nil || len(nodes) != 1 {
			t.Fatalf("%s: %v %v", kind, nodes, err)
		}
		a := nodes[0].Attrs
		if a["initiator_type"] != "user" || a["initiator_id"] != "alice" {
			t.Errorf("a %s node must carry its run's owner: %v", kind, a)
		}
	}
}
