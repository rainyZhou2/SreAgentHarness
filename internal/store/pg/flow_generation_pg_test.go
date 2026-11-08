package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// Replanned flows reuse the flow_runs row with a higher generation; node
// attempts of different generations may share node ids and attempt numbers,
// and LoadFlow only returns the current generation's attempts.
func TestFlowGenerationsPostgres(t *testing.T) {
	admin, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "t-flowgen-" + uuid.NewString()[:8]
	runID, _ := seedRun(t, admin, tenant)
	flowID := uuid.NewString()
	if _, err := admin.pool.Exec(ctx, `INSERT INTO flow_runs (id, tenant_id, run_id, compiled_ref, compiled_hash, status)
		VALUES ($1,$2,$3,'c','h','FAILED')`, flowID, tenant, runID); err != nil {
		t.Fatal(err)
	}
	insertNode := func(gen int, status string) {
		t.Helper()
		if _, err := admin.pool.Exec(ctx, `INSERT INTO flow_node_runs (id, tenant_id, flow_run_id, node_id, node_type,
			status, attempt, generation) VALUES ($1,$2,$3,'probe','agent',$4,1,$5)`,
			uuid.NewString(), tenant, flowID, status, gen); err != nil {
			t.Fatalf("generation %d node: %v", gen, err)
		}
	}
	insertNode(0, "FAILED")
	insertNode(1, "RUNNING") // same node id and attempt, new generation
	if _, err := admin.pool.Exec(ctx, `INSERT INTO flow_node_runs (id, tenant_id, flow_run_id, node_id, node_type,
		status, attempt, generation) VALUES ($1,$2,$3,'probe','agent','RUNNING',1,1)`, uuid.NewString(), tenant, flowID); err == nil {
		t.Fatal("duplicate node attempt within one generation must be rejected")
	}

	snap, err := admin.LoadFlow(ctx, tenant, runID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Generation != 0 || len(snap.Nodes) != 1 || snap.Nodes[0].Status != "FAILED" {
		t.Fatalf("generation 0 view: %+v", snap)
	}
	if _, err := admin.pool.Exec(ctx, `UPDATE flow_runs SET generation=1, status='RUNNING' WHERE id=$1`, flowID); err != nil {
		t.Fatal(err)
	}
	snap, err = admin.LoadFlow(ctx, tenant, runID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Generation != 1 || len(snap.Nodes) != 1 || snap.Nodes[0].Status != "RUNNING" {
		t.Fatalf("generation 1 view: %+v", snap)
	}
}
