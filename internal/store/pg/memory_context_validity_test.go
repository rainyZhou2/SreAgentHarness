package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go-sre-harness/internal/memory"
)

func TestPinnedMemoryLifecyclePostgres(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		update string
		want   int
	}{
		{"active", "", 1},
		{"restricted", "sensitivity='restricted'", 0},
		{"own preference", "scope='user',scope_id='pin-user',sensitivity='restricted'", 1},
		{"foreign preference", "scope='user',scope_id='other'", 0},
		{"stale", "status='STALE'", 0},
		{"archived", "status='ARCHIVED'", 0},
		{"expired", "expires_at=now()-interval '1 hour'", 0},
		{"revised", "current_version=2,content_text='unreviewed'", 0},
		{"environment", "environment='other'", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tenant := "pin-" + uuid.NewString()
			runID, stepID := seedRun(t, s, tenant)
			if _, err := s.pool.Exec(ctx, `UPDATE agent_runs SET initiator_type='user',initiator_id='pin-user' WHERE id=$1`, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(ctx, `UPDATE agent_steps SET kind='retrieve_memory' WHERE id=$1`, stepID); err != nil {
				t.Fatal(err)
			}
			m, _, err := s.CreateMemory(ctx, memory.NewMemory{TenantID: tenant, Scope: "organization", Type: "semantic", Status: "ACTIVE", Subject: "pin", Title: "pin", Content: "approved", ContentHash: uuid.NewString(), Trust: "T2", Sensitivity: "internal", CreatedBy: "test"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.pool.Exec(ctx, `INSERT INTO step_memory_hits (tenant_id,step_id,memory_id,memory_version,score,rank) VALUES ($1,$2,$3,1,1,1)`, tenant, stepID, m.ID); err != nil {
				t.Fatal(err)
			}
			if tc.update != "" {
				if _, err := s.pool.Exec(ctx, "UPDATE agent_memories SET "+tc.update+" WHERE id=$1", m.ID); err != nil {
					t.Fatal(err)
				}
			}
			for _, before := range []int64{0, 2} {
				got, err := s.pinnedMemories(ctx, tenant, runID, "", before)
				if err != nil || len(got) != tc.want {
					t.Fatalf("before=%d memories=%+v error=%v", before, got, err)
				}
			}
			foreign, err := s.pinnedMemories(ctx, "foreign", runID, "", 2)
			if err != nil || len(foreign) != 0 {
				t.Fatalf("foreign context leaked: %+v %v", foreign, err)
			}
		})
	}
}
