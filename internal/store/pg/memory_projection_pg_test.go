package pg

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/memory"
)

func TestPinnedMemoryGovernancePostgres(t *testing.T) {
	s, _, _ := pgTestStores(t)
	ctx := context.Background()
	tenant := "projection-" + uuid.NewString()
	tctx := memory.WithAccessContext(ctx, memory.AccessContext{TenantID: tenant, Subjects: []string{"role:admin"}})
	runID, stepID := seedRun(t, s, tenant)
	if _, err := s.pool.Exec(ctx, `UPDATE agent_steps SET kind='retrieve_memory' WHERE id=$1`, stepID); err != nil {
		t.Fatal(err)
	}
	environment := "production"
	descriptor := memory.EmbeddingDescriptor{Model: "text-embedding", Version: "v3", Dimensions: 2}
	m, _, err := s.CreateMemory(ctx, memory.NewMemory{TenantID: tenant, Scope: "organization", Type: "semantic",
		Status: memory.Verified, Subject: "projection", Title: "governed", Content: "content", ContentHash: uuid.NewString(),
		Trust: "T2", Sensitivity: "internal", CreatedBy: "test", Environment: &environment,
		Applicability:       &memory.Applicability{Environments: []string{"production"}, Services: []string{"checkout"}},
		EmbeddingDescriptor: descriptor, Embedding: []float32{0.1, 0.2}})
	if err != nil {
		t.Fatal(err)
	}
	verificationID := uuid.NewString()
	if _, err := s.pool.Exec(ctx, `INSERT INTO memory_verifications
		(id,tenant_id,memory_id,memory_version,evidence_refs,decision_ref,outcome_ref,input_hash,verifier,idempotency_key)
		VALUES ($1,$2,$3,1,'["obs:1"]','decision:1','outcome:1','sha256:test','service:verifier',$4)`,
		verificationID, tenant, m.ID, uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE agent_memories SET decision_ref='decision:1',outcome_ref='outcome:1' WHERE id=$1`, m.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `INSERT INTO step_memory_hits (tenant_id,step_id,memory_id,memory_version,score,rank)
		VALUES ($1,$2,$3,1,0.8,1)`, tenant, stepID, m.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.pinnedMemories(tctx, tenant, runID, "", 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("memories=%+v error=%v", got, err)
	}
	ref := got[0]
	if ref.Status != memory.Verified || ref.Sensitivity != "internal" || ref.VerificationID != verificationID ||
		ref.Environment == nil || *ref.Environment != environment || ref.Applicability == nil ||
		ref.Embedding == nil || *ref.Embedding != (cognitive.EmbeddingRef{Model: descriptor.Model, Version: descriptor.Version, Dimensions: descriptor.Dimensions}) {
		t.Fatalf("governance projection: %#v", ref)
	}
}
