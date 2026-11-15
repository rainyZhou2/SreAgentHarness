package migrations_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"go-sre-harness/migrations"
)

func TestMemoryGovernanceMigration(t *testing.T) {
	pool := freshDB(t)
	ctx := context.Background()
	if err := migrations.ApplyUntil(ctx, pool, "0036_unified_security"); err != nil {
		t.Fatal(err)
	}
	tenant := "migration"
	type fixture struct{ id, status, scope string }
	fixtures := []fixture{{uuid.NewString(), "ACTIVE", "service"}, {uuid.NewString(), "VALIDATING", "organization"},
		{uuid.NewString(), "REJECTED", "service"}, {uuid.NewString(), "ARCHIVED", "incident"},
		{uuid.NewString(), "CANDIDATE", "service"}, {uuid.NewString(), "STALE", "service"},
		{uuid.NewString(), "SUPERSEDED", "service"}, {uuid.NewString(), "ACTIVE", "user"}}
	for _, item := range fixtures {
		if _, err := pool.Exec(ctx, `INSERT INTO agent_memories (id,tenant_id,scope,scope_id,memory_type,status,subject,title,
			content_text,content_ref,content_hash,created_by) VALUES ($1,$2,$3,'owner','semantic',$4,'subject','title','content',
			'mem://migration/evidence/hash',$5,'alice')`, item.id, tenant, item.scope, item.status, item.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE agent_memories SET superseded_by=$2 WHERE id=$1`, fixtures[6].id, fixtures[0].id); err != nil {
		t.Fatal(err)
	}
	for _, model := range []string{"hash-uni-bigram-v2-2", "http:text-embedding"} {
		if _, err := pool.Exec(ctx, `INSERT INTO memory_embeddings (tenant_id,memory_id,memory_version,embedding_model,dims,embedding)
			VALUES ($1,$2,1,$3,2,ARRAY[0.1,0.2]::real[])`, tenant, fixtures[0].id, model); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrations.ApplyUntil(ctx, pool, "0037_memory_governance"); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"ACTIVE": "APPROVED", "VALIDATING": "CANDIDATE", "REJECTED": "REVOKED",
		"ARCHIVED": "REVOKED", "CANDIDATE": "CANDIDATE", "STALE": "STALE", "SUPERSEDED": "SUPERSEDED"}
	for _, item := range fixtures {
		var status, basis string
		var approvedBy *string
		if err := pool.QueryRow(ctx, `SELECT status,approval_basis,approved_by FROM agent_memories WHERE id=$1`, item.id).
			Scan(&status, &basis, &approvedBy); err != nil {
			t.Fatal(err)
		}
		if status != want[item.status] || approvedBy != nil {
			t.Fatalf("status migration for %s: status=%s approved_by=%v", item.status, status, approvedBy)
		}
		if item.status == "ACTIVE" && basis != "MIGRATED_ACTIVE" {
			t.Fatalf("migration basis: %s", basis)
		}
		var grants int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_memory_acl WHERE memory_id=$1`, item.id).Scan(&grants); err != nil || grants == 0 {
			t.Fatalf("ACL migration: %d %v", grants, err)
		}
		if item.scope != "user" {
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_memory_acl WHERE memory_id=$1 AND subject_type='role'
				AND subject_id='service'`, item.id).Scan(&grants); err != nil || grants != 1 {
				t.Fatalf("service ACL migration: %d %v", grants, err)
			}
		}
	}
	var verifications int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM memory_verifications`).Scan(&verifications); err != nil || verifications != 0 {
		t.Fatalf("migration must not fabricate verification: %d %v", verifications, err)
	}
	versions := map[string]string{}
	rows, err := pool.Query(ctx, `SELECT embedding_model,embedding_version FROM memory_embeddings`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var model, version string
		if err := rows.Scan(&model, &version); err != nil {
			t.Fatal(err)
		}
		versions[model] = version
	}
	if versions["hash-uni-bigram-v2-2"] != "v2" || versions["http:text-embedding"] != "v1" {
		t.Fatalf("embedding migration: %+v", versions)
	}
	var primaryKey string
	if err := pool.QueryRow(ctx, `SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='memory_embeddings'::regclass
		AND contype='p'`).Scan(&primaryKey); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"tenant_id", "memory_id", "memory_version", "embedding_model", "embedding_version", "dims"} {
		if !contains(primaryKey, column) {
			t.Fatalf("embedding primary key %q misses %s", primaryKey, column)
		}
	}
}

func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
