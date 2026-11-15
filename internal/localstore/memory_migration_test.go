package localstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/memory"
)

func writeMemoryV2Fixture(t *testing.T, path string, records []memoryRec) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE meta (key TEXT PRIMARY KEY,value TEXT NOT NULL)`,
		`CREATE TABLE objects (kind TEXT NOT NULL,id TEXT NOT NULL,parent TEXT NOT NULL DEFAULT '',seq INTEGER NOT NULL DEFAULT 0,data BLOB NOT NULL,PRIMARY KEY(kind,id))`,
		`CREATE TABLE event_index (run_id TEXT NOT NULL,seq INTEGER NOT NULL,id TEXT NOT NULL,type TEXT NOT NULL,step_id TEXT NOT NULL DEFAULT '',file TEXT NOT NULL,offset INTEGER NOT NULL,length INTEGER NOT NULL,occurred_at TEXT NOT NULL,PRIMARY KEY(run_id,seq))`,
		`INSERT INTO meta VALUES('schema_version','2')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO objects(kind,id,data) VALUES('memory',?,?)`, rec.Mem.ID, data); err != nil {
			t.Fatal(err)
		}
	}
}

func memoryV2Record(id, status, scope string) memoryRec {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return memoryRec{Mem: memory.Memory{ID: id, TenantID: "tenant", Scope: scope, ScopeID: "owner", Type: "semantic",
		Status: status, Version: 1, Subject: "subject", Title: "title", Content: "content", ContentHash: id,
		Trust: "T2", Sensitivity: "internal", CreatedBy: "alice", CreatedAt: now, UpdatedAt: now},
		Embeddings: map[string][]float32{"hash-uni-bigram-v2-16": make([]float32, 16)}}
}

func TestMemoryGovernanceMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local.db")
	verifiedAt := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	active := memoryV2Record("active", "ACTIVE", "service")
	active.Verifications = map[string]memory.VerificationRecord{"verify": {ID: "verification", TenantID: "tenant", MemoryID: "active",
		MemoryVersion: 1, InputHash: "sha256:input", Verifier: "service:verifier", CreatedAt: verifiedAt}}
	active.Conflicts = map[string]memory.ConflictRecord{"conflict": {TenantID: "tenant", Reason: "contradiction", CreatedBy: "alice"}}
	records := []memoryRec{active, memoryV2Record("validating", "VALIDATING", "service"),
		memoryV2Record("rejected", "REJECTED", "service"), memoryV2Record("archived", "ARCHIVED", "organization"),
		memoryV2Record("personal", "ACTIVE", "user")}
	writeMemoryV2Fixture(t, path, records)
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.Upgraded() == nil || s.Upgraded().From != 2 || s.Upgraded().To != 3 {
		t.Fatalf("upgrade: %+v", s.Upgraded())
	}
	if _, err := os.Stat(s.Upgraded().Backup); err != nil {
		t.Fatal(err)
	}
	wantStatuses := map[string]string{"active": memory.Approved, "validating": memory.StatusCandidate,
		"rejected": memory.Revoked, "archived": memory.Revoked, "personal": memory.Approved}
	for id, want := range wantStatuses {
		rec := s.memories[id]
		if rec.Mem.Status != want || len(rec.ACL) == 0 {
			t.Fatalf("%s migration: %+v", id, rec)
		}
		if rec.Mem.Scope != "user" && !hasSubject(rec.ACL, "role:service") {
			t.Fatalf("%s is missing its worker grant: %+v", id, rec.ACL)
		}
		if len(rec.EmbeddingDescriptors) != 1 {
			t.Fatalf("%s embedding descriptors: %+v", id, rec.EmbeddingDescriptors)
		}
		for key, descriptor := range rec.EmbeddingDescriptors {
			if !descriptor.Valid() || descriptor.Version != "v2" || !descriptor.Matches(rec.Embeddings[key]) {
				t.Fatalf("%s invalid embedding migration: %+v", id, rec)
			}
		}
	}
	if got := s.memories["active"]; got.Mem.ApprovalBasis != "MIGRATED_ACTIVE" || got.Mem.ApprovedBy != nil ||
		len(got.Verifications) != 1 || len(got.Conflicts) != 1 {
		t.Fatalf("publication and governance records: %+v", got)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.memories["active"]; got.Mem.ApprovalBasis != "MIGRATED_ACTIVE" || len(got.Verifications) != 1 || len(got.Conflicts) != 1 {
		t.Fatalf("governance records did not survive restart: %+v", got)
	}
}

func TestMemoryGovernanceMigrationRollsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local.db")
	bad := memoryV2Record("bad", "ACTIVE", "service")
	bad.Embeddings = map[string][]float32{"": {1, 2}}
	writeMemoryV2Fixture(t, path, []memoryRec{bad})
	if _, err := Open(path); err == nil {
		t.Fatal("an invalid legacy embedding must abort the upgrade")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version string
	if err := db.QueryRow(`SELECT value FROM meta WHERE key='schema_version'`).Scan(&version); err != nil || version != "2" {
		t.Fatalf("failed migration must roll back: version=%s err=%v", version, err)
	}
	backup, err := sql.Open("sqlite", path+".v2.bak")
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	if err := backup.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func hasSubject(grants []memory.ACLGrant, subject string) bool {
	for _, grant := range grants {
		if grant.Subject == subject {
			return true
		}
	}
	return false
}
