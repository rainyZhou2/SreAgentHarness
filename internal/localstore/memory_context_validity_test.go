package localstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func TestPinnedMemoryCannotBypassRevocationOrVersion(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "kernel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	user := "u"
	s.runs["r"] = domain.Run{ID: "r", TenantID: "t", InitiatorType: "user", InitiatorID: &user}
	s.steps["retrieve"] = domain.Step{ID: "retrieve", RunID: "r", TenantID: "t", Seq: 1, Kind: domain.StepRetrieve, Status: domain.StepSucceeded}
	s.memHits["retrieve"] = []memoryHitRec{{MemoryID: "m", Version: 1}}
	base := memory.Memory{ID: "m", TenantID: "t", Version: 1, Status: "ACTIVE", Trust: "T2", Sensitivity: "internal", Content: "approved"}
	for _, tc := range []struct {
		name   string
		change func(*memory.Memory)
		want   int
	}{
		{"valid", func(*memory.Memory) {}, 1},
		{"restricted", func(m *memory.Memory) { m.Sensitivity = "restricted" }, 0},
		{"unknown sensitivity", func(m *memory.Memory) { m.Sensitivity = "" }, 0},
		{"own preference", func(m *memory.Memory) { m.Scope, m.ScopeID, m.Sensitivity = "user", user, "restricted" }, 1},
		{"foreign preference", func(m *memory.Memory) { m.Scope, m.ScopeID = "user", "other" }, 0},
		{"stale", func(m *memory.Memory) { m.Status = "STALE" }, 0},
		{"foreign", func(m *memory.Memory) { m.TenantID = "foreign" }, 0},
		{"expired", func(m *memory.Memory) { v := time.Now().Add(-time.Hour); m.ExpiresAt = &v }, 0},
		{"revised", func(m *memory.Memory) { m.Version = 2; m.Content = "unreviewed changed content" }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.change(&m)
			s.memories["m"] = memoryRec{Mem: m}
			got, err := s.SnapshotContext(context.Background(), "t", "r", "", 2)
			if err != nil || len(got.Memories) != tc.want {
				t.Fatalf("context=%+v error=%v", got, err)
			}
		})
	}
}
