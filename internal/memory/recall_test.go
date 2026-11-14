package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
)

type recallStore struct {
	candidateStore
	m   Memory
	err error
}

func (s recallStore) GetMemory(context.Context, string, string) (Memory, error) { return s.m, s.err }

func TestHistoricalRecallUsesCurrentLifecycle(t *testing.T) {
	now := time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Second)
	base := Memory{ID: "m", TenantID: "t", Version: 2, Status: "ACTIVE", Trust: "T2", Sensitivity: "internal"}
	for _, tc := range []struct {
		name   string
		mutate func(*Memory)
	}{
		{"foreign", func(m *Memory) { m.TenantID = "other" }},
		{"wrong-id", func(m *Memory) { m.ID = "other" }},
		{"updated", func(m *Memory) { m.Version++ }},
		{"revoked", func(m *Memory) { m.Status = "REVOKED" }},
		{"expired", func(m *Memory) { m.ExpiresAt = &past }},
		{"superseded", func(m *Memory) { v := "new"; m.SupersededBy = &v }},
		{"untrusted", func(m *Memory) { m.Trust = "T4" }},
		{"restricted", func(m *Memory) { m.Sensitivity = "restricted" }},
		{"user-scope", func(m *Memory) { m.Scope, m.ScopeID = "user", "someone" }},
		{"unknown-risk", func(m *Memory) {
			m.Applicability = &Applicability{RiskOfReuse: "high", VersionConstraints: []string{"driver>=550"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := base
			tc.mutate(&m)
			r := Retriever{Store: recallStore{m: m}, Now: func() time.Time { return now }}
			valid, err := r.RecallableMemory(context.Background(), "t", "run", "m", 2)
			if err != nil || valid {
				t.Fatalf("valid=%v err=%v", valid, err)
			}
		})
	}
	r := Retriever{Store: recallStore{m: base}, Now: func() time.Time { return now }}
	if ok, err := r.RecallableMemory(context.Background(), "t", "run", "m", 2); !ok || err != nil {
		t.Fatalf("active memory: %v %v", ok, err)
	}
	if ok, err := r.RecallableMemory(domain.WithMemoryExclude(context.Background(), []string{"m"}), "t", "run", "m", 2); ok || err != nil {
		t.Fatalf("excluded memory: %v %v", ok, err)
	}
	for _, storeErr := range []error{domain.ErrNotFound, errors.New("storage unavailable")} {
		r.Store = recallStore{err: storeErr}
		ok, err := r.RecallableMemory(context.Background(), "t", "run", "m", 2)
		if ok || (storeErr == domain.ErrNotFound && err != nil) || (storeErr != domain.ErrNotFound && !errors.Is(err, storeErr)) {
			t.Fatalf("storage error: %v %v", ok, err)
		}
	}
	r.Store = candidateStore{}
	if ok, err := r.RecallableMemory(context.Background(), "t", "run", "m", 2); ok || err != nil {
		t.Fatalf("missing reader: %v %v", ok, err)
	}
}
