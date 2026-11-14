package memory

import (
	"context"
	"errors"
	"time"

	"go-sre-harness/internal/domain"
)

// MemoryContext gives snapshot readers the same current environment as search.
// The kernel only depends on this context-enrichment contract, not Env or SRE.
func (r *Retriever) MemoryContext(ctx context.Context, tenantID, runID string) context.Context {
	return WithEnv(ctx, r.environment(ctx, tenantID, runID, nil))
}

// RecallableMemory revalidates a historical search hit against the live ledger.
// Missing readers and missing records fail closed; storage errors remain errors.
func (r *Retriever) RecallableMemory(ctx context.Context, tenantID, runID, id string, version int64) (bool, error) {
	reader, ok := r.Store.(interface {
		GetMemory(context.Context, string, string) (Memory, error)
	})
	if !ok || id == "" || version < 1 || domain.MemoryExcluded(ctx, id) {
		return false, nil
	}
	m, err := reader.GetMemory(ctx, tenantID, id)
	if errors.Is(err, domain.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if m.ID != id || m.Version != version || m.Scope == "user" || (m.Sensitivity != "public" && m.Sensitivity != "internal") {
		return false, nil
	}
	valid, _ := m.Recallable(tenantID, r.environment(ctx, tenantID, runID, nil), r.now())
	return valid, nil
}

// Recallable applies lifecycle and tenant constraints even for custom providers.
func (m Memory) Recallable(tenantID string, env Env, at time.Time) (bool, string) {
	if tenantID == "" || m.TenantID != tenantID {
		return false, "tenant"
	}
	if m.Status != "ACTIVE" {
		return false, "status " + m.Status
	}
	if rank, ok := trustRank[m.Trust]; !ok || rank >= trustRank["T4"] {
		return false, "untrusted"
	}
	if m.Environment != nil && *m.Environment != "" && !matchAny([]string{*m.Environment}, []string{env.Environment}, true) {
		return false, "environment"
	}
	return m.Applies(env, at)
}

func (r *Retriever) environment(ctx context.Context, tenantID, runID string, services []string) Env {
	env, ok := EnvFrom(ctx)
	if !ok && r.Env != nil {
		env, ok = r.Env(ctx, tenantID, runID)
	}
	if !ok {
		return Env{Services: services}
	}
	if len(env.Services) == 0 {
		env.Services = services
	}
	return env
}

func (r *Retriever) now() time.Time {
	if r.Now != nil {
		return r.Now().UTC()
	}
	return time.Now().UTC()
}
