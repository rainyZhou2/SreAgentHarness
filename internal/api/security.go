package api

import (
	"context"

	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/security"
)

type securedMemoryStore struct {
	MemoryStore
	writer     memory.WriterStore
	governance memory.GovernanceStore
}

func WithMemorySecurity(next MemoryStore, authorizer security.Authorizer) MemoryStore {
	return securedMemoryStore{MemoryStore: next, writer: memory.WithSecurityWriter(next, authorizer),
		governance: memory.WithSecurityGovernanceStore(next, authorizer)}
}

func (s securedMemoryStore) CreateMemory(ctx context.Context, value memory.NewMemory) (memory.Memory, bool, error) {
	return s.writer.CreateMemory(ctx, value)
}

func (s securedMemoryStore) TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error) {
	return s.writer.TransitionMemory(ctx, tenantID, id, from, to, actor, reason)
}

func (s securedMemoryStore) VerifyMemory(ctx context.Context, input memory.VerificationInput, verifier, idempotencyKey string) (memory.VerificationRecord, memory.Memory, error) {
	return s.governance.VerifyMemory(ctx, input, verifier, idempotencyKey)
}

func (s securedMemoryStore) ApproveMemory(ctx context.Context, tenantID, id string, expectedVersion int64, approver string) (memory.Memory, error) {
	return s.governance.ApproveMemory(ctx, tenantID, id, expectedVersion, approver)
}
