package memory

import (
	"context"
	"strconv"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/security"
)

type securedGovernanceStore struct {
	next       GovernanceStore
	authorizer security.Authorizer
}

func WithSecurityGovernanceStore(next GovernanceStore, authorizer security.Authorizer) GovernanceStore {
	return securedGovernanceStore{next: next, authorizer: authorizer}
}

func (s securedGovernanceStore) GetMemory(ctx context.Context, tenantID, id string) (Memory, error) {
	return s.next.GetMemory(ctx, tenantID, id)
}

func (s securedGovernanceStore) VerifyMemory(ctx context.Context, input VerificationInput, verifier, idempotencyKey string) (VerificationRecord, Memory, error) {
	caller, err := memoryCaller(ctx, input.TenantID)
	if err != nil {
		return VerificationRecord{}, Memory{}, err
	}
	binding := input.MemoryID + "\x00" + strconv.FormatInt(input.ExpectedVersion, 10) + "\x00" + input.InputHash + "\x00" + idempotencyKey
	request := newMemoryRequest(caller, security.MemoryPublish, "memory://"+input.MemoryID, artifact.Hash([]byte(binding)), "R2", "internal")
	if err := authorizeMemory(ctx, s.authorizer, request); err != nil {
		return VerificationRecord{}, Memory{}, err
	}
	return s.next.VerifyMemory(ctx, input, verifier, idempotencyKey)
}

func (s securedGovernanceStore) ApproveMemory(ctx context.Context, tenantID, id string, expectedVersion int64, approver string) (Memory, error) {
	caller, err := memoryCaller(ctx, tenantID)
	if err != nil {
		return Memory{}, err
	}
	binding := id + "\x00" + strconv.FormatInt(expectedVersion, 10) + "\x00" + approver
	request := newMemoryRequest(caller, security.MemoryPublish, "memory://"+id, artifact.Hash([]byte(binding)), "R2", "internal")
	if err := authorizeMemory(ctx, s.authorizer, request); err != nil {
		return Memory{}, err
	}
	return s.next.ApproveMemory(ctx, tenantID, id, expectedVersion, approver)
}
