package memory

import (
	"context"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

type WriterStore interface {
	CreateMemory(ctx context.Context, memory NewMemory) (Memory, bool, error)
	TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (Memory, error)
}

type SecureRetriever struct {
	*Retriever
	authorizer security.Authorizer
}

func WithSecurityRetriever(next *Retriever, authorizer security.Authorizer) *SecureRetriever {
	return &SecureRetriever{Retriever: next, authorizer: authorizer}
}

func (r *SecureRetriever) Retrieve(ctx context.Context, lease domain.Lease, goal string) (domain.StepSuccess, error) {
	caller, err := memoryCaller(ctx, lease.TenantID)
	if err != nil {
		return domain.StepSuccess{}, err
	}
	if caller.RunID != lease.RunID {
		return domain.StepSuccess{}, &security.Error{Code: security.InvalidRequest, Message: "memory retrieval run does not match the lease"}
	}
	request := newMemoryRequest(caller, security.MemoryRead, "memory://retrieve", artifact.Hash([]byte(goal)), "R0", "internal")
	if err := authorizeMemory(ctx, r.authorizer, request); err != nil {
		return domain.StepSuccess{}, err
	}
	return r.Retriever.Retrieve(ctx, lease, goal)
}

func (r *SecureRetriever) Search(ctx context.Context, tenantID, runID, query string, extra []string, k int) (SearchResult, error) {
	caller, err := memoryCaller(ctx, tenantID)
	if err != nil {
		return SearchResult{}, err
	}
	if caller.RunID != "" && caller.RunID != runID {
		return SearchResult{}, &security.Error{Code: security.InvalidRequest, Message: "memory retrieval run does not match the caller"}
	}
	request := newMemoryRequest(caller, security.MemoryRead, "memory://search", artifact.Hash([]byte(query)), "R0", "internal")
	if err := authorizeMemory(ctx, r.authorizer, request); err != nil {
		return SearchResult{}, err
	}
	return r.Retriever.Search(ctx, tenantID, runID, query, extra, k)
}

func (r *SecureRetriever) PlanMemories(ctx context.Context, tenantID, goal string, services []string, k int) ([]PlanMemory, error) {
	caller, err := memoryCaller(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	request := newMemoryRequest(caller, security.MemoryRead, "memory://plans", artifact.Hash([]byte(goal)), "R0", "internal")
	if err := authorizeMemory(ctx, r.authorizer, request); err != nil {
		return nil, err
	}
	return r.Retriever.PlanMemories(ctx, tenantID, goal, services, k)
}

type securedWriter struct {
	next       WriterStore
	authorizer security.Authorizer
}

func WithSecurityWriter(next WriterStore, authorizer security.Authorizer) WriterStore {
	return securedWriter{next: next, authorizer: authorizer}
}

func (w securedWriter) CreateMemory(ctx context.Context, memory NewMemory) (Memory, bool, error) {
	caller, err := memoryCaller(ctx, memory.TenantID)
	if err != nil {
		return Memory{}, false, err
	}
	operation := security.MemoryWrite
	risk := "R1"
	if memory.Status == Approved {
		operation = security.MemoryPublish
		risk = "R2"
	}
	content := memory.ContentRef + "\x00" + memory.ContentHash
	if content == "\x00" {
		content = memory.Content
	}
	dataClass := memory.Sensitivity
	if dataClass == "" {
		dataClass = "internal"
	}
	request := newMemoryRequest(caller, operation, "memory://write", artifact.Hash([]byte(content)), risk, dataClass)
	if err := authorizeMemory(ctx, w.authorizer, request); err != nil {
		return Memory{}, false, err
	}
	return w.next.CreateMemory(ctx, memory)
}

func (w securedWriter) TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (Memory, error) {
	caller, err := memoryCaller(ctx, tenantID)
	if err != nil {
		return Memory{}, err
	}
	operation := security.MemoryWrite
	risk := "R1"
	if to == Approved {
		operation = security.MemoryPublish
		risk = "R2"
	}
	request := newMemoryRequest(caller, operation, "memory://"+id, artifact.Hash([]byte(id+"\x00"+from+"\x00"+to)), risk, "internal")
	if err := authorizeMemory(ctx, w.authorizer, request); err != nil {
		return Memory{}, err
	}
	return w.next.TransitionMemory(ctx, tenantID, id, from, to, actor, reason)
}

type securedConsolidationStore struct {
	ConsolidationStore
	authorizer security.Authorizer
}

func WithSecurityConsolidationStore(next ConsolidationStore, authorizer security.Authorizer) ConsolidationStore {
	return securedConsolidationStore{ConsolidationStore: next, authorizer: authorizer}
}

func (s securedConsolidationStore) CreateMemory(ctx context.Context, memory NewMemory) (Memory, bool, error) {
	writer, ok := s.ConsolidationStore.(WriterStore)
	if ok {
		return WithSecurityWriter(writer, s.authorizer).CreateMemory(ctx, memory)
	}
	caller, err := memoryCaller(ctx, memory.TenantID)
	if err != nil {
		return Memory{}, false, err
	}
	operation := security.MemoryWrite
	risk := "R1"
	if memory.Status == Approved {
		operation = security.MemoryPublish
		risk = "R2"
	}
	content := memory.ContentRef + "\x00" + memory.ContentHash
	if content == "\x00" {
		content = memory.Content
	}
	request := newMemoryRequest(caller, operation, "memory://write", artifact.Hash([]byte(content)), risk, memory.Sensitivity)
	if err := authorizeMemory(ctx, s.authorizer, request); err != nil {
		return Memory{}, false, err
	}
	return s.ConsolidationStore.CreateMemory(ctx, memory)
}

func memoryCaller(ctx context.Context, tenantID string) (security.Caller, error) {
	caller, ok := security.CallerFrom(ctx)
	if !ok || caller.TenantID != tenantID {
		return security.Caller{}, &security.Error{Code: security.InvalidRequest, Message: "memory caller does not match the tenant"}
	}
	return caller, nil
}

func newMemoryRequest(caller security.Caller, operation security.Operation, resourceRef, inputHash, risk, dataClass string) security.Request {
	if dataClass == "" {
		dataClass = "internal"
	}
	return security.Request{RequestID: caller.RunID + ":" + string(operation) + ":" + inputHash,
		Operation: operation, TenantID: caller.TenantID, Subject: caller.Subject, ResourceRef: resourceRef,
		Environment: caller.Environment, DataClass: dataClass, Risk: risk, InputHash: inputHash,
		WorkID: caller.WorkID, RunID: caller.RunID}
}

func authorizeMemory(ctx context.Context, authorizer security.Authorizer, request security.Request) error {
	if authorizer == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "memory security authorizer is not configured"}
	}
	decision, err := authorizer.Authorize(ctx, request)
	if err != nil {
		return err
	}
	switch decision.Effect {
	case security.Allow:
	case security.RequireApproval:
		return &security.Error{Code: security.ApprovalRequired, Message: "memory operation requires an administrative approval"}
	default:
		return &security.Error{Code: security.Denied, Message: "memory operation denied"}
	}
	return authorizer.Recheck(ctx, request, decision)
}
