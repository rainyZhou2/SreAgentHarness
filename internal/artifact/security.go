package artifact

import (
	"context"

	"go-sre-harness/internal/security"
)

type securedStore struct {
	next       Store
	authorizer security.Authorizer
}

func WithSecurity(next Store, authorizer security.Authorizer) Store {
	return securedStore{next: next, authorizer: authorizer}
}

func (s securedStore) Put(ctx context.Context, tenantID, kind string, data []byte) (string, string, error) {
	hash := Hash(data)
	request, err := artifactRequest(ctx, security.ArtifactWrite, tenantID, "artifact://"+kind+"/"+hash, hash, "R1")
	if err != nil {
		return "", "", err
	}
	if err := s.authorize(ctx, request); err != nil {
		return "", "", err
	}
	return s.next.Put(ctx, tenantID, kind, data)
}

func (s securedStore) Get(ctx context.Context, tenantID, ref string) ([]byte, error) {
	request, err := artifactRequest(ctx, security.ArtifactRead, tenantID, ref, Hash(nil), "R0")
	if err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, request); err != nil {
		return nil, err
	}
	return s.next.Get(ctx, tenantID, ref)
}

func (s securedStore) Delete(ctx context.Context, tenantID, ref string) error {
	deleter, ok := s.next.(Deleter)
	if !ok {
		return &security.Error{Code: security.PolicyUnavailable, Message: "artifact deletion is not supported"}
	}
	request, err := artifactRequest(ctx, security.ArtifactDelete, tenantID, ref, Hash([]byte(ref)), "R2")
	if err != nil {
		return err
	}
	if err := s.authorize(ctx, request); err != nil {
		return err
	}
	return deleter.Delete(ctx, tenantID, ref)
}

func (s securedStore) authorize(ctx context.Context, request security.Request) error {
	if s.authorizer == nil {
		return &security.Error{Code: security.PolicyUnavailable, Message: "artifact security authorizer is not configured"}
	}
	decision, err := s.authorizer.Authorize(ctx, request)
	if err != nil {
		return err
	}
	switch decision.Effect {
	case security.Allow:
	case security.RequireApproval:
		return &security.Error{Code: security.ApprovalRequired, Message: "artifact operation requires an administrative approval"}
	default:
		return &security.Error{Code: security.Denied, Message: "artifact operation denied"}
	}
	return s.authorizer.Recheck(ctx, request, decision)
}

func artifactRequest(ctx context.Context, operation security.Operation, tenantID, resourceRef, inputHash, risk string) (security.Request, error) {
	caller, ok := security.CallerFrom(ctx)
	if !ok || caller.TenantID != tenantID {
		return security.Request{}, &security.Error{Code: security.InvalidRequest, Message: "artifact caller does not match the tenant"}
	}
	requestID := caller.RunID + ":" + string(operation) + ":" + Hash([]byte(resourceRef+inputHash))
	if requestID[0] == ':' {
		requestID = requestID[1:]
	}
	return security.Request{RequestID: requestID, Operation: operation, TenantID: tenantID, Subject: caller.Subject,
		ResourceRef: resourceRef, Environment: caller.Environment, DataClass: "internal", Risk: risk, InputHash: inputHash,
		WorkID: caller.WorkID, RunID: caller.RunID}, nil
}

var _ Store = securedStore{}
var _ Deleter = securedStore{}
