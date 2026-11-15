package gateway

import (
	"context"
	"encoding/json"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/security"
)

type modelSecurity struct {
	authorizer security.Authorizer
	request    security.Request
	decision   security.PolicyDecision
}

func (g *Gateway) SetSecurity(authorizer security.Authorizer) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.security = authorizer
}

func (g *Gateway) modelAuthorizer() security.Authorizer {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.security
}

func (g *Gateway) withSecurityCaller(ctx context.Context, scope Scope) context.Context {
	if g.modelAuthorizer() == nil {
		return ctx
	}
	subject := scope.Subject
	if subject == "" {
		subject = "run:" + scope.RunID
	}
	return security.WithCaller(ctx, security.Caller{TenantID: scope.TenantID, Subject: subject,
		WorkID: scope.WorkID, RunID: scope.RunID, Environment: scope.Environment})
}

func (g *Gateway) authorizeModel(ctx context.Context, scope Scope, request Request, canonical json.RawMessage) (*modelSecurity, error) {
	authorizer := g.modelAuthorizer()
	if authorizer == nil {
		return nil, nil
	}
	subject := scope.Subject
	if subject == "" {
		subject = "run:" + scope.RunID
	}
	securityRequest := security.Request{RequestID: "model:" + scope.RunID + ":" + scope.StepAttemptID,
		Operation: security.ModelEgress, TenantID: scope.TenantID, Subject: subject,
		ResourceRef: "model://" + request.ModelAlias, Environment: scope.Environment, DataClass: "internal",
		Risk: "R1", InputHash: artifact.Hash(canonical), WorkID: scope.WorkID, RunID: scope.RunID, OccurredAt: g.now().UTC()}
	decision, err := authorizer.Authorize(ctx, securityRequest)
	if err != nil {
		return nil, err
	}
	switch decision.Effect {
	case security.Allow:
	case security.RequireApproval:
		return nil, &security.Error{Code: security.ApprovalRequired, Message: "model egress requires an administrative approval"}
	default:
		return nil, &security.Error{Code: security.Denied, Message: "model egress denied"}
	}
	return &modelSecurity{authorizer: authorizer, request: securityRequest, decision: decision}, nil
}

func (s *modelSecurity) recheck(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.authorizer.Recheck(ctx, s.request, s.decision)
}
