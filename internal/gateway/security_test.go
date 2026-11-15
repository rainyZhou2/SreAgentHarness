package gateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go-sre-harness/internal/security"
)

type securityArtifactProbe struct {
	calls int
}

func (p *securityArtifactProbe) Put(context.Context, string, string, []byte) (string, string, error) {
	p.calls++
	return "mem://t/model/request", "sha256:request", nil
}

type securityProviderProbe struct {
	calls int
}

func (p *securityProviderProbe) ID() string { return "provider" }
func (p *securityProviderProbe) Stream(context.Context, Endpoint, Request, Sink) error {
	p.calls++
	return nil
}

type fallbackProviderProbe struct {
	calls int
}

func (p *fallbackProviderProbe) ID() string { return "provider" }
func (p *fallbackProviderProbe) Stream(_ context.Context, _ Endpoint, _ Request, emit Sink) error {
	p.calls++
	if p.calls == 1 {
		return &Error{Code: ErrProviderUnavailable, Sent: false}
	}
	emit(Event{Type: EvStarted})
	usage := Usage{}
	emit(Event{Type: EvCompleted, FinishReason: "end_turn", Usage: &usage})
	return nil
}

type gatewayAuthorizer struct {
	request    security.Request
	decision   security.PolicyDecision
	recheckErr error
	rechecks   int
}

func (a *gatewayAuthorizer) Authorize(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	a.request = request
	return a.decision, nil
}
func (a *gatewayAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	a.rechecks++
	return a.recheckErr
}

func securityRoute() RoutePolicy {
	return RoutePolicy{ID: "route", Version: 1, MaxAttempts: 2, Aliases: map[string][]string{"default": {"one", "two"}},
		Endpoints: []Endpoint{
			{ID: "one", ProviderID: "provider", ModelID: "model", ContextWindow: 10000, MaxOutput: 1000},
			{ID: "two", ProviderID: "provider", ModelID: "model", ContextWindow: 10000, MaxOutput: 1000},
		}}
}

func TestGenerateSecurityBeforeArtifactsAndProvider(t *testing.T) {
	artifacts := &securityArtifactProbe{}
	provider := &securityProviderProbe{}
	gateway := New(securityRoute(), NewMemLedger(), artifacts, Admission{}, slog.Default(), provider)
	authorizer := &gatewayAuthorizer{decision: security.PolicyDecision{Effect: security.Deny, ExpiresAt: time.Now().Add(time.Minute)}}
	gateway.SetSecurity(authorizer)
	_, err := gateway.Generate(context.Background(), Scope{TenantID: "t", RunID: "r", WorkID: "w", Subject: "work:w"}, Request{ModelAlias: "default"}, nil)
	if err == nil || artifacts.calls != 0 || provider.calls != 0 {
		t.Fatalf("artifacts=%d provider=%d err=%v", artifacts.calls, provider.calls, err)
	}
	if authorizer.request.Operation != security.ModelEgress || authorizer.request.InputHash == "" || authorizer.request.WorkID != "w" {
		t.Fatalf("request=%+v", authorizer.request)
	}
}

func TestEveryProviderAttemptRechecks(t *testing.T) {
	artifacts := &securityArtifactProbe{}
	provider := &fallbackProviderProbe{}
	gateway := New(securityRoute(), NewMemLedger(), artifacts, Admission{}, slog.Default(), provider)
	authorizer := &gatewayAuthorizer{decision: security.PolicyDecision{Effect: security.Allow, ExpiresAt: time.Now().Add(time.Minute)}}
	gateway.SetSecurity(authorizer)
	authorizer.recheckErr = &security.Error{Code: security.Revoked}
	_, err := gateway.Generate(context.Background(), Scope{TenantID: "t", RunID: "r"}, Request{ModelAlias: "default"}, nil)
	if err == nil || provider.calls != 0 {
		t.Fatalf("revoked request reached provider: calls=%d err=%v", provider.calls, err)
	}
	authorizer.recheckErr = nil
	if _, err = gateway.Generate(context.Background(), Scope{TenantID: "t", RunID: "r"}, Request{ModelAlias: "default"}, nil); err != nil {
		t.Fatal(err)
	}
	if authorizer.rechecks < 3 || provider.calls != 2 {
		t.Fatalf("rechecks=%d calls=%d", authorizer.rechecks, provider.calls)
	}
}
