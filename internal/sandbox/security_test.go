package sandbox

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/security"
	"go-sre-harness/internal/tools"
)

type securityBackendProbe struct {
	execCalls    int
	destroyCalls int
}

func (b *securityBackendProbe) Info() Info { return Info{Backend: "probe", Isolation: "container"} }
func (b *securityBackendProbe) Exec(context.Context, Request) (Result, error) {
	b.execCalls++
	return Result{}, nil
}
func (b *securityBackendProbe) Destroy(context.Context, string, string) error {
	b.destroyCalls++
	return nil
}

type sandboxAuthorizer struct {
	request    security.Request
	decision   security.PolicyDecision
	recheckErr error
}

func (a *sandboxAuthorizer) Authorize(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	a.request = request
	return a.decision, nil
}
func (a *sandboxAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	return a.recheckErr
}

func TestBackendSecurityCannotBeBypassed(t *testing.T) {
	backend := &securityBackendProbe{}
	authorizer := &sandboxAuthorizer{decision: security.PolicyDecision{Effect: security.Allow, ExpiresAt: time.Now().Add(time.Minute)}}
	guarded := WithSecurity(backend, authorizer, "production")
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t", RunID: "r", WorkID: "w", Subject: "work:w", AttemptID: "a"})
	if _, err := guarded.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	authorizer.recheckErr = &security.Error{Code: security.Revoked}
	if _, err := guarded.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"true"}}); err == nil {
		t.Fatal("revoked command reached backend")
	}
	if backend.execCalls != 1 || authorizer.request.Operation != security.SandboxExecute || authorizer.request.InputHash == "" {
		t.Fatalf("calls=%d request=%+v", backend.execCalls, authorizer.request)
	}
}

func TestBackendSecurityRejectsContextMismatch(t *testing.T) {
	for name, request := range map[string]Request{
		"tenant": {TenantID: "other", RunID: "r", Argv: []string{"true"}},
		"run":    {TenantID: "t", RunID: "other", Argv: []string{"true"}},
	} {
		t.Run(name, func(t *testing.T) {
			backend := &securityBackendProbe{}
			authorizer := &sandboxAuthorizer{decision: security.PolicyDecision{Effect: security.Allow}}
			guarded := WithSecurity(backend, authorizer, "production")
			ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t", RunID: "r"})
			if _, err := guarded.Exec(ctx, request); err == nil || backend.execCalls != 0 {
				t.Fatalf("calls=%d err=%v", backend.execCalls, err)
			}
		})
	}
}

func TestBackendSecurityDestroyAndApproval(t *testing.T) {
	backend := &securityBackendProbe{}
	authorizer := &sandboxAuthorizer{decision: security.PolicyDecision{Effect: security.RequireApproval}}
	guarded := WithSecurity(backend, authorizer, "production")
	ctx := tools.WithCallInfo(context.Background(), tools.CallInfo{TenantID: "t", RunID: "r", AttemptID: "a"})
	if _, err := guarded.Exec(ctx, Request{TenantID: "t", RunID: "r", Argv: []string{"true"}}); err == nil || backend.execCalls != 0 {
		t.Fatalf("approval bypass: calls=%d err=%v", backend.execCalls, err)
	}
	authorizer.decision.Effect = security.Allow
	if err := guarded.Destroy(ctx, "t", "r"); err != nil || backend.destroyCalls != 1 {
		t.Fatalf("destroy calls=%d err=%v", backend.destroyCalls, err)
	}
	if err := guarded.Destroy(ctx, "other", "r"); err == nil || backend.destroyCalls != 1 {
		t.Fatal("tenant mismatch reached destroy")
	}
}
