package artifact

import (
	"context"
	"slices"
	"testing"
	"time"

	"go-sre-harness/internal/security"
)

type artifactAuthorizer struct {
	requests []security.Request
	decision security.PolicyDecision
}

func (a *artifactAuthorizer) Authorize(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	a.requests = append(a.requests, request)
	return a.decision, nil
}
func (a *artifactAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	return nil
}

func TestArtifactSecurityOperations(t *testing.T) {
	store := NewMemory()
	authorizer := &artifactAuthorizer{decision: security.PolicyDecision{Effect: security.Allow, ExpiresAt: time.Now().Add(time.Minute)}}
	guarded := WithSecurity(store, authorizer)
	ctx := security.WithCaller(context.Background(), security.Caller{TenantID: "t", Subject: "work:w", WorkID: "w", RunID: "r"})
	content := []byte(`{"content":"classified"}`)
	ref, _, err := guarded.Put(ctx, "t", "memory", content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = guarded.Get(ctx, "t", ref); err != nil {
		t.Fatal(err)
	}
	if err = guarded.(Deleter).Delete(ctx, "t", ref); err != nil {
		t.Fatal(err)
	}
	got := []security.Operation{authorizer.requests[0].Operation, authorizer.requests[1].Operation, authorizer.requests[2].Operation}
	want := []security.Operation{security.ArtifactWrite, security.ArtifactRead, security.ArtifactDelete}
	if !slices.Equal(got, want) {
		t.Fatalf("operations=%v", got)
	}
	for _, request := range authorizer.requests {
		if len(request.Input) != 0 || request.InputHash == "" {
			t.Fatalf("sensitive content entered request: %+v", request)
		}
	}
}

func TestArtifactSecurityRejectsWrongTenant(t *testing.T) {
	store := NewMemory()
	authorizer := &artifactAuthorizer{decision: security.PolicyDecision{Effect: security.Allow}}
	guarded := WithSecurity(store, authorizer)
	ctx := security.WithCaller(context.Background(), security.Caller{TenantID: "t", Subject: "reviewer"})
	if _, _, err := guarded.Put(ctx, "other", "memory", []byte("secret")); err == nil {
		t.Fatal("cross-tenant write was accepted")
	}
	if len(authorizer.requests) != 0 {
		t.Fatal("cross-tenant request reached the authorizer")
	}
}
