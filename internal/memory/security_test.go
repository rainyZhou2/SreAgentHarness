package memory

import (
	"context"
	"testing"
	"time"

	"go-sre-harness/internal/security"
)

type memoryWriterProbe struct {
	calls int
}

func (w *memoryWriterProbe) CreateMemory(context.Context, NewMemory) (Memory, bool, error) {
	w.calls++
	return Memory{}, true, nil
}
func (w *memoryWriterProbe) TransitionMemory(context.Context, string, string, string, string, string, string) (Memory, error) {
	w.calls++
	return Memory{}, nil
}

type memoryAuthorizer struct {
	requests []security.Request
	decision security.PolicyDecision
}

func (a *memoryAuthorizer) Authorize(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	a.requests = append(a.requests, request)
	return a.decision, nil
}
func (a *memoryAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	return nil
}

func TestMemoryPublishRequiresFreshDecision(t *testing.T) {
	writer := &memoryWriterProbe{}
	authorizer := &memoryAuthorizer{decision: security.PolicyDecision{Effect: security.RequireApproval, RequiredApprovals: 1, ExpiresAt: time.Now().Add(time.Minute)}}
	guarded := WithSecurityWriter(writer, authorizer)
	ctx := security.WithCaller(context.Background(), security.Caller{TenantID: "t", Subject: "reviewer"})
	_, _, err := guarded.CreateMemory(ctx, NewMemory{TenantID: "t", Status: Approved, Sensitivity: "restricted", Content: "classified"})
	if err == nil || writer.calls != 0 {
		t.Fatalf("publication without an approval binding: calls=%d err=%v", writer.calls, err)
	}
	if authorizer.requests[0].Operation != security.MemoryPublish || len(authorizer.requests[0].Input) != 0 {
		t.Fatalf("request=%+v", authorizer.requests[0])
	}
	authorizer.decision.Effect = security.Allow
	if _, err := guarded.TransitionMemory(ctx, "t", "m1", Verified, Approved, "reviewer", "verified"); err != nil {
		t.Fatal(err)
	}
	if writer.calls != 1 || authorizer.requests[1].Operation != security.MemoryPublish {
		t.Fatalf("calls=%d requests=%+v", writer.calls, authorizer.requests)
	}
}

func TestMemoryReadSecurityMappings(t *testing.T) {
	authorizer := &memoryAuthorizer{decision: security.PolicyDecision{Effect: security.Deny}}
	retriever := WithSecurityRetriever(nil, authorizer)
	ctx := security.WithCaller(context.Background(), security.Caller{TenantID: "t", Subject: "run:r", RunID: "r"})
	if _, err := retriever.Search(ctx, "t", "r", "sensitive query", nil, 1); err == nil {
		t.Fatal("denied search was accepted")
	}
	if _, err := retriever.PlanMemories(ctx, "t", "sensitive goal", nil, 1); err == nil {
		t.Fatal("denied plan read was accepted")
	}
	for _, request := range authorizer.requests {
		if request.Operation != security.MemoryRead || len(request.Input) != 0 {
			t.Fatalf("request=%+v", request)
		}
	}
}
