package memory

import (
	"context"
	"testing"

	"go-sre-harness/internal/security"
)

type governanceProbe struct{ calls int }

func (p *governanceProbe) GetMemory(context.Context, string, string) (Memory, error) {
	return Memory{}, nil
}

func (p *governanceProbe) VerifyMemory(context.Context, VerificationInput, string, string) (VerificationRecord, Memory, error) {
	p.calls++
	return VerificationRecord{}, Memory{}, nil
}

func (p *governanceProbe) ApproveMemory(context.Context, string, string, int64, string) (Memory, error) {
	p.calls++
	return Memory{}, nil
}

func TestGovernanceSecurityUsesPublishPolicy(t *testing.T) {
	probe := &governanceProbe{}
	authorizer := &memoryAuthorizer{decision: security.PolicyDecision{Effect: security.Deny}}
	guarded := WithSecurityGovernanceStore(probe, authorizer)
	ctx := security.WithCaller(context.Background(), security.Caller{TenantID: "t", Subject: "service:verifier"})
	input := VerificationInput{TenantID: "t", MemoryID: "m", ExpectedVersion: 1, InputHash: "sha256:input"}
	if _, _, err := guarded.VerifyMemory(ctx, input, "verifier", "request-1"); err == nil {
		t.Fatal("denied verification reached the store")
	}
	if _, err := guarded.ApproveMemory(ctx, "t", "m", 1, "approver"); err == nil {
		t.Fatal("denied approval reached the store")
	}
	if probe.calls != 0 || len(authorizer.requests) != 2 {
		t.Fatalf("calls=%d requests=%d", probe.calls, len(authorizer.requests))
	}
	for _, request := range authorizer.requests {
		if request.Operation != security.MemoryPublish || request.Risk != "R2" {
			t.Fatalf("request=%+v", request)
		}
	}
}
