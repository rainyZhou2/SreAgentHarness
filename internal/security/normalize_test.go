package security

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRequestNormalize(t *testing.T) {
	now := time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)
	r, err := (Request{RequestID: " req-1 ", Operation: ToolExecute, TenantID: " tenant-a ",
		Subject: " agent:run-1 ", ResourceRef: " service://prod/api ", Environment: " production ",
		DataClass: " internal ", Risk: "R0", InputHash: validHash(), OccurredAt: now}).Normalize(now)
	if err != nil || r.RequestID != "req-1" || r.TenantID != "tenant-a" || !r.OccurredAt.Equal(now) {
		t.Fatalf("normalize: %+v %v", r, err)
	}
	for _, bad := range []Request{
		{Operation: "unknown", TenantID: "t", Subject: "s", InputHash: validHash()},
		{Operation: ToolExecute, TenantID: "t", Subject: "s", InputHash: "sha256:bad"},
		{Operation: ToolExecute, TenantID: "t", Subject: "s", InputHash: validHash(), RequestID: strings.Repeat("x", 201)},
		{Operation: ToolExecute, TenantID: "t", Subject: "s", InputHash: validHash(), Attributes: map[string]string{"secret": strings.Repeat("x", 257)}},
	} {
		if _, err := bad.Normalize(now); err == nil {
			t.Fatalf("accepted invalid request: %+v", bad)
		}
	}
}

func TestOperationSideEffects(t *testing.T) {
	if (Request{Operation: ToolExecute, Risk: "R0"}).SideEffecting() {
		t.Fatal("R0 tool must be read-only")
	}
	if !(Request{Operation: ToolExecute, Risk: "R1"}).SideEffecting() {
		t.Fatal("R1 tool must be side-effecting")
	}
	if (Request{Operation: ArtifactRead}).SideEffecting() {
		t.Fatal("artifact read must be read-only")
	}
	for _, op := range []Operation{SandboxExecute, ArtifactWrite, ArtifactDelete, MemoryWrite, MemoryPublish, ModelEgress} {
		if !(Request{Operation: op}).SideEffecting() {
			t.Fatalf("%s must be side-effecting", op)
		}
	}
}

func TestSecurityError(t *testing.T) {
	cause := errors.New("provider detail")
	err := &Error{Code: PolicyUnavailable, Message: "policy unavailable", Err: cause}
	if err.Error() != "security_policy_unavailable: policy unavailable" || !errors.Is(err, cause) {
		t.Fatalf("unexpected error contract: %v", err)
	}
}

func validHash() string { return "sha256:" + strings.Repeat("a", 64) }
