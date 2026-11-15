package replay

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/security"
)

type replayAuthorizer struct {
	reads int
}

func (a *replayAuthorizer) Authorize(context.Context, security.Request) (security.PolicyDecision, error) {
	a.reads++
	return security.PolicyDecision{Effect: security.Allow}, nil
}
func (a *replayAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	a.reads++
	return nil
}

func validReplayHash() string { return "sha256:" + strings.Repeat("a", 64) }

func TestGuardRejectsEveryProductionSideEffect(t *testing.T) {
	recorder := NewMemoryRecorder()
	guard := NewGuard(nil, recorder)
	for _, operation := range []security.Operation{security.ToolExecute, security.MCPInvoke, security.SandboxExecute,
		security.ArtifactWrite, security.ArtifactDelete, security.MemoryWrite, security.MemoryPublish, security.ModelEgress} {
		request := security.Request{Operation: operation, TenantID: "t", Subject: "replay", Risk: "R2", InputHash: validReplayHash()}
		if _, err := guard.Authorizer().Authorize(context.Background(), request); !errors.Is(err, ErrReadOnly) {
			t.Fatalf("%s: %v", operation, err)
		}
	}
	if len(recorder.Entries()) != 8 {
		t.Fatalf("entries=%d", len(recorder.Entries()))
	}
}

func TestGuardRecheckAlwaysClosesSideEffects(t *testing.T) {
	guard := NewGuard(nil, NewMemoryRecorder())
	request := security.Request{Operation: security.ArtifactDelete, TenantID: "t", Subject: "replay", Risk: "R2", InputHash: validReplayHash()}
	decision := security.PolicyDecision{Effect: security.Allow, DecisionID: validReplayHash(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := guard.Authorizer().Recheck(context.Background(), request, decision); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("recheck: %v", err)
	}
}

func TestGuardAllowsReadOnly(t *testing.T) {
	reads := &replayAuthorizer{}
	guard := NewGuard(reads, NewMemoryRecorder())
	for _, operation := range []security.Operation{security.ArtifactRead, security.MemoryRead} {
		request := security.Request{Operation: operation, TenantID: "t", Subject: "replay", InputHash: validReplayHash()}
		if _, err := guard.Authorizer().Authorize(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	if reads.reads != 2 {
		t.Fatalf("reads=%d", reads.reads)
	}
}
