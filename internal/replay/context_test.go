package replay

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/security"
)

func TestProjectionContextAuthorizerLifecycle(t *testing.T) {
	reads := &replayAuthorizer{}
	guard := NewGuard(reads, NewMemoryRecorder())
	ctx, projection, err := guard.Context(context.Background(), "work.view")
	if err != nil {
		t.Fatal(err)
	}
	authorizer, err := AuthorizerFromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	read := security.Request{Operation: security.MemoryRead, TenantID: "t", Subject: "replay", InputHash: validReplayHash()}
	if _, err := authorizer.Authorize(ctx, read); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []security.Operation{security.ToolExecute, security.MCPInvoke, security.SandboxExecute,
		security.ArtifactWrite, security.ArtifactDelete, security.MemoryWrite, security.MemoryPublish, security.ModelEgress} {
		request := security.Request{Operation: operation, TenantID: "t", Subject: "replay", Risk: "R2", InputHash: validReplayHash()}
		if _, err := authorizer.Authorize(ctx, request); !errors.Is(err, ErrReadOnly) {
			t.Fatalf("%s err=%v", operation, err)
		}
	}
	decision := security.PolicyDecision{Effect: security.Allow, DecisionID: validReplayHash(), ExpiresAt: time.Now().Add(time.Hour)}
	if err := authorizer.Recheck(ctx, read, decision); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("recheck err=%v", err)
	}
	if reads.reads != 1 {
		t.Fatalf("read calls=%d", reads.reads)
	}
	projection.End(nil)
	if _, err := AuthorizerFromContext(ctx); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ended context err=%v", err)
	}
	if _, err := authorizer.Authorize(ctx, read); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("ended authorizer err=%v", err)
	}
}

func TestProjectionContextRejectsMissingUnknownAndNested(t *testing.T) {
	guard := NewGuard(nil, NewMemoryRecorder())
	if _, err := AuthorizerFromContext(context.Background()); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("missing projection err=%v", err)
	}
	if _, _, err := guard.Context(context.Background(), "unknown"); err == nil {
		t.Fatal("unknown projection accepted")
	}
	ctx, projection, err := guard.Context(context.Background(), "work.view")
	if err != nil {
		t.Fatal(err)
	}
	defer projection.End(nil)
	if _, _, err := guard.Context(ctx, "incident.view"); err == nil {
		t.Fatal("nested projection accepted")
	}
	if _, err := Project(ctx, guard, "decision.v3", struct{}{}, func(context.Context, struct{}) (struct{}, error) {
		return struct{}{}, nil
	}); err == nil {
		t.Fatal("nested Project accepted")
	}
}

func TestProjectionContextsAreIndependentAcrossGoroutines(t *testing.T) {
	guard := NewGuard(&replayAuthorizer{}, NewMemoryRecorder())
	ctx1, first, err := guard.Context(context.Background(), "work.view")
	if err != nil {
		t.Fatal(err)
	}
	ctx2, second, err := guard.Context(context.Background(), "incident.view")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		first.End(nil)
	}()
	go func() {
		defer wg.Done()
		if _, err := AuthorizerFromContext(ctx2); err != nil {
			t.Error(err)
		}
		second.End(nil)
	}()
	wg.Wait()
	if _, err := AuthorizerFromContext(ctx1); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("first context err=%v", err)
	}
}
