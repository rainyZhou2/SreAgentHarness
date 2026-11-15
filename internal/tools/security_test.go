package tools

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/security"
)

type recordingAuthorizer struct {
	request    security.Request
	decision   security.PolicyDecision
	rechecks   int
	recheckErr error
}

func (a *recordingAuthorizer) Authorize(_ context.Context, request security.Request) (security.PolicyDecision, error) {
	a.request = request
	return a.decision, nil
}

func (a *recordingAuthorizer) Recheck(_ context.Context, _ security.Request, _ security.PolicyDecision) error {
	a.rechecks++
	return a.recheckErr
}

type toolStub struct {
	desc  Descriptor
	calls int
}

func (s *toolStub) Descriptor() Descriptor { return s.desc }
func (s *toolStub) Invoke(context.Context, string, json.RawMessage) (json.RawMessage, error) {
	s.calls++
	return json.RawMessage(`{"ok":true}`), nil
}

func TestGuardedToolOperationAndRecheck(t *testing.T) {
	descriptor := Descriptor{Name: "mcp.ops.restart", Version: "1", Risk: domain.RiskR1, Source: "mcp:ops", TargetArg: "service"}
	call := CallInfo{TenantID: "t", RunID: "r", WorkID: "w", Subject: "work:w", StepID: "s", AttemptID: "a", ActionID: "action-1"}
	ctx := security.WithDecisionSlot(WithCallInfo(context.Background(), call))
	authorizer := &recordingAuthorizer{decision: security.PolicyDecision{Effect: security.Allow, ExpiresAt: time.Now().Add(time.Minute)}}
	stub := &toolStub{desc: descriptor}
	tool := WithSecurity(stub, authorizer, "production")
	if _, err := tool.Invoke(ctx, "idem", json.RawMessage(`{"service":"api"}`)); err != nil {
		t.Fatal(err)
	}
	if authorizer.request.Operation != security.MCPInvoke || authorizer.request.WorkID != "w" || authorizer.request.Subject != "work:w" || authorizer.request.InputHash == "" || authorizer.request.ActionID != "action-1" {
		t.Fatalf("request: %+v", authorizer.request)
	}
	if authorizer.rechecks != 1 || stub.calls != 1 {
		t.Fatalf("rechecks=%d calls=%d", authorizer.rechecks, stub.calls)
	}
	if binding, ok := security.DecisionFrom(ctx); !ok || binding.RequestID != authorizer.request.RequestID {
		t.Fatalf("binding=%+v ok=%v", binding, ok)
	}
}

func TestAuthorizeConfirmedCallDoesNotInvokeTool(t *testing.T) {
	authorizer := &recordingAuthorizer{decision: security.PolicyDecision{Effect: security.Allow, DecisionID: "decision-1"}}
	stub := &toolStub{desc: Descriptor{Name: "svc.restart", Version: "1", Risk: domain.RiskR1, TargetArg: "service"}}
	tool := WithSecurity(stub, authorizer, "production")
	ctx := security.WithDecisionSlot(WithCallInfo(context.Background(), CallInfo{TenantID: "t", RunID: "r", AttemptID: "a", ActionID: "action-1"}))
	binding, ok, err := AuthorizeCall(ctx, tool, json.RawMessage(`{"service":"api"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || binding.DecisionID != "decision-1" || authorizer.rechecks != 1 || stub.calls != 0 {
		t.Fatalf("binding=%+v supported=%v rechecks=%d calls=%d", binding, ok, authorizer.rechecks, stub.calls)
	}
}

type enforcedEvaluator struct{}

func (enforcedEvaluator) Evaluate(context.Context, security.Request) (security.PolicyDecision, error) {
	return security.PolicyDecision{Effect: security.Allow, PolicyVersion: "test"}, nil
}

type enforcedRevocations struct{}

func (enforcedRevocations) Stamps(context.Context, security.Request) ([]security.RevocationStamp, error) {
	return nil, nil
}

type enforcedRecorder struct{}

func (enforcedRecorder) RecordDecision(context.Context, security.Request, security.PolicyDecision) error {
	return nil
}

func TestGuardedToolEnforceBindsNormalizedRequest(t *testing.T) {
	now := time.Now().UTC()
	service := &security.Service{Mode: security.ModeEnforce, Evaluator: enforcedEvaluator{},
		Revocations: enforcedRevocations{}, FreshRevocations: enforcedRevocations{}, Recorder: enforcedRecorder{},
		Now: func() time.Time { return now }}
	stub := &toolStub{desc: Descriptor{Name: "svc.restart", Version: "1", Risk: domain.RiskR1}}
	tool := WithSecurity(stub, service, "production")
	ctx := security.WithDecisionSlot(WithCallInfo(context.Background(), CallInfo{TenantID: "t", RunID: "r", AttemptID: "a"}))
	if _, err := tool.Invoke(ctx, "idem", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if binding, ok := security.DecisionFrom(ctx); !ok || binding.DecisionID == "" || stub.calls != 1 {
		t.Fatalf("binding=%+v ok=%v calls=%d", binding, ok, stub.calls)
	}
}

func TestGuardedToolRevocationAndContext(t *testing.T) {
	authorizer := &recordingAuthorizer{decision: security.PolicyDecision{Effect: security.Allow}, recheckErr: &security.Error{Code: security.Revoked}}
	stub := &toolStub{desc: Descriptor{Name: "svc.restart", Risk: domain.RiskR1, TargetArg: "service"}}
	tool := WithSecurity(stub, authorizer, "production")
	ctx := WithCallInfo(context.Background(), CallInfo{TenantID: "t", RunID: "r"})
	if _, err := tool.Invoke(ctx, "idem", json.RawMessage(`{"service":"api"}`)); err == nil || stub.calls != 0 {
		t.Fatalf("revocation reached tool: calls=%d err=%v", stub.calls, err)
	}
	if _, err := tool.Invoke(context.Background(), "idem", json.RawMessage(`{}`)); err == nil {
		t.Fatal("missing authenticated context was accepted")
	}
	authorizer.recheckErr = nil
	authorizer.decision.Effect = security.Deny
	if _, err := tool.Invoke(ctx, "idem", json.RawMessage(`{"service":"api"}`)); err == nil || stub.calls != 0 {
		t.Fatal("denied decision reached tool")
	}
	var securityErr *security.Error
	_, err := tool.Invoke(ctx, "idem", json.RawMessage(`{"service":"api"}`))
	if !errors.As(err, &securityErr) {
		t.Fatalf("error type=%T", err)
	}
}
