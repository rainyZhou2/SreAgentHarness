package security

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type EvaluatorFunc func(context.Context, Request) (PolicyDecision, error)

func (f EvaluatorFunc) Evaluate(ctx context.Context, request Request) (PolicyDecision, error) {
	return f(ctx, request)
}

type emptyRevocations struct{}

func (emptyRevocations) Stamps(context.Context, Request) ([]RevocationStamp, error) { return nil, nil }

type mutableRevocations struct {
	mu     sync.RWMutex
	stamps []RevocationStamp
	err    error
}

func (r *mutableRevocations) Stamps(context.Context, Request) ([]RevocationStamp, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]RevocationStamp(nil), r.stamps...), r.err
}

func (r *mutableRevocations) set(stamps []RevocationStamp) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stamps = stamps
}

type recordCapture struct {
	request  Request
	decision PolicyDecision
	calls    int
}

func (r *recordCapture) RecordDecision(_ context.Context, request Request, decision PolicyDecision) error {
	r.request, r.decision, r.calls = request, decision, r.calls+1
	return nil
}

type failingRecorder struct{}

func (failingRecorder) RecordDecision(context.Context, Request, PolicyDecision) error {
	return errors.New("audit unavailable")
}

type allowEvaluator struct{}

func (allowEvaluator) Evaluate(context.Context, Request) (PolicyDecision, error) {
	return PolicyDecision{Effect: Allow, PolicyVersion: "allow-v1"}, nil
}

func fixedNow() time.Time { return time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC) }

func assertCode(t *testing.T, err error, code ErrorCode) {
	t.Helper()
	var securityErr *Error
	if !errors.As(err, &securityErr) || securityErr.Code != code {
		t.Fatalf("error=%v, want code %s", err, code)
	}
}

func TestAuthorizerModes(t *testing.T) {
	now := fixedNow()
	request := Request{Operation: ArtifactWrite, TenantID: "t", Subject: "agent", ResourceRef: "artifact://a", InputHash: validHash()}
	for _, test := range []struct {
		mode Mode
		want Effect
	}{{ModeLegacy, Allow}, {ModeShadow, Allow}, {ModeEnforce, Deny}} {
		reader := emptyRevocations{}
		service := &Service{Mode: test.mode, Evaluator: EvaluatorFunc(func(context.Context, Request) (PolicyDecision, error) {
			return PolicyDecision{Effect: Deny, PolicyVersion: "v1", Reasons: []Reason{{Code: "test_deny"}}}, nil
		}), Revocations: reader, FreshRevocations: reader, Recorder: &recordCapture{}, Now: func() time.Time { return now }}
		decision, err := service.Authorize(context.Background(), request)
		if err != nil || decision.Effect != test.want || decision.DecisionID == "" || decision.RequestHash == "" {
			t.Fatalf("%s: %+v %v", test.mode, decision, err)
		}
	}
}

func TestAuthorizerAuditFailureClosesSideEffect(t *testing.T) {
	reader := emptyRevocations{}
	service := &Service{Mode: ModeEnforce, Evaluator: allowEvaluator{}, Revocations: reader, FreshRevocations: reader, Recorder: failingRecorder{}, Now: fixedNow}
	_, err := service.Authorize(context.Background(), Request{Operation: ArtifactWrite, TenantID: "t", Subject: "s", InputHash: validHash()})
	assertCode(t, err, PolicyUnavailable)
}

func TestRecheckRevokedExpiredAndTampered(t *testing.T) {
	now := fixedNow()
	reader := &mutableRevocations{}
	request := Request{TenantID: "t", Subject: "s", Operation: ArtifactWrite, InputHash: validHash()}
	service := &Service{Mode: ModeEnforce, Evaluator: allowEvaluator{}, Revocations: reader, FreshRevocations: reader, Recorder: &recordCapture{}, Now: func() time.Time { return now }}
	decision, err := service.Authorize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recheck(context.Background(), request, decision); err != nil {
		t.Fatal(err)
	}
	reader.set([]RevocationStamp{{Scope: "tenant", ScopeID: "t", Version: 1}})
	assertCode(t, service.Recheck(context.Background(), request, decision), Revoked)
	reader.set(nil)
	tampered := decision
	tampered.PolicyVersion = "forged"
	assertCode(t, service.Recheck(context.Background(), request, tampered), PolicyUnavailable)
	foreign := request
	foreign.WorkID = "other"
	assertCode(t, service.Recheck(context.Background(), foreign, decision), InvalidRequest)
	expired := decision
	expired.ExpiresAt = now.Add(-time.Second)
	assertCode(t, service.Recheck(context.Background(), request, expired), DecisionExpired)
}

func TestSideEffectRecheckBypassesCache(t *testing.T) {
	cached := emptyRevocations{}
	fresh := &mutableRevocations{stamps: []RevocationStamp{{Scope: "run", ScopeID: "r", Version: 1}}}
	request := Request{TenantID: "t", Subject: "s", RunID: "r", Operation: ArtifactWrite, InputHash: validHash()}
	service := &Service{Mode: ModeEnforce, Evaluator: allowEvaluator{}, Revocations: cached, FreshRevocations: fresh, Recorder: &recordCapture{}, Now: fixedNow}
	decision, err := service.Authorize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	assertCode(t, service.Recheck(context.Background(), request, decision), Revoked)
}

func TestRecheckApprovalAndDecisionSlices(t *testing.T) {
	reader := emptyRevocations{}
	service := &Service{Mode: ModeEnforce, Evaluator: EvaluatorFunc(func(context.Context, Request) (PolicyDecision, error) {
		return PolicyDecision{Effect: RequireApproval, RequiredApprovals: 2, PolicyVersion: "approval-v1", Reasons: []Reason{{Code: "b"}, {Code: "a"}}}, nil
	}), Revocations: reader, FreshRevocations: reader, Recorder: &recordCapture{}, Now: fixedNow}
	request := Request{TenantID: "t", Subject: "s", Operation: ArtifactWrite, InputHash: validHash()}
	decision, err := service.Authorize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	original := append([]Reason(nil), decision.Reasons...)
	assertCode(t, service.Recheck(context.Background(), request, decision), ApprovalRequired)
	for i := range original {
		if decision.Reasons[i] != original[i] {
			t.Fatal("recheck mutated caller decision")
		}
	}
}
