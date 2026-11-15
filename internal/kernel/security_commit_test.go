package kernel_test

import (
	"context"
	"sync"
	"testing"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/security"
	"go-sre-harness/internal/tools"
)

type kernelSecurityAuthorizer struct {
	mu      sync.Mutex
	request security.Request
	legacy  security.Service
}

func (a *kernelSecurityAuthorizer) Authorize(ctx context.Context, request security.Request) (security.PolicyDecision, error) {
	a.mu.Lock()
	a.request = request
	a.mu.Unlock()
	a.legacy.Mode = security.ModeLegacy
	return a.legacy.Authorize(ctx, request)
}

func (a *kernelSecurityAuthorizer) Recheck(context.Context, security.Request, security.PolicyDecision) error {
	return nil
}

type securityCommitCapture struct {
	kernel.StateStore
	lease  domain.Lease
	result domain.StepSuccess
}

func (s *securityCommitCapture) CommitSuccess(ctx context.Context, lease domain.Lease, result domain.StepSuccess) error {
	if lease.Kind == domain.StepTool {
		s.lease, s.result = lease, result
	}
	return s.StateStore.CommitSuccess(ctx, lease, result)
}

func TestWorkerBindsAuthorizedDecisionToSideEffectCommit(t *testing.T) {
	var calls int32
	authorizer := &kernelSecurityAuthorizer{}
	tool := tools.WithSecurity(fixedTool{name: "deploy.restart", risk: domain.RiskR1, out: `{"restarted":true}`, calls: &calls}, authorizer, "staging")
	agent := &scripted{decide: func(n int, _ cognitive.ReasoningInput) (domain.Decision, error) {
		if n == 0 {
			return toolDecision("deploy.restart", `{"service":"checkout"}`), nil
		}
		return finalDecision("done"), nil
	}}
	env := newLoopEnvWith(t, kernel.Config{}, agent, []tools.Tool{tool}, false)
	capture := &securityCommitCapture{StateStore: env.st}
	env.w = kernel.NewWorker(kernel.Config{ID: "w1", Rules: &profile.RulesFor{Loader: env.st}}, capture, env.arts, agent,
		tools.NewRegistry(tool, kernel.BatchTool{}), policy.Engine{Environment: "staging", AllowR1WithoutApproval: true}, nil)
	for i := 0; i < 2; i++ {
		if err := env.w.ExecuteOne(context.Background(), env.run.ID); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 || capture.result.SecurityDecisionID == "" {
		t.Fatalf("calls=%d decision=%q", calls, capture.result.SecurityDecisionID)
	}
	authorizer.mu.Lock()
	request := authorizer.request
	authorizer.mu.Unlock()
	if request.ActionID != capture.lease.Intent.ID || request.InputHash != capture.lease.Intent.InputHash ||
		request.RunID != capture.lease.RunID || request.Subject != capture.lease.Subject || request.Environment != capture.lease.Environment {
		t.Fatalf("request=%+v lease=%+v", request, capture.lease)
	}
}
