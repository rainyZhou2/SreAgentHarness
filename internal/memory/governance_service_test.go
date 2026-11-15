package memory

import (
	"context"
	"errors"
	"testing"

	"go-sre-harness/internal/domain"
)

type authorityProbe struct {
	decision DecisionEvidence
	outcome  OutcomeEvidence
	err      error
}

func (a authorityProbe) MemoryDecision(context.Context, string, string) (DecisionEvidence, error) {
	return a.decision, a.err
}
func (a authorityProbe) MemoryOutcome(context.Context, string, string) (OutcomeEvidence, error) {
	return a.outcome, a.err
}

type governanceStoreProbe struct {
	memory Memory
	input  VerificationInput
	calls  int
}

func (s *governanceStoreProbe) GetMemory(context.Context, string, string) (Memory, error) {
	return s.memory, nil
}
func (s *governanceStoreProbe) VerifyMemory(_ context.Context, input VerificationInput, _, _ string) (VerificationRecord, Memory, error) {
	s.calls++
	s.input = input
	return VerificationRecord{ID: "v1"}, s.memory, nil
}
func (s *governanceStoreProbe) ApproveMemory(context.Context, string, string, int64, string) (Memory, error) {
	return s.memory, nil
}

func TestGovernanceServiceReloadsAuthorityAndBindsEvidence(t *testing.T) {
	store := &governanceStoreProbe{memory: Memory{ID: "m", TenantID: "t", Version: 2,
		Status: StatusCandidate, EvidenceRefs: []string{"evidence:1", "evidence:2"}}}
	authority := authorityProbe{
		decision: DecisionEvidence{Verdict: "CONFIRMED", WorkID: "w", Generation: 3, EvidenceRefs: []string{"evidence:1"}},
		outcome:  OutcomeEvidence{Verdict: "SATISFIED", WorkID: "w", Generation: 3, EvidenceRefs: []string{"evidence:2"}},
	}
	service := GovernanceService{Store: store, Authority: authority}
	record, _, err := service.Verify(context.Background(), VerificationRequest{TenantID: "t", MemoryID: "m",
		ExpectedVersion: 2, DecisionRef: "decision:w:key", OutcomeRef: "outcome:w:o", Verifier: "verifier", IdempotencyKey: "request"})
	if err != nil || record.ID != "v1" || store.calls != 1 || store.input.DecisionVerdict != "CONFIRMED" {
		t.Fatalf("record=%+v input=%+v calls=%d err=%v", record, store.input, store.calls, err)
	}
	for name, mutate := range map[string]func(*authorityProbe){
		"unconfirmed":          func(a *authorityProbe) { a.decision.Verdict = "PROBABLE" },
		"failed outcome":       func(a *authorityProbe) { a.outcome.Verdict = "NOT_SATISFIED" },
		"different work":       func(a *authorityProbe) { a.outcome.WorkID = "other" },
		"different generation": func(a *authorityProbe) { a.outcome.Generation++ },
		"unaccepted evidence":  func(a *authorityProbe) { a.outcome.EvidenceRefs = nil },
	} {
		t.Run(name, func(t *testing.T) {
			a := authority
			mutate(&a)
			if _, _, err := (GovernanceService{Store: store, Authority: a}).Verify(context.Background(), VerificationRequest{
				TenantID: "t", MemoryID: "m", ExpectedVersion: 2, DecisionRef: "d", OutcomeRef: "o", Verifier: "v", IdempotencyKey: "k",
			}); err == nil {
				t.Fatal("invalid authority result must fail closed")
			}
		})
	}
}

func TestGovernanceServiceRequiresEvidenceAndCurrentVersion(t *testing.T) {
	store := &governanceStoreProbe{memory: Memory{ID: "m", TenantID: "t", Version: 2, Status: StatusCandidate}}
	authority := authorityProbe{decision: DecisionEvidence{Verdict: "CONFIRMED", WorkID: "w", Generation: 1},
		outcome: OutcomeEvidence{Verdict: "SATISFIED", WorkID: "w", Generation: 1}}
	service := GovernanceService{Store: store, Authority: authority}
	_, _, err := service.Verify(context.Background(), VerificationRequest{TenantID: "t", MemoryID: "m", ExpectedVersion: 2,
		DecisionRef: "d", OutcomeRef: "o", Verifier: "v", IdempotencyKey: "k"})
	if err == nil {
		t.Fatal("empty evidence must not be verified")
	}
	_, _, err = service.Verify(context.Background(), VerificationRequest{TenantID: "t", MemoryID: "m", ExpectedVersion: 1,
		DecisionRef: "d", OutcomeRef: "o", Verifier: "v", IdempotencyKey: "k"})
	if !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("stale version error=%v", err)
	}
}
