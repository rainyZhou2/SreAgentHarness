package memory

import (
	"context"

	"go-sre-harness/internal/domain"
)

type DecisionEvidence struct {
	Verdict      string
	WorkID       string
	Generation   int
	EvidenceRefs []string
}

type OutcomeEvidence struct {
	Verdict      string
	WorkID       string
	Generation   int
	EvidenceRefs []string
}

type VerificationAuthority interface {
	MemoryDecision(context.Context, string, string) (DecisionEvidence, error)
	MemoryOutcome(context.Context, string, string) (OutcomeEvidence, error)
}

type GovernanceStore interface {
	GetMemory(context.Context, string, string) (Memory, error)
	VerifyMemory(context.Context, VerificationInput, string, string) (VerificationRecord, Memory, error)
	ApproveMemory(context.Context, string, string, int64, string) (Memory, error)
}

type VerificationRequest struct {
	TenantID        string
	MemoryID        string
	ExpectedVersion int64
	DecisionRef     string
	OutcomeRef      string
	Verifier        string
	IdempotencyKey  string
}

type GovernanceService struct {
	Store     GovernanceStore
	Authority VerificationAuthority
}

func (s GovernanceService) Verify(ctx context.Context, request VerificationRequest) (VerificationRecord, Memory, error) {
	if s.Store == nil || s.Authority == nil || request.Verifier == "" || request.IdempotencyKey == "" {
		return VerificationRecord{}, Memory{}, &domain.RejectedError{Reason: "verification_service_unavailable"}
	}
	current, err := s.Store.GetMemory(ctx, request.TenantID, request.MemoryID)
	if err != nil {
		return VerificationRecord{}, Memory{}, err
	}
	if current.Version != request.ExpectedVersion {
		return VerificationRecord{}, Memory{}, domain.ErrVersionConflict
	}
	if len(current.EvidenceRefs) == 0 {
		return VerificationRecord{}, Memory{}, &domain.RejectedError{Reason: "verification_evidence_required"}
	}
	decision, err := s.Authority.MemoryDecision(ctx, request.TenantID, request.DecisionRef)
	if err != nil {
		return VerificationRecord{}, Memory{}, err
	}
	outcome, err := s.Authority.MemoryOutcome(ctx, request.TenantID, request.OutcomeRef)
	if err != nil {
		return VerificationRecord{}, Memory{}, err
	}
	if decision.Verdict != "CONFIRMED" || outcome.Verdict != "SATISFIED" {
		return VerificationRecord{}, Memory{}, &domain.RejectedError{Reason: "verification_verdict_not_satisfied"}
	}
	if decision.WorkID == "" || decision.WorkID != outcome.WorkID || decision.Generation < 1 || decision.Generation != outcome.Generation {
		return VerificationRecord{}, Memory{}, &domain.RejectedError{Reason: "verification_work_mismatch"}
	}
	accepted := make(map[string]bool, len(decision.EvidenceRefs)+len(outcome.EvidenceRefs))
	for _, ref := range append(append([]string(nil), decision.EvidenceRefs...), outcome.EvidenceRefs...) {
		accepted[ref] = true
	}
	for _, ref := range current.EvidenceRefs {
		if !accepted[ref] {
			return VerificationRecord{}, Memory{}, &domain.RejectedError{Reason: "verification_evidence_not_accepted"}
		}
	}
	input := VerificationInputFor(current, request.DecisionRef, request.OutcomeRef)
	return s.Store.VerifyMemory(ctx, input, request.Verifier, request.IdempotencyKey)
}
