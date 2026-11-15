package cpsql

import (
	"context"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func memoryAuthorityRef(kind, ref string) (string, string, error) {
	parts := strings.Split(ref, ":")
	if len(parts) != 3 || parts[0] != kind || parts[1] == "" || parts[2] == "" {
		return "", "", &domain.RejectedError{Reason: "invalid_" + kind + "_ref"}
	}
	return parts[1], parts[2], nil
}

func (s *Store) MemoryDecision(ctx context.Context, tenantID, ref string) (memory.DecisionEvidence, error) {
	workID, key, err := memoryAuthorityRef("decision", ref)
	if err != nil {
		return memory.DecisionEvidence{}, err
	}
	hypotheses, err := s.ListHypotheses(ctx, tenantID, workID)
	if err != nil {
		return memory.DecisionEvidence{}, err
	}
	for _, hypothesis := range hypotheses {
		if hypothesis.Key != key || hypothesis.Decision == nil || !hypothesis.CurrentDecisionFor(hypothesis.Decision.DecisionScope) {
			continue
		}
		decision := hypothesis.Decision
		return memory.DecisionEvidence{Verdict: decision.Verdict, WorkID: decision.WorkID,
			Generation: decision.Generation, EvidenceRefs: append([]string(nil), decision.AcceptedEvidenceRefs...)}, nil
	}
	return memory.DecisionEvidence{}, domain.ErrNotFound
}

func (s *Store) MemoryOutcome(ctx context.Context, tenantID, ref string) (memory.OutcomeEvidence, error) {
	workID, id, err := memoryAuthorityRef("outcome", ref)
	if err != nil {
		return memory.OutcomeEvidence{}, err
	}
	outcomes, err := s.ListOutcomes(ctx, tenantID, workID, 500)
	if err != nil {
		return memory.OutcomeEvidence{}, err
	}
	for _, evaluation := range outcomes {
		if evaluation.ID != id || evaluation.TenantID != tenantID || evaluation.WorkID != workID || evaluation.Generation < 1 {
			continue
		}
		return memory.OutcomeEvidence{Verdict: string(evaluation.Verdict), WorkID: evaluation.WorkID,
			Generation: evaluation.Generation, EvidenceRefs: append([]string(nil), evaluation.EvidenceRefs...)}, nil
	}
	return memory.OutcomeEvidence{}, domain.ErrNotFound
}
