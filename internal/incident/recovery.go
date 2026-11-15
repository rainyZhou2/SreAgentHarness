package incident

import (
	"time"

	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

// RecoveryStatus is orthogonal to investigation and review.
type RecoveryStatus string

const (
	RecoveryImpactActive  RecoveryStatus = "IMPACT_ACTIVE"
	RecoveryMitigating    RecoveryStatus = "MITIGATING"
	RecoveryImpactStopped RecoveryStatus = "IMPACT_STOPPED"
	RecoveryStabilizing   RecoveryStatus = "STABILIZING"
	RecoveryRecovered     RecoveryStatus = "RECOVERED"
)

// OutcomeRef identifies the machine evaluation behind a recovery state.
type OutcomeRef struct {
	WorkID         string          `json:"work_id"`
	EvaluationID   string          `json:"evaluation_id,omitempty"`
	Verdict        outcome.Verdict `json:"verdict"`
	RawVerdict     outcome.Verdict `json:"raw_verdict"`
	EvaluatedAt    time.Time       `json:"evaluated_at"`
	SatisfiedSince *time.Time      `json:"satisfied_since,omitempty"`
	Reasons        []string        `json:"reasons,omitempty"`
}

// RecoveryView is derived only from current machine outcomes.
type RecoveryView struct {
	Status  RecoveryStatus `json:"status"`
	Outcome *OutcomeRef    `json:"outcome,omitempty"`
}

func latestOutcome(evals []outcome.Evaluation, w work.Work) (outcome.Evaluation, bool) {
	if w.Goal.Success == nil {
		return outcome.Evaluation{}, false
	}
	hash := w.Goal.Success.Hash()
	for _, ev := range evals {
		if ev.TenantID == w.TenantID && ev.WorkID == w.ID && ev.Generation == w.Generation && ev.SpecHash == hash {
			return ev, true
		}
	}
	return outcome.Evaluation{}, false
}

func outcomeReference(ev outcome.Evaluation) OutcomeRef {
	return OutcomeRef{WorkID: ev.WorkID, EvaluationID: ev.ID, Verdict: ev.Verdict, RawVerdict: ev.RawVerdict,
		EvaluatedAt: ev.EvaluatedAt, SatisfiedSince: ev.SatisfiedSince, Reasons: append([]string(nil), ev.Reasons...)}
}

func projectRecovery(incident work.Work, children []work.Work, evals []outcome.Evaluation, summaries []ChildView) RecoveryView {
	if ev, ok := latestOutcome(evals, incident); ok {
		ref := outcomeReference(ev)
		view := RecoveryView{Outcome: &ref}
		switch {
		case ev.Verdict == outcome.Satisfied:
			view.Status = RecoveryRecovered
		case ev.Verdict == outcome.Inconclusive && ev.RawVerdict == outcome.Satisfied:
			view.Status = RecoveryStabilizing
		case ev.Verdict == outcome.NotSatisfied:
			view.Status = RecoveryImpactActive
		}
		if view.Status != "" {
			return view
		}
	}
	for _, child := range children {
		if child.Goal.Labels["child_key"] != "mitigation" {
			continue
		}
		for _, summary := range summaries {
			if summary.WorkID != child.ID || summary.Outcome == nil {
				continue
			}
			ev := summary.Outcome
			if ev.RawVerdict == outcome.Satisfied {
				status := RecoveryImpactStopped
				if ev.Verdict == outcome.Inconclusive {
					status = RecoveryStabilizing
				}
				copy := *ev
				return RecoveryView{Status: status, Outcome: &copy}
			}
		}
	}
	for _, child := range children {
		if child.Goal.Labels["child_key"] == "mitigation" && !child.Status.Terminal() {
			return RecoveryView{Status: RecoveryMitigating}
		}
	}
	return RecoveryView{Status: RecoveryImpactActive}
}
