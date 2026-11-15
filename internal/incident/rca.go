package incident

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

// RCAStatus describes current causal knowledge, not service health.
type RCAStatus string

const (
	RCANotStarted    RCAStatus = "NOT_STARTED"
	RCAInvestigating RCAStatus = "INVESTIGATING"
	RCAConfirmed     RCAStatus = "CONFIRMED"
	RCAProbable      RCAStatus = "PROBABLE"
	RCAInconclusive  RCAStatus = "INCONCLUSIVE"
	RCAContested     RCAStatus = "CONTESTED"
)

// DecisionRef is a currently valid root-cause adjudication.
type DecisionRef struct {
	WorkID        string    `json:"work_id"`
	HypothesisKey string    `json:"hypothesis_key"`
	Statement     string    `json:"statement"`
	Verdict       string    `json:"verdict"`
	AdjudicatedAt time.Time `json:"adjudicated_at"`
}

// RCAView aggregates all direct RCA children.
type RCAView struct {
	Status         RCAStatus     `json:"status"`
	RootCauseKnown bool          `json:"root_cause_known"`
	Decisions      []DecisionRef `json:"decisions,omitempty"`
	OpenHypotheses int           `json:"open_hypotheses"`
}

func projectRCA(ctx context.Context, store Store, children []work.Work, now time.Time) (RCAView, error) {
	var rcaWorks []work.Work
	for _, child := range children {
		if child.Goal.Labels["child_key"] == "rca" {
			rcaWorks = append(rcaWorks, child)
		}
	}
	if len(rcaWorks) == 0 {
		return RCAView{Status: RCANotStarted}, nil
	}
	out := RCAView{Status: RCAInvestigating}
	confirmed := map[string]DecisionRef{}
	probable, inconclusive, contested, active := false, false, false, false
	for _, child := range rcaWorks {
		if !child.Status.Terminal() {
			active = true
		}
		ledger, err := store.ListEvidence(ctx, child.TenantID, child.ID, evidence.MaxDecisionEvidence)
		if err != nil {
			return RCAView{}, fmt.Errorf("RCA Work %s evidence: %w", child.ID, err)
		}
		hypotheses, err := store.ListHypotheses(ctx, child.TenantID, child.ID)
		if err != nil {
			return RCAView{}, fmt.Errorf("RCA Work %s hypotheses: %w", child.ID, err)
		}
		scope := evidence.DecisionScope{TenantID: child.TenantID, WorkID: child.ID, Generation: child.Generation}
		for _, h := range hypotheses {
			if h.Category != evidence.CatRootCause {
				continue
			}
			if h.Decision == nil || !h.ValidDecisionFor(scope, ledger, now) {
				out.OpenHypotheses++
				inconclusive = true
				continue
			}
			ref := DecisionRef{WorkID: child.ID, HypothesisKey: h.Key, Statement: h.Statement,
				Verdict: h.Decision.Verdict, AdjudicatedAt: h.Decision.AdjudicatedAt}
			out.Decisions = append(out.Decisions, ref)
			switch h.Decision.Verdict {
			case evidence.DecisionConfirmed:
				confirmed[h.Statement] = ref
			case evidence.DecisionProbable:
				probable = true
				out.OpenHypotheses++
			case evidence.DecisionContested:
				contested = true
				out.OpenHypotheses++
			default:
				inconclusive = true
				out.OpenHypotheses++
			}
		}
		if len(hypotheses) == 0 && child.Status.Terminal() {
			inconclusive = true
		}
	}
	sort.Slice(out.Decisions, func(i, j int) bool {
		if out.Decisions[i].Statement == out.Decisions[j].Statement {
			return out.Decisions[i].WorkID < out.Decisions[j].WorkID
		}
		return out.Decisions[i].Statement < out.Decisions[j].Statement
	})
	switch {
	case len(confirmed) > 1 || contested:
		out.Status = RCAContested
	case len(confirmed) == 1:
		out.Status = RCAConfirmed
		out.RootCauseKnown = true
	case probable:
		out.Status = RCAProbable
	case inconclusive || !active:
		out.Status = RCAInconclusive
	}
	return out, nil
}
