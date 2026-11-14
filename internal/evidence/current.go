package evidence

import (
	"time"

	"go-sre-harness/internal/cpkit"
)

// MaxDecisionEvidence bounds a complete ledger used for adjudication. Reaching
// the read limit is ambiguous and must fail closed, never accept a partial view.
const MaxDecisionEvidence = 1000

// ValidDecisionFor checks a historical verdict against the current ledger.
// Offline replay remains independent of later retractions and supersession.
func (h Hypothesis) ValidDecisionFor(scope DecisionScope, ledger []Record, at time.Time) bool {
	if len(ledger) >= MaxDecisionEvidence || !h.CurrentDecisionFor(scope) || at.IsZero() || at.Before(h.Decision.AdjudicatedAt) {
		return false
	}
	d := h.Decision
	input := decisionInput(scope, h.Key, d.Input.Results, ledger, at)
	// Only the observation horizon changes. New relevant evidence invalidates
	// the old decision; readers never manufacture a replacement verdict.
	input.At = d.AdjudicatedAt
	digest, err := cpkit.Hash(input)
	return err == nil && digest == d.InputDigest
}
