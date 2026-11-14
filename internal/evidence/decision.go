package evidence

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

const (
	DecisionConfirmed    = "CONFIRMED"
	DecisionProbable     = "PROBABLE"
	DecisionInconclusive = "INCONCLUSIVE"
	DecisionContested    = "CONTESTED"
	DecisionProfile      = "sre.rca"
	AdjudicatorVersion   = "1"
)

// ErrDecisionLimit requires a smaller investigation before durable adjudication.
var ErrDecisionLimit = fmt.Errorf("%w: adjudication resource limit", ErrInvalid)

// DecisionScope binds a conclusion to the goal generation it investigated.
type DecisionScope struct {
	TenantID   string `json:"tenant_id"`
	WorkID     string `json:"work_id"`
	Generation int    `json:"generation"`
}

// DecisionVerdict is the adjudicator's persisted conclusion, never an agent vote.
type DecisionVerdict struct {
	DecisionScope
	ClaimedSupport bool           `json:"claimed_support,omitempty"`
	Input          *DecisionInput `json:"input,omitempty"`

	DecisionKey             string    `json:"decision_key"`
	Profile                 string    `json:"profile"`
	ProfileVersion          int       `json:"profile_version"`
	Verdict                 string    `json:"verdict"`
	AcceptedEvidenceRefs    []string  `json:"accepted_evidence_refs,omitempty"`
	RejectedEvidenceRefs    []string  `json:"rejected_evidence_refs,omitempty"`
	IndependentSupportCount int       `json:"independent_support_count"`
	IndependentRefuteCount  int       `json:"independent_refute_count"`
	UnresolvedConflicts     []string  `json:"unresolved_conflicts,omitempty"`
	RequiredNextChecks      []string  `json:"required_next_checks,omitempty"`
	InputDigest             string    `json:"input_digest"`
	AdjudicatorVersion      string    `json:"adjudicator_version"`
	AdjudicatedAt           time.Time `json:"adjudicated_at"`
}

// CurrentFor rejects legacy, foreign and obsolete decisions at reader boundaries.
func (d *DecisionVerdict) CurrentFor(scope DecisionScope, key string) bool {
	if scope.TenantID == "" || scope.WorkID == "" || scope.Generation < 1 || key == "" || d == nil || d.DecisionScope != scope || d.DecisionKey != key || d.Profile != DecisionProfile || d.ProfileVersion != 1 || d.AdjudicatorVersion != AdjudicatorVersion || d.InputDigest == "" || d.AdjudicatedAt.IsZero() {
		return false
	}
	switch d.Verdict {
	case DecisionConfirmed, DecisionProbable:
		return d.IndependentSupportCount > 0 && d.IndependentRefuteCount == 0 && len(d.UnresolvedConflicts) == 0 && len(d.AcceptedEvidenceRefs) > 0
	case DecisionInconclusive:
		return d.IndependentSupportCount == 0
	case DecisionContested:
		return d.IndependentSupportCount > 0 && d.IndependentRefuteCount > 0 && len(d.UnresolvedConflicts) > 0
	}
	return false
}

// HypothesisStatus maps the decision to the legacy hypothesis vocabulary.
func (d *DecisionVerdict) HypothesisStatus() string {
	if d == nil {
		return HypInconclusive
	}
	switch d.Verdict {
	case DecisionConfirmed, DecisionProbable:
		return HypSupported
	case DecisionContested:
		return HypChallenged
	case DecisionInconclusive:
		if d.IndependentRefuteCount > 0 && d.IndependentSupportCount == 0 {
			return HypRefuted
		}
	}
	return HypInconclusive
}

// Adjudicate resolves references before synthesis and derives the single downstream verdict.
func Adjudicate(scope DecisionScope, envs []ResultEnvelope, ledger []Record, at time.Time) (Synthesis, []Hypothesis, error) {
	if scope.TenantID == "" || scope.WorkID == "" || scope.Generation < 1 || at.IsZero() {
		return Synthesis{}, nil, fmt.Errorf("%w: decision scope and time required", ErrInvalid)
	}
	syn := synthesize(scope, envs, ledger, at)
	resolver := newReferenceResolverAt(scope, ledger, at)
	byKey := map[string]*Hypothesis{}
	for _, env := range envs {
		if env.Status != "COMPLETED" {
			continue
		}
		for _, h := range env.Hypotheses {
			key := HypothesisKey(h.Statement)
			if byKey[key] == nil {
				byKey[key] = &Hypothesis{TenantID: scope.TenantID, WorkID: scope.WorkID, Key: key, Statement: cpkit.Truncate(h.Statement, MaxStatement), Category: h.Category, UpdatedAt: at}
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var out []Hypothesis
	for _, key := range keys {
		h := byKey[key]
		d := &DecisionVerdict{DecisionScope: scope, DecisionKey: key, Profile: DecisionProfile, ProfileVersion: 1, Verdict: DecisionInconclusive, AdjudicatorVersion: AdjudicatorVersion, AdjudicatedAt: at}
		supports, refutes := map[string]bool{}, map[string]bool{}
		causal := false

		for _, f := range syn.Findings {
			if f.HypothesisID != key {
				continue
			}
			d.ClaimedSupport = d.ClaimedSupport || f.Claimed == "supported" || f.Claimed == "verified"
			h.Proponents = appendUnique(h.Proponents, f.Role)
			for _, ref := range f.Supports {
				h.Supporting = appendUnique(h.Supporting, ref)
				d.AcceptedEvidenceRefs = appendUnique(d.AcceptedEvidenceRefs, ref)
				r, err := resolver.Resolve(ref, f.RunID)
				if err != nil {
					return syn, nil, err
				}
				if r.Relation == RelSupports || r.Relation == RelCausedBy {
					supports[r.Group()] = true
				}
				causal = causal || r.Relation == RelCausedBy
			}
			for _, ref := range f.Refutes {
				h.Refuting = appendUnique(h.Refuting, ref)
				d.AcceptedEvidenceRefs = appendUnique(d.AcceptedEvidenceRefs, ref)
				r, err := resolver.Resolve(ref, f.RunID)
				if err != nil {
					return syn, nil, err
				}
				refutes[r.Group()] = true
			}
			for _, ref := range f.Rejected {
				d.RejectedEvidenceRefs = appendUnique(d.RejectedEvidenceRefs, ref)
			}
			for _, check := range f.NextChecks {
				d.RequiredNextChecks = appendUnique(d.RequiredNextChecks, check)
			}
			for _, gap := range f.Gaps {
				d.RequiredNextChecks = appendUnique(d.RequiredNextChecks, gap)
			}
		}
		d.IndependentSupportCount, d.IndependentRefuteCount = len(supports), len(refutes)
		switch {
		case len(supports) > 0 && len(refutes) > 0:
			d.Verdict = DecisionContested
			d.UnresolvedConflicts = []string{key}
			d.RequiredNextChecks = appendUnique(d.RequiredNextChecks, fmt.Sprintf("distinguish %q with an independent observation", h.Statement))
		case len(refutes) > 0:
			d.Verdict = DecisionInconclusive
		case len(supports) > 0 && causal:
			d.Verdict = DecisionConfirmed
		case len(supports) > 0:
			d.Verdict = DecisionProbable
		}
		if d.Verdict != DecisionConfirmed && len(d.RequiredNextChecks) == 0 {
			d.RequiredNextChecks = []string{"collect fresh independent causal evidence and test alternatives"}
		}
		for _, xs := range [][]string{h.Supporting, h.Refuting, h.Proponents, d.AcceptedEvidenceRefs, d.RejectedEvidenceRefs, d.RequiredNextChecks} {
			sort.Strings(xs)
		}

		input := decisionInput(scope, key, envs, ledger, at)
		d.Input = &input
		var err error
		d.InputDigest, err = cpkit.Hash(input)
		if err != nil {
			return syn, nil, fmt.Errorf("%w: decision digest: %v", ErrInvalid, err)
		}

		h.Decision = d
		h.Status = d.HypothesisStatus()
		out = append(out, *h)
	}
	if err := ValidateDecisionBatch(out); err != nil {
		return syn, nil, err
	}
	return syn, out, nil
}

// ValidateDecisionBatch reserves space for the Work audit event envelope.
func ValidateDecisionBatch(hypotheses []Hypothesis) error {
	decisions := make([]*DecisionVerdict, 0, len(hypotheses))
	for _, h := range hypotheses {
		decisions = append(decisions, h.Decision)
	}
	raw, err := json.Marshal(decisions)
	if err != nil {
		return fmt.Errorf("%w: decision audit payload: %v", ErrInvalid, err)
	}
	if len(raw) > 48<<10 {
		return fmt.Errorf("%w: decision audit batch exceeds 48 KiB", ErrDecisionLimit)
	}
	return nil
}

// DecisionInput preserves bounded immutable adjudication inputs for offline replay.
type DecisionInput struct {
	Scope    DecisionScope    `json:"scope"`
	At       time.Time        `json:"at"`
	Results  []ResultEnvelope `json:"results"`
	Evidence []Record         `json:"evidence"`
}

func decisionInput(scope DecisionScope, key string, envs []ResultEnvelope, ledger []Record, at time.Time) DecisionInput {
	in := DecisionInput{Scope: scope, At: at}
	aliases := map[string]bool{}
	for _, env := range envs {
		if env.Status != "COMPLETED" {
			continue
		}
		e := ResultEnvelope{RunID: env.RunID, AgentRole: env.AgentRole, Status: env.Status, Uncertainties: env.Uncertainties, NextChecks: env.NextChecks}
		for _, h := range env.Hypotheses {
			if HypothesisKey(h.Statement) == key {
				e.Hypotheses = append(e.Hypotheses, h)
				for _, refs := range [][]string{h.Supports, h.Refutes} {
					for _, ref := range refs {
						aliases[strings.TrimSpace(ref)] = true
						aliases["run:"+env.RunID+"#"+ref] = true
					}
				}
			}
		}
		if len(e.Hypotheses) > 0 {
			in.Results = append(in.Results, e)
		}
	}
	selected := map[string]bool{}
	supersessionRefs := map[string]bool{}
	for _, r := range ledger {
		if r.TenantID != scope.TenantID || r.WorkID != scope.WorkID {
			continue
		}
		if aliases[r.ID] || aliases["evidence:"+r.ID] || (r.SourceRef != "" && aliases[r.SourceRef]) {
			selected[r.ID] = true
			supersessionRefs[r.ID] = true
			if r.SourceRef != "" {
				supersessionRefs[r.SourceRef] = true
			}
			in.Evidence = append(in.Evidence, r)
		}
	}
	// Supersession is relevant even when the new record was not cited.
	for _, r := range ledger {
		if r.TenantID != scope.TenantID || r.WorkID != scope.WorkID || selected[r.ID] || r.CapturedAt.After(at) {
			continue
		}
		if supersessionRefs[strings.TrimPrefix(r.Supersedes, "evidence:")] {
			in.Evidence = append(in.Evidence, r)
		}
	}
	sort.Slice(in.Evidence, func(i, j int) bool { return in.Evidence[i].ID < in.Evidence[j].ID })
	return in
}

// ReplayDecision is pure: it never reads providers, writes stores or dispatches actions.
func ReplayDecision(d DecisionVerdict) (DecisionVerdict, error) {
	if d.Input == nil || d.Profile != DecisionProfile || d.ProfileVersion != 1 || d.AdjudicatorVersion != AdjudicatorVersion {
		return DecisionVerdict{}, fmt.Errorf("%w: unsupported decision replay", ErrInvalid)
	}
	digest, err := cpkit.Hash(*d.Input)
	if err != nil {
		return DecisionVerdict{}, fmt.Errorf("%w: decision input: %v", ErrInvalid, err)
	}
	if d.Input.Scope != d.DecisionScope || !d.Input.At.Equal(d.AdjudicatedAt) || digest != d.InputDigest {
		return DecisionVerdict{}, fmt.Errorf("%w: decision input digest mismatch", ErrInvalid)
	}
	_, hs, err := Adjudicate(d.Input.Scope, d.Input.Results, d.Input.Evidence, d.Input.At)
	if err != nil {
		return DecisionVerdict{}, err
	}
	if len(hs) != 1 || hs[0].Key != d.DecisionKey {
		return DecisionVerdict{}, fmt.Errorf("%w: decision key mismatch", ErrInvalid)
	}
	return *hs[0].Decision, nil
}
