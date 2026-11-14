package evidence

import (
	"encoding/json"
	"fmt"
	"slices"
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
	AdjudicatorVersion   = "2"
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

// CurrentFor rejects legacy, foreign, obsolete and unreproducible decisions.
func (d *DecisionVerdict) CurrentFor(scope DecisionScope, key string) bool {
	_, ok := d.currentHypothesis(scope, key)
	return ok
}

func (d *DecisionVerdict) currentHypothesis(scope DecisionScope, key string) (Hypothesis, bool) {
	if scope.TenantID == "" || scope.WorkID == "" || scope.Generation < 1 || key == "" || d == nil || d.DecisionScope != scope || d.DecisionKey != key || d.Profile != DecisionProfile || d.ProfileVersion != 1 || d.AdjudicatorVersion != AdjudicatorVersion || d.InputDigest == "" || d.AdjudicatedAt.IsZero() {
		return Hypothesis{}, false
	}
	_, replayed, err := replayDecision(*d)
	if err != nil {
		return Hypothesis{}, false
	}
	actual, err := cpkit.Hash(d)
	if err != nil {
		return Hypothesis{}, false
	}
	expected, err := cpkit.Hash(replayed.Decision)
	return replayed, err == nil && actual == expected
}

// CurrentDecisionFor also binds the displayed claim to the adjudicated input.
func (h Hypothesis) CurrentDecisionFor(scope DecisionScope) bool {
	expected, ok := h.Decision.currentHypothesis(scope, h.Key)
	return ok && sameClaim(h, expected)
}

func sameClaim(a, b Hypothesis) bool {
	return a.Statement == b.Statement && a.Category == b.Category &&
		slices.Equal(a.Supporting, b.Supporting) && slices.Equal(a.Refuting, b.Refuting) && slices.Equal(a.Proponents, b.Proponents)
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

// ValidateAdjudication binds a submitted hypothesis to the durable evidence
// snapshot and returns the findings derived by the same adjudicator.
func ValidateAdjudication(h Hypothesis, ledger []Record) ([]Finding, error) {
	d := h.Decision
	if d == nil || !h.CurrentDecisionFor(d.DecisionScope) {
		return nil, fmt.Errorf("%w: unreproducible decision", ErrInvalid)
	}
	input := decisionInput(d.DecisionScope, h.Key, d.Input.Results, ledger, d.AdjudicatedAt)
	digest, err := cpkit.Hash(input)
	if err != nil || digest != d.InputDigest {
		return nil, fmt.Errorf("%w: decision input differs from durable evidence", ErrInvalid)
	}
	syn, expected, err := Adjudicate(input.Scope, input.Results, input.Evidence, input.At)
	if err != nil {
		return nil, err
	}
	if len(expected) != 1 {
		return nil, fmt.Errorf("%w: ambiguous decision claim", ErrInvalid)
	}
	if !sameClaim(h, expected[0]) {
		return nil, fmt.Errorf("%w: hypothesis differs from adjudicated claim", ErrInvalid)
	}
	return syn.Findings, nil
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
	ledgerByID := map[string]Record{}
	selectedSources := map[string]bool{}
	for _, r := range ledger {
		if r.TenantID != scope.TenantID || r.WorkID != scope.WorkID || r.CapturedAt.After(at) {
			continue
		}
		if r.ID != "" {
			ledgerByID[r.ID] = r
		}
		if aliases[r.ID] || aliases["evidence:"+r.ID] || (r.SourceRef != "" && aliases[r.SourceRef]) {
			selected[r.ID] = true
			if r.SourceRef != "" {
				selectedSources[r.SourceRef] = true
			}
			in.Evidence = append(in.Evidence, r)
		}
	}
	// Supersession is relevant even when the new record was not cited.
	for _, r := range ledger {
		if r.TenantID != scope.TenantID || r.WorkID != scope.WorkID || selected[r.ID] || r.CapturedAt.After(at) {
			continue
		}
		// Preserve the resolver's identity precedence when projecting the
		// snapshot: removing an unrelated ID must not turn it into a source alias.
		ref, canonical := strings.CutPrefix(r.Supersedes, "evidence:")
		relevant := selectedSources[ref]
		_, namesID := ledgerByID[ref]
		if canonical || namesID {
			relevant = selected[ref]
		}
		if relevant {
			in.Evidence = append(in.Evidence, r)
		}
	}
	// A cited superseder may name an otherwise unrelated record by raw ID.
	// Retain that identity, including its dependencies, so replay cannot
	// reinterpret the raw ID as a source alias when projecting the ledger.
	included := map[string]bool{}
	for _, r := range in.Evidence {
		included[r.ID] = true
	}
	for i := 0; i < len(in.Evidence); i++ {
		ref, canonical := strings.CutPrefix(in.Evidence[i].Supersedes, "evidence:")
		if canonical {
			continue
		}
		if target, ok := ledgerByID[ref]; ok && !included[target.ID] {
			included[target.ID] = true
			in.Evidence = append(in.Evidence, target)
		}
	}
	sort.Slice(in.Evidence, func(i, j int) bool { return in.Evidence[i].ID < in.Evidence[j].ID })
	return in
}

// ReplayDecision is pure: it never reads providers, writes stores or dispatches actions.
func ReplayDecision(d DecisionVerdict) (DecisionVerdict, error) {
	_, h, err := replayDecision(d)
	if err != nil {
		return DecisionVerdict{}, err
	}
	return *h.Decision, nil
}

func replayDecision(d DecisionVerdict) (Synthesis, Hypothesis, error) {
	if d.Input == nil || d.Profile != DecisionProfile || d.ProfileVersion != 1 || d.AdjudicatorVersion != AdjudicatorVersion {
		return Synthesis{}, Hypothesis{}, fmt.Errorf("%w: unsupported decision replay", ErrInvalid)
	}
	digest, err := cpkit.Hash(*d.Input)
	if err != nil {
		return Synthesis{}, Hypothesis{}, fmt.Errorf("%w: decision input: %v", ErrInvalid, err)
	}
	if d.Input.Scope != d.DecisionScope || !d.Input.At.Equal(d.AdjudicatedAt) || digest != d.InputDigest {
		return Synthesis{}, Hypothesis{}, fmt.Errorf("%w: decision input digest mismatch", ErrInvalid)
	}
	syn, hs, err := Adjudicate(d.Input.Scope, d.Input.Results, d.Input.Evidence, d.Input.At)
	if err != nil {
		return Synthesis{}, Hypothesis{}, err
	}
	if len(hs) != 1 || hs[0].Key != d.DecisionKey {
		return Synthesis{}, Hypothesis{}, fmt.Errorf("%w: decision key mismatch", ErrInvalid)
	}
	return syn, hs[0], nil
}
