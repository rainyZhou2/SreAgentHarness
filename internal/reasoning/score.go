// Package reasoning measures, reuses and improves the agent's explicit
// decision trace: the plan, hypotheses with confidence, expected observation
// and verification the model is asked to produce. It never reads
// provider-hidden reasoning.
//
// Score is a pure function so live runs (stored thoughts) and offline
// replays (replayed decisions) are measured the same way.
package reasoning

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"unicode"

	"go-sre-harness/internal/domain"
)

// Step is one step of a trace: the thoughts it produced and, for tool
// steps, the call it made.
type Step struct {
	Seq      int64            `json:"seq"`
	Reason   bool             `json:"reason,omitempty"`
	Thoughts []domain.Thought `json:"thoughts,omitempty"`
	Tool     string           `json:"tool,omitempty"`
	Args     json.RawMessage  `json:"args,omitempty"`
}

// Trace is a run (or a replayed case) to score.
type Trace struct {
	Steps     []Step
	Completed bool
	// Correct is the operator's verdict on the root cause, when known.
	Correct *bool
}

// Metrics are the reasoning-quality measures of one run.
type Metrics struct {
	ReasonSteps    int      `json:"reason_steps"`
	ToolCalls      int      `json:"tool_calls"`
	RedundantCalls int      `json:"redundant_calls"`
	Hypotheses     int      `json:"hypotheses"`
	Verified       int      `json:"verified"`
	Refuted        int      `json:"refuted"`
	Unverified     int      `json:"unverified"`
	Brier          *float64 `json:"brier,omitempty"`
	CalibrationN   int      `json:"calibration_n"`
	// CalibrationCoverage is the share of settled (VERIFIED/REFUTED)
	// hypotheses that carried a confidence.
	CalibrationCoverage float64 `json:"calibration_coverage"`
	Grounded            int     `json:"grounded"`
	Claims              int     `json:"claims"`
	Grounding           float64 `json:"grounding"`
	Redundancy          float64 `json:"redundancy"`
	// StepsToVerdict counts reasoning steps until the first hypothesis was
	// verified or refuted (0: never).
	StepsToVerdict int `json:"steps_to_verdict"`
	// Conclusion is the final verification verdict (VERIFIED, REFUTED,
	// UNVERIFIED) or "" when the run did not conclude.
	Conclusion string  `json:"conclusion,omitempty"`
	Completed  bool    `json:"completed"`
	Correct    *bool   `json:"correct,omitempty"`
	Quality    float64 `json:"quality"`
	// Structured is the share of thoughts that came from structured output.
	Structured float64 `json:"structured"`
	// CitedMemories are memories the model cited as evidence or support
	// (not merely retrieved), without version.
	CitedMemories []string `json:"cited_memories,omitempty"`
	// Citations counts distinct refs cited per step (supports, refutes,
	// cites); Unresolved those that named nothing the model was shown.
	// CitationValidity is the resolved share (1 when nothing was cited).
	Citations        int     `json:"citations"`
	Unresolved       int     `json:"unresolved_citations"`
	CitationValidity float64 `json:"citation_validity"`
	// UnsupportedVerdicts counts hypotheses settled VERIFIED or REFUTED
	// without a resolved citation. UnsupportedConclusion is set when the
	// conclusion is VERIFIED but neither the verification nor any verified
	// hypothesis cites a resolved item: a self-declared verdict, scored and
	// counted as unverified.
	UnsupportedVerdicts   int  `json:"unsupported_verdicts"`
	UnsupportedConclusion bool `json:"unsupported_conclusion,omitempty"`
	// Plan describes how the run was planned (nil: not planned).
	Plan *PlanMetrics `json:"plan,omitempty"`
}

// HypothesisIDAttr is the thought attribute carrying the model's stable
// hypothesis id.
const HypothesisIDAttr = "hypothesis_id"

// hypKey identifies a hypothesis across steps: the model's id when given,
// otherwise the normalized statement.
func hypKey(th domain.Thought) string {
	if id, _ := th.Attrs[HypothesisIDAttr].(string); strings.TrimSpace(id) != "" {
		return "id:" + strings.ToLower(strings.TrimSpace(id))
	}
	if k := normalize(th.Summary); k != "" {
		return "s:" + k
	}
	return ""
}

// citedMemory returns the memory id of a citation ref ("memory:<id>@<v>").
// Automatic "based_on" links from the plan to every retrieved memory are not
// citations.
func citedMemory(r domain.ThoughtRef) string {
	if r.Relation == "based_on" || !strings.HasPrefix(r.Ref, "memory:") {
		return ""
	}
	id := strings.TrimPrefix(r.Ref, "memory:")
	if i := strings.LastIndexByte(id, '@'); i > 0 {
		id = id[:i]
	}
	return id
}

// Composite weights (documented in the design; Quality is a convenience
// summary, the gate also checks the individual dimensions).
const (
	wGrounding   = 0.3
	wCalibration = 0.3
	wConclusion  = 0.2
	wEfficiency  = 0.2
)

type hyp struct {
	status     domain.ThoughtStatus
	confidence float64
	supported  bool // some version cited a resolved ref
}

// isCitation reports whether a ref is a model citation (not a server link
// such as based_on or uses).
func isCitation(r domain.ThoughtRef) bool {
	switch r.Relation {
	case "supports", "refutes", "cites":
		return true
	}
	return false
}

// hasResolvedCitation reports whether a thought cites a resolved ref.
func hasResolvedCitation(th domain.Thought) bool {
	for _, r := range th.Refs {
		if isCitation(r) && r.Resolved() {
			return true
		}
	}
	return false
}

// Score computes the metrics of a trace.
func Score(t Trace) Metrics {
	m := Metrics{Completed: t.Completed, Correct: t.Correct}
	hyps := map[string]hyp{}
	var order []string
	seenCalls := map[string]bool{}
	thoughts, structured := 0, 0
	cited := map[string]bool{}
	verificationSupported := false
	for _, s := range t.Steps {
		stepRefs := map[string]bool{}
		if s.Tool != "" {
			m.ToolCalls++
			key := s.Tool + "\x00" + canonical(s.Args)
			if seenCalls[key] {
				m.RedundantCalls++
			}
			seenCalls[key] = true
		}
		if s.Reason {
			m.ReasonSteps++
		}
		for _, th := range s.Thoughts {
			if th.Kind == domain.ThoughtObservation {
				continue
			}
			thoughts++
			if th.Structured {
				structured++
			}
			for _, r := range th.Refs {
				if isCitation(r) && !stepRefs[r.Ref] {
					stepRefs[r.Ref] = true
					m.Citations++
					if !r.Resolved() {
						m.Unresolved++
					}
				}
				if !r.Resolved() {
					continue
				}
				if id := citedMemory(r); id != "" && !cited[id] {
					cited[id] = true
					m.CitedMemories = append(m.CitedMemories, id)
				}
			}
			switch th.Kind {
			case domain.ThoughtHypothesis:
				m.Claims++
				grounded := hasResolvedCitation(th)
				if grounded {
					m.Grounded++
				}
				k := hypKey(th)
				if k == "" {
					continue
				}
				if _, ok := hyps[k]; !ok {
					order = append(order, k)
				}
				prev := hyps[k]
				h := hyp{status: th.Status, confidence: th.Confidence, supported: prev.supported || grounded}
				if h.confidence == 0 {
					h.confidence = prev.confidence
				}
				hyps[k] = h
				if m.StepsToVerdict == 0 && (th.Status == domain.ThoughtVerified || th.Status == domain.ThoughtRefuted) {
					m.StepsToVerdict = max(m.ReasonSteps, 1)
				}
			case domain.ThoughtDecision:
				m.Claims++
				if hasResolvedCitation(th) {
					m.Grounded++
				}
			case domain.ThoughtVerification:
				verificationSupported = hasResolvedCitation(th)
				switch th.Status {
				case domain.ThoughtVerified, domain.ThoughtRefuted:
					m.Conclusion = string(th.Status)
				default:
					m.Conclusion = string(domain.ThoughtUnverified)
				}
			case domain.ThoughtConclusion:
				if m.Conclusion == "" {
					m.Conclusion = string(domain.ThoughtUnverified)
				}
			}
		}
	}
	var sq, imputed float64
	settled := 0
	verifiedSupported := false
	for _, k := range order {
		h := hyps[k]
		m.Hypotheses++
		if h.status == domain.ThoughtVerified || h.status == domain.ThoughtRefuted {
			if !h.supported {
				m.UnsupportedVerdicts++
			} else if h.status == domain.ThoughtVerified {
				verifiedSupported = true
			}
		}
		var y float64
		switch h.status {
		case domain.ThoughtVerified:
			m.Verified++
			y = 1
		case domain.ThoughtRefuted:
			m.Refuted++
		default:
			m.Unverified++
			continue
		}
		settled++
		if h.confidence > 0 && h.confidence <= 1 {
			e := (h.confidence - y) * (h.confidence - y)
			sq += e
			imputed += e
			m.CalibrationN++
		} else {
			// A settled hypothesis without a confidence is scored as the
			// no-skill forecast 0.5 (Brier 0.25): below any useful forecast.
			// Missing confidence is tracked by CalibrationCoverage.
			imputed += 0.25
		}
	}
	if m.CalibrationN > 0 {
		b := round(sq / float64(m.CalibrationN))
		m.Brier = &b
	}
	sort.Strings(m.CitedMemories)
	calibration := 0.5 // nothing settled: no calibration evidence
	if settled > 0 {
		m.CalibrationCoverage = round(float64(m.CalibrationN) / float64(settled))
		calibration = 1 - imputed/float64(settled)
	}
	if m.Claims > 0 {
		m.Grounding = round(float64(m.Grounded) / float64(m.Claims))
	}
	if m.ToolCalls > 0 {
		m.Redundancy = round(float64(m.RedundantCalls) / float64(m.ToolCalls))
	}
	m.CitationValidity = 1
	if m.Citations > 0 {
		m.CitationValidity = round(float64(m.Citations-m.Unresolved) / float64(m.Citations))
	}
	if m.Conclusion == string(domain.ThoughtVerified) && !verificationSupported && !verifiedSupported {
		m.UnsupportedConclusion = true
	}
	if thoughts > 0 {
		m.Structured = round(float64(structured) / float64(thoughts))
	}
	m.Quality = quality(m, calibration)
	return m
}

func quality(m Metrics, calibration float64) float64 {
	conclusion := 0.0
	switch {
	case m.Correct != nil && *m.Correct:
		conclusion = 1
	case m.Correct != nil:
		conclusion = 0
	case m.VerifiedConclusion():
		conclusion = 1
	case m.Conclusion == string(domain.ThoughtVerified), m.Conclusion == string(domain.ThoughtUnverified):
		conclusion = 0.3
	}
	grounding := m.Grounding
	if m.Claims == 0 {
		grounding = 0
	}
	return round(wGrounding*grounding + wCalibration*calibration + wConclusion*conclusion + wEfficiency*(1-m.Redundancy))
}

// VerifiedConclusion reports a VERIFIED conclusion backed by at least one
// resolved citation.
func (m Metrics) VerifiedConclusion() bool {
	return m.Conclusion == string(domain.ThoughtVerified) && !m.UnsupportedConclusion
}

// Success reports whether a scored run counts as a success for pattern
// statistics: it completed and either the operator confirmed the root cause
// or, without feedback, the model verified its conclusion with a resolved
// citation.
func (m Metrics) Success() bool {
	if !m.Completed {
		return false
	}
	if m.Correct != nil {
		return *m.Correct
	}
	return m.VerifiedConclusion()
}

// DecisionThoughts renders a decision as thoughts, the way the kernel stores
// them, for scoring offline replays.
func DecisionThoughts(d domain.Decision) []domain.Thought {
	return ResolvedDecisionThoughts(d, nil)
}

// ResolvedDecisionThoughts is DecisionThoughts with citations resolved the
// way the kernel resolves them (nil: keep them as written).
func ResolvedDecisionThoughts(d domain.Decision, resolve func(string) (string, bool)) []domain.Thought {
	ref := func(raw, relation string) domain.ThoughtRef {
		if resolve == nil {
			return domain.ThoughtRef{Ref: raw, Relation: relation}
		}
		if c, ok := resolve(raw); ok {
			return domain.ThoughtRef{Ref: c, Relation: relation}
		}
		return domain.ThoughtRef{Ref: domain.UnresolvedRefPrefix + raw, Relation: relation}
	}
	var out []domain.Thought
	structured := d.Thought != nil && d.Thought.Structured
	add := func(kind domain.ThoughtKind, summary string, status domain.ThoughtStatus, conf float64, refs []domain.ThoughtRef) {
		if strings.TrimSpace(summary) == "" {
			return
		}
		out = append(out, domain.Thought{Seq: len(out) + 1, Kind: kind, Summary: summary, Status: status,
			Confidence: conf, Structured: structured, Refs: refs})
	}
	var evidence []domain.ThoughtRef
	for _, r := range d.EvidenceRefs {
		evidence = append(evidence, ref(r, "supports"))
	}
	if d.Thought != nil {
		add(domain.ThoughtPlan, d.Thought.Plan, domain.ThoughtUnknown, d.Thought.Confidence, nil)
	}
	add(domain.ThoughtDecision, d.ReasonSummary, domain.ThoughtUnknown, 0, evidence)
	for _, h := range d.Hypotheses {
		status := domain.ThoughtUnverified
		switch strings.ToUpper(h.Status) {
		case string(domain.ThoughtVerified):
			status = domain.ThoughtVerified
		case string(domain.ThoughtRefuted):
			status = domain.ThoughtRefuted
		}
		var refs []domain.ThoughtRef
		for _, r := range h.Supports {
			refs = append(refs, ref(r, "supports"))
		}
		for _, r := range h.Refutes {
			refs = append(refs, ref(r, "refutes"))
		}
		n := len(out)
		add(domain.ThoughtHypothesis, h.Statement, status, h.ConfidenceEstimate, refs)
		if id := strings.TrimSpace(h.ID); id != "" && len(out) > n {
			out[n].Attrs = map[string]any{HypothesisIDAttr: id}
		}
	}
	if d.Kind == domain.DecisionFinal {
		if d.Thought != nil && d.Thought.Verification != nil {
			v := d.Thought.Verification
			status := domain.ThoughtUnverified
			switch strings.ToUpper(v.Verdict) {
			case string(domain.ThoughtVerified):
				status = domain.ThoughtVerified
			case string(domain.ThoughtRefuted):
				status = domain.ThoughtRefuted
			}
			add(domain.ThoughtVerification, strings.TrimSpace(v.Verdict+" "+v.Expected+" "+v.Observed), status, 0, evidence)
		}
		add(domain.ThoughtConclusion, d.Answer, domain.ThoughtUnknown, 0, evidence)
	}
	return out
}

func normalize(s string) string {
	var b strings.Builder
	space := false
	for _, r := range strings.ToLower(s) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if space && b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteRune(r)
			space = false
		default:
			space = true
		}
	}
	return b.String()
}

func canonical(b json.RawMessage) string {
	var v any
	if len(b) == 0 || json.Unmarshal(b, &v) != nil {
		return string(b)
	}
	out, _ := json.Marshal(v)
	return string(out)
}

func round(f float64) float64 { return math.Round(f*1000) / 1000 }
