// Package evidence is the cross-run evidence and hypothesis ledger of a Work
// (RFC §12–13). Evidence is append-only: a correction is a new record that
// supersedes the old one. Hypotheses carry supporting and refuting evidence
// across runs and agents; their status follows from the evidence, not from
// a vote or an averaged confidence.
//
// Agent outputs are untrusted data: a run result is parsed into a bounded
// ResultEnvelope, and only references and short statements are stored.
package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Source types.
const (
	SourceMetric   = "metric"
	SourceLog      = "log"
	SourceTrace    = "trace"
	SourceChange   = "change"
	SourceTopology = "topology"
	SourceTool     = "tool"
	SourceHuman    = "human"
	SourceRun      = "run"
	SourceEvent    = "event"
	SourceOutcome  = "outcome"
	SourceAction   = "action"
)

var sourceTypes = map[string]bool{SourceMetric: true, SourceLog: true, SourceTrace: true, SourceChange: true,
	SourceTopology: true, SourceTool: true, SourceHuman: true, SourceRun: true, SourceEvent: true,
	SourceOutcome: true, SourceAction: true}

// Record is one piece of evidence.
type Record struct {
	ID           string     `json:"id"`
	TenantID     string     `json:"tenant_id"`
	WorkID       string     `json:"work_id"`
	SourceType   string     `json:"source_type"`
	SourceRef    string     `json:"source_ref"`
	ArtifactRef  string     `json:"artifact_ref,omitempty"`
	ContentHash  string     `json:"content_hash"`
	Summary      string     `json:"summary,omitempty"` // untrusted, bounded
	OccurredFrom *time.Time `json:"occurred_from,omitempty"`
	OccurredTo   *time.Time `json:"occurred_to,omitempty"`
	CapturedAt   time.Time  `json:"captured_at"`
	TrustTier    string     `json:"trust_tier"`
	Redaction    string     `json:"redaction_level,omitempty"`
	Claims       []string   `json:"claims,omitempty"`
	Supersedes   string     `json:"supersedes,omitempty"`
	RunID        string     `json:"run_id,omitempty"`
	AgentRole    string     `json:"agent_role,omitempty"`
	ActorID      string     `json:"actor_id,omitempty"`

	// Causal attributes (design §11.3). They are optional: records written
	// before they existed read as observations of unknown quality.

	// Kind is what the record is: observation | change | action_effect |
	// human_report | inference.
	Kind string `json:"kind,omitempty"`
	// Relation is how the record bears on the claims it is cited for:
	// supports | refutes | correlates | caused_by | temporally_precedes.
	// "correlates" is never read as "caused_by".
	Relation string `json:"relation,omitempty"`
	// IndependenceGroup identifies the underlying observation (source +
	// query digest + window). Records of one group are one piece of
	// evidence, however many agents cite them.
	IndependenceGroup string `json:"independence_group,omitempty"`
	// Quality: valid | stale | partial | injected | unresolved.
	Quality string `json:"quality,omitempty"`
	// SourceID is the catalog source the record came from.
	SourceID string `json:"source_id,omitempty"`
	// SnapshotID is the world snapshot the conclusion was drawn against.
	SnapshotID string `json:"snapshot_id,omitempty"`
}

// Evidence kinds, relations and qualities.
const (
	KindObservation  = "observation"
	KindChange       = "change"
	KindActionEffect = "action_effect"
	KindHumanReport  = "human_report"
	KindInference    = "inference"

	RelSupports   = "supports"
	RelRefutes    = "refutes"
	RelCorrelates = "correlates"
	RelCausedBy   = "caused_by"
	RelPrecedes   = "temporally_precedes"

	QualityValid      = "valid"
	QualityStale      = "stale"
	QualityPartial    = "partial"
	QualityInjected   = "injected"
	QualityUnresolved = "unresolved"
)

var (
	evidenceKinds = map[string]bool{"": true, KindObservation: true, KindChange: true, KindActionEffect: true,
		KindHumanReport: true, KindInference: true}
	evidenceRelations = map[string]bool{"": true, RelSupports: true, RelRefutes: true, RelCorrelates: true,
		RelCausedBy: true, RelPrecedes: true}
	evidenceQualities = map[string]bool{"": true, QualityValid: true, QualityStale: true, QualityPartial: true,
		QualityInjected: true, QualityUnresolved: true}
)

// Usable reports whether the record may support a conclusion: stale,
// partial, injected and unresolved evidence is kept for audit but does not
// count toward a verdict.
func (r Record) Usable() bool { return r.Quality == QualityValid }

// Group is the independence group of the record; records without one are
// independent by content.
func (r Record) Group() string {
	if r.IndependenceGroup != "" {
		return r.IndependenceGroup
	}
	return r.ContentHash
}

// Limits.
const (
	MaxSummary    = 2000
	MaxNextChecks = 8
	MaxClaims     = 32
	MaxStatement  = 500
	MaxHypotheses = 50
)

// ErrInvalid wraps validation errors.
var ErrInvalid = errors.New("invalid evidence")

// Normalize validates a record and fills defaults.
func (r *Record) Normalize(now time.Time) error {
	if !sourceTypes[r.SourceType] {
		return fmt.Errorf("%w: unknown source type %q", ErrInvalid, r.SourceType)
	}
	if r.SourceRef == "" || len(r.SourceRef) > 512 {
		return fmt.Errorf("%w: source_ref is required", ErrInvalid)
	}
	if len(r.Claims) > MaxClaims {
		return fmt.Errorf("%w: too many claims", ErrInvalid)
	}
	if !evidenceKinds[r.Kind] || !evidenceRelations[r.Relation] || !evidenceQualities[r.Quality] {
		return fmt.Errorf("%w: unknown kind, relation or quality", ErrInvalid)
	}
	if len(r.IndependenceGroup) > 200 || len(r.SourceID) > 128 || len(r.SnapshotID) > 64 {
		return fmt.Errorf("%w: causal attributes are too long", ErrInvalid)
	}
	if r.Relation == RelCausedBy && r.Kind == KindInference {
		// A model inference may propose a cause; it cannot assert one.
		r.Relation = RelCorrelates
	}
	r.Summary = cpkit.Truncate(r.Summary, MaxSummary)
	if r.TrustTier == "" {
		r.TrustTier = "T4"
		if r.SourceType == SourceHuman {
			r.TrustTier = "T2"
		}
	}
	if r.OccurredFrom != nil && r.OccurredTo != nil && r.OccurredTo.Before(*r.OccurredFrom) {
		return fmt.Errorf("%w: occurred_to before occurred_from", ErrInvalid)
	}
	if r.ContentHash == "" {
		r.ContentHash = cpkit.MustHash(struct{ T, R, A, S string }{r.SourceType, r.SourceRef, r.ArtifactRef, r.Summary})
	}
	if r.CapturedAt.IsZero() {
		r.CapturedAt = now
	}
	if r.ID == "" {
		r.ID = cpkit.NewID()
	}
	return nil
}

// Hypothesis statuses.
const (
	HypOpen         = "OPEN"
	HypSupported    = "SUPPORTED"
	HypRefuted      = "REFUTED"
	HypChallenged   = "CHALLENGED"
	HypInconclusive = "INCONCLUSIVE"
)

// RCA categories.
const (
	CatTrigger      = "trigger"
	CatRootCause    = "root_cause"
	CatContributing = "contributing_factor"
	CatDetection    = "detection_gap"
	CatRecovery     = "recovery_gap"
	CatUnknown      = "unclassified"
)

// Hypothesis is a Work-level hypothesis tracked across runs.
type Hypothesis struct {
	ID         string           `json:"id"`
	TenantID   string           `json:"tenant_id"`
	WorkID     string           `json:"work_id"`
	Key        string           `json:"key"` // normalized statement key (dedup across agents)
	Statement  string           `json:"statement"`
	Category   string           `json:"category"`
	Status     string           `json:"status"`
	Decision   *DecisionVerdict `json:"decision,omitempty"`
	Supporting []string         `json:"supporting_evidence_refs,omitempty"`
	Refuting   []string         `json:"refuting_evidence_refs,omitempty"`
	// Confidence is the model's self-estimate, kept for calibration only.
	Confidence float64   `json:"confidence"`
	Proponents []string  `json:"proponents,omitempty"` // agent roles / run ids
	CreatedBy  string    `json:"created_by_run_id,omitempty"`
	UpdatedBy  string    `json:"last_updated_by_run_id,omitempty"`
	Version    int64     `json:"version"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// DeriveStatus applies the evidence rule: refuting evidence without support
// refutes; both sides challenge; support alone supports; nothing is open.
func DeriveStatus(supporting, refuting int) string {
	switch {
	case supporting > 0 && refuting > 0:
		return HypChallenged
	case refuting > 0:
		return HypRefuted
	case supporting > 0:
		return HypSupported
	}
	return HypOpen
}

// HypothesisKey normalizes a statement for cross-agent matching.
func HypothesisKey(statement string) string {
	s := strings.ToLower(strings.Join(strings.Fields(statement), " "))
	s = strings.Trim(s, " .;:!")
	return cpkit.HashBytes([]byte(s))[7:31]
}

// Store is the persistence port (internal/store/cpsql).
type Store interface {
	// AddEvidence is idempotent on (tenant, work, content hash).
	AddEvidence(ctx context.Context, r Record) (Record, bool, error)
	ListEvidence(ctx context.Context, tenantID, workID string, limit int) ([]Record, error)
	GetEvidence(ctx context.Context, tenantID, id string) (Record, error)
	// UpsertHypothesis inserts or CAS-updates by (tenant, work, key).
	UpsertHypothesis(ctx context.Context, h Hypothesis, expectedVersion int64) (Hypothesis, error)
	ListHypotheses(ctx context.Context, tenantID, workID string) ([]Hypothesis, error)
}

// ErrVersionConflict is returned by UpsertHypothesis on a stale version.
var ErrVersionConflict = errors.New("hypothesis version conflict")

// ResultHypothesis is a hypothesis reported by one run.
type ResultHypothesis struct {
	ID         string   `json:"id"`
	Statement  string   `json:"statement"`
	Status     string   `json:"status"`
	Confidence float64  `json:"confidence_estimate"`
	Supports   []string `json:"supports,omitempty"`
	Refutes    []string `json:"refutes,omitempty"`
	Category   string   `json:"category,omitempty"`
}

// ResultEnvelope is the bounded, structured output of one agent run.
type ResultEnvelope struct {
	RunID         string             `json:"run_id"`
	AgentRole     string             `json:"agent_role,omitempty"`
	Status        string             `json:"status"` // COMPLETED | FAILED | CANCELLED | ...
	Required      bool               `json:"required"`
	Summary       string             `json:"summary,omitempty"`
	Hypotheses    []ResultHypothesis `json:"hypotheses,omitempty"`
	Verification  string             `json:"verification,omitempty"` // independent verifier verdict
	CostUSD       float64            `json:"cost_usd,omitempty"`
	Uncertainties []string           `json:"uncertainties,omitempty"`
	Proposed      json.RawMessage    `json:"proposed_action,omitempty"`
	FailureReason string             `json:"failure_reason,omitempty"`
	// NextChecks are tools the run suggests checking next (untrusted: the
	// evidence planner only uses them to rank tools it already allows).
	NextChecks []string `json:"next_checks,omitempty"`
}

// ParseResult reads a kernel run result artifact into an envelope. Unknown
// fields are ignored; everything is bounded.
func ParseResult(runID, role, status string, raw []byte) ResultEnvelope {
	env := ResultEnvelope{RunID: runID, AgentRole: role, Status: status}
	if len(raw) == 0 {
		return env
	}
	var doc struct {
		Answer     string             `json:"answer"`
		Summary    string             `json:"summary"`
		Hypotheses []ResultHypothesis `json:"hypotheses"`
		Verify     struct {
			Verdict string `json:"verdict"`
		} `json:"independent_verification"`
		Loop struct {
			CostUSD float64 `json:"cost_usd"`
		} `json:"loop"`
		Uncertainties []string          `json:"uncertainties"`
		Proposed      json.RawMessage   `json:"proposed_action"`
		NextChecks    []json.RawMessage `json:"next_checks"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		env.Summary = cpkit.Truncate(string(raw), MaxSummary)
		return env
	}
	env.Summary = doc.Answer
	if env.Summary == "" {
		env.Summary = doc.Summary
	}
	env.Summary = cpkit.Truncate(env.Summary, MaxSummary)
	env.Verification, env.CostUSD = doc.Verify.Verdict, doc.Loop.CostUSD
	for i, h := range doc.Hypotheses {
		if i >= MaxHypotheses {
			break
		}
		h.Statement = cpkit.Truncate(strings.TrimSpace(h.Statement), MaxStatement)
		if h.Statement == "" {
			continue
		}
		if len(h.Supports) > MaxClaims {
			h.Supports = h.Supports[:MaxClaims]
		}
		if len(h.Refutes) > MaxClaims {
			h.Refutes = h.Refutes[:MaxClaims]
		}
		env.Hypotheses = append(env.Hypotheses, h)
	}
	for i, u := range doc.Uncertainties {
		if i >= 16 {
			break
		}
		env.Uncertainties = append(env.Uncertainties, cpkit.Truncate(u, 300))
	}
	if len(doc.Proposed) > 0 && len(doc.Proposed) <= 16<<10 && json.Valid(doc.Proposed) {
		env.Proposed = doc.Proposed
	}
	// next_checks: ["tool", ...] or [{"tool": "..."}, ...]
	for _, raw := range doc.NextChecks {
		if len(env.NextChecks) >= MaxNextChecks {
			break
		}
		var name string
		if json.Unmarshal(raw, &name) != nil {
			var obj struct {
				Tool string `json:"tool"`
			}
			if json.Unmarshal(raw, &obj) != nil {
				continue
			}
			name = obj.Tool
		}
		if name = strings.TrimSpace(name); name != "" && len(name) <= 128 {
			env.NextChecks = appendUnique(env.NextChecks, name)
		}
	}
	return env
}

// Conflict is a hypothesis that agents disagree on.
type Conflict struct {
	Key       string   `json:"key"`
	Statement string   `json:"statement"`
	SupportBy []string `json:"supported_by"`
	RefuteBy  []string `json:"refuted_by"`
}

// MergeReport is the merged view of a parallel investigation.
type MergeReport struct {
	Verdict    string             `json:"verdict"` // COMPLETE | PARTIAL | FAILED | INCONCLUSIVE
	Hypotheses []MergedHypothesis `json:"hypotheses"`
	Conflicts  []Conflict         `json:"conflicts,omitempty"`
	// Gaps lists agents whose investigation is missing (failed/cancelled).
	Gaps           []string          `json:"gaps,omitempty"`
	RequiredFailed []string          `json:"required_failed,omitempty"`
	Summaries      map[string]string `json:"summaries,omitempty"`
	CostUSD        float64           `json:"cost_usd"`
	// NextChecks maps a suggested tool to the share of completed runs that
	// suggested it.
	NextChecks map[string]float64 `json:"next_checks,omitempty"`
}

// MergedHypothesis aggregates one hypothesis across agents.
type MergedHypothesis struct {
	Key        string   `json:"key"`
	Statement  string   `json:"statement"`
	Category   string   `json:"category"`
	Status     string   `json:"status"`
	Supporting []string `json:"supporting_evidence_refs,omitempty"`
	Refuting   []string `json:"refuting_evidence_refs,omitempty"`
	Proponents []string `json:"proponents"`
}

// Merge summarizes untrusted agent claims for display. Only Adjudicate can
// promote a hypothesis; reported disagreements remain unresolved claims.
func Merge(results []ResultEnvelope) MergeReport {
	rep := MergeReport{Summaries: map[string]string{}}
	byKey := map[string]*MergedHypothesis{}
	support := map[string]map[string]bool{}
	refute := map[string]map[string]bool{}
	ok := 0
	for _, r := range results {
		who := r.AgentRole
		if who == "" {
			who = r.RunID
		}
		rep.CostUSD += r.CostUSD
		if r.Status != "COMPLETED" {
			rep.Gaps = append(rep.Gaps, who)
			if r.Required {
				rep.RequiredFailed = append(rep.RequiredFailed, who)
			}
			continue
		}
		ok++
		for _, c := range r.NextChecks {
			if rep.NextChecks == nil {
				rep.NextChecks = map[string]float64{}
			}
			rep.NextChecks[c]++
		}
		if r.Summary != "" {
			rep.Summaries[who] = cpkit.Truncate(r.Summary, 600)
		}
		for _, h := range r.Hypotheses {
			k := HypothesisKey(h.Statement)
			m := byKey[k]
			if m == nil {
				cat := h.Category
				if cat == "" {
					cat = CatUnknown
				}
				m = &MergedHypothesis{Key: k, Statement: h.Statement, Category: cat}
				byKey[k] = m
				support[k], refute[k] = map[string]bool{}, map[string]bool{}
			}
			m.Proponents = appendUnique(m.Proponents, who)
			for _, e := range h.Supports {
				m.Supporting = appendUnique(m.Supporting, qualify(r.RunID, e))
			}
			for _, e := range h.Refutes {
				m.Refuting = appendUnique(m.Refuting, qualify(r.RunID, e))
			}
			switch strings.ToUpper(h.Status) {
			case "VERIFIED", "SUPPORTED":
				support[k][who] = true
			case "REFUTED":
				refute[k][who] = true
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m := byKey[k]
		m.Status = HypOpen
		if len(support[k]) > 0 && len(refute[k]) > 0 {
			c := Conflict{Key: k, Statement: m.Statement}
			for w := range support[k] {
				c.SupportBy = append(c.SupportBy, w)
			}
			for w := range refute[k] {
				c.RefuteBy = append(c.RefuteBy, w)
			}
			sort.Strings(c.SupportBy)
			sort.Strings(c.RefuteBy)
			rep.Conflicts = append(rep.Conflicts, c)
			m.Status = HypInconclusive
		}
		rep.Hypotheses = append(rep.Hypotheses, *m)
	}
	sort.Strings(rep.Gaps)
	sort.Strings(rep.RequiredFailed)
	switch {
	case ok == 0:
		rep.Verdict = "FAILED"
	case len(rep.RequiredFailed) > 0:
		rep.Verdict = "FAILED"
	case len(rep.Gaps) > 0:
		rep.Verdict = "PARTIAL"
	case len(rep.Conflicts) > 0:
		rep.Verdict = "INCONCLUSIVE"
	default:
		rep.Verdict = "COMPLETE"
	}
	for k, n := range rep.NextChecks {
		rep.NextChecks[k] = math.Round(n/float64(max(ok, 1))*100) / 100
	}
	return rep
}

// qualify scopes a run-local evidence ref ("obs:step3") to its run so refs
// from different agents never collide.
func qualify(runID, ref string) string {
	if strings.Contains(ref, "://") || strings.HasPrefix(ref, "run:") {
		return ref
	}
	return "run:" + runID + "#" + cpkit.Truncate(ref, 200)
}

func appendUnique(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(s, v)
}
