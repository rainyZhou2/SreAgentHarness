// Package outcome verifies whether a Work actually achieved its business
// goal (RFC §11). It is deliberately independent of the model: a verdict is
// computed by deterministic evaluators over metrics, world state, events and
// human confirmations, and the model can neither lower a threshold nor
// declare success.
//
// Missing, stale or insufficient data is INCONCLUSIVE, never SATISFIED, and a
// recovery must hold for StableFor before it counts.
package outcome

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Verdict is the result of an evaluation.
type Verdict string

const (
	Satisfied    Verdict = "SATISFIED"
	NotSatisfied Verdict = "NOT_SATISFIED"
	Inconclusive Verdict = "INCONCLUSIVE"
	Unknown      Verdict = "UNKNOWN"
)

// Valid reports whether v is a known verdict.
func (v Verdict) Valid() bool {
	switch v {
	case Satisfied, NotSatisfied, Inconclusive, Unknown:
		return true
	}
	return false
}

// Objective kinds.
const (
	KindMetric = "metric"
	KindState  = "state"
	KindEvent  = "event"
	KindHuman  = "human"
)

// Comparison operators (a closed set: no expressions are executed).
const (
	OpLT  = "lt"
	OpLTE = "lte"
	OpGT  = "gt"
	OpGTE = "gte"
	OpEQ  = "eq"
	OpNE  = "ne"
)

var opAliases = map[string]string{
	"<": OpLT, "less_than": OpLT, "<=": OpLTE, "less_than_or_equal": OpLTE,
	">": OpGT, "greater_than": OpGT, ">=": OpGTE, "greater_than_or_equal": OpGTE,
	"==": OpEQ, "equal": OpEQ, "!=": OpNE, "not_equal": OpNE,
}

// Aggregations of a metric series. "all" (the default) requires every
// sample in the window to pass, the strictest reading.
const (
	AggAll  = "all"
	AggAvg  = "avg"
	AggMax  = "max"
	AggMin  = "min"
	AggLast = "last"
)

// MetricExpr compares a deterministic metric query over a window.
type MetricExpr struct {
	// Source names a registered metric source (e.g. "prometheus-prod").
	Source string `json:"source"`
	// Query is the source-specific query (or a query reference resolved by
	// the source). It is data for the source, never executed here.
	Query     string  `json:"query"`
	Aggregate string  `json:"aggregate,omitempty"`
	Operator  string  `json:"operator"`
	Threshold float64 `json:"threshold"`
}

// StateExpr checks the current value of a world fact.
type StateExpr struct {
	Predicate string   `json:"predicate"`
	Equals    string   `json:"equals,omitempty"`
	In        []string `json:"in,omitempty"`
	NotEquals string   `json:"not_equals,omitempty"`
}

// EventExpr checks whether an event of a type arrived (or did not arrive)
// for the target within the window.
type EventExpr struct {
	Type   string `json:"type"`
	Absent bool   `json:"absent,omitempty"`
}

// Objective is one success condition.
type Objective struct {
	ID         string         `json:"id"`
	Kind       string         `json:"kind"`
	TargetRef  string         `json:"target_ref,omitempty"`
	Metric     *MetricExpr    `json:"metric,omitempty"`
	State      *StateExpr     `json:"state,omitempty"`
	Event      *EventExpr     `json:"event,omitempty"`
	Window     cpkit.Duration `json:"window,omitempty"`
	Freshness  cpkit.Duration `json:"freshness,omitempty"`
	MinSamples int            `json:"min_samples,omitempty"`
	// Optional objectives are reported but do not decide the verdict.
	Optional    bool   `json:"optional,omitempty"`
	Description string `json:"description,omitempty"`
}

// Spec is a versioned set of objectives. AllOf must all hold; when AnyOf is
// set, at least one of it must hold too.
type Spec struct {
	Version   int            `json:"version"`
	AllOf     []Objective    `json:"all_of,omitempty"`
	AnyOf     []Objective    `json:"any_of,omitempty"`
	StableFor cpkit.Duration `json:"stable_for,omitempty"`
}

// Limits on a spec.
const (
	MaxObjectives = 32
	MaxWindow     = 24 * time.Hour
)

// ErrInvalidSpec wraps every validation failure.
var ErrInvalidSpec = errors.New("invalid outcome spec")

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidSpec, fmt.Sprintf(format, a...))
}

// Normalize validates the spec and fills defaults (in place). Targets are
// canonical resource refs.
func (s *Spec) Normalize() error {
	if s.Version == 0 {
		s.Version = 1
	}
	if s.Version != 1 {
		return invalid("unsupported version %d", s.Version)
	}
	if len(s.AllOf)+len(s.AnyOf) == 0 {
		return invalid("at least one objective is required")
	}
	if len(s.AllOf)+len(s.AnyOf) > MaxObjectives {
		return invalid("more than %d objectives", MaxObjectives)
	}
	if s.StableFor < 0 || s.StableFor.D() > MaxWindow {
		return invalid("stable_for out of range")
	}
	seen := map[string]bool{}
	for _, list := range [][]Objective{s.AllOf, s.AnyOf} {
		for i := range list {
			o := &list[i]
			if err := o.normalize(); err != nil {
				return err
			}
			if seen[o.ID] {
				return invalid("objective id %q is duplicated", o.ID)
			}
			seen[o.ID] = true
		}
	}
	return nil
}

func (o *Objective) normalize() error {
	o.ID = strings.TrimSpace(o.ID)
	if o.ID == "" || len(o.ID) > 64 {
		return invalid("objective id is required (max 64 chars)")
	}
	if o.TargetRef != "" {
		ref, err := cpkit.NormalizeRef(o.TargetRef)
		if err != nil {
			return invalid("objective %s: %v", o.ID, err)
		}
		o.TargetRef = ref
	}
	if o.Window < 0 || o.Window.D() > MaxWindow || o.Freshness < 0 || o.Freshness.D() > MaxWindow {
		return invalid("objective %s: window/freshness out of range", o.ID)
	}
	set := 0
	for _, p := range []bool{o.Metric != nil, o.State != nil, o.Event != nil} {
		if p {
			set++
		}
	}
	switch o.Kind {
	case KindMetric:
		if o.Metric == nil || set != 1 {
			return invalid("objective %s: metric objectives need exactly a metric expression", o.ID)
		}
		m := o.Metric
		if m.Source == "" || strings.TrimSpace(m.Query) == "" {
			return invalid("objective %s: metric source and query are required", o.ID)
		}
		op, ok := normalizeOp(m.Operator)
		if !ok {
			return invalid("objective %s: unknown operator %q", o.ID, m.Operator)
		}
		m.Operator = op
		switch m.Aggregate {
		case "":
			m.Aggregate = AggAll
		case AggAll, AggAvg, AggMax, AggMin, AggLast:
		default:
			return invalid("objective %s: unknown aggregate %q", o.ID, m.Aggregate)
		}
		if o.Window == 0 {
			return invalid("objective %s: metric objectives need a window", o.ID)
		}
		if o.MinSamples <= 0 {
			o.MinSamples = 1
		}
	case KindState:
		if o.State == nil || set != 1 || o.State.Predicate == "" {
			return invalid("objective %s: state objectives need a predicate (target_ref defaults to the Work subject)", o.ID)
		}
		n := 0
		if o.State.Equals != "" {
			n++
		}
		if len(o.State.In) > 0 {
			n++
		}
		if o.State.NotEquals != "" {
			n++
		}
		if n != 1 {
			return invalid("objective %s: set exactly one of equals, in, not_equals", o.ID)
		}
	case KindEvent:
		if o.Event == nil || set != 1 || o.Event.Type == "" || o.Window == 0 {
			return invalid("objective %s: event objectives need a type and a window", o.ID)
		}
	case KindHuman:
		if set != 0 {
			return invalid("objective %s: human objectives take no expression", o.ID)
		}
	default:
		return invalid("objective %s: unknown kind %q", o.ID, o.Kind)
	}
	return nil
}

func normalizeOp(op string) (string, bool) {
	op = strings.ToLower(strings.TrimSpace(op))
	switch op {
	case OpLT, OpLTE, OpGT, OpGTE, OpEQ, OpNE:
		return op, true
	}
	v, ok := opAliases[op]
	return v, ok
}

// Hash is the content hash an evaluation is bound to. A changed threshold is
// a changed spec: its stabilization history does not carry over.
func (s Spec) Hash() string { return cpkit.MustHash(s) }

// ObjectiveResult is the verdict of one objective.
type ObjectiveResult struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Verdict     Verdict  `json:"verdict"`
	Optional    bool     `json:"optional,omitempty"`
	Value       *float64 `json:"value,omitempty"`
	Observed    string   `json:"observed,omitempty"`
	Samples     int      `json:"samples,omitempty"`
	Reason      string   `json:"reason,omitempty"`
	EvidenceRef string   `json:"evidence_ref,omitempty"`
}

// Evaluation is the verdict of a spec at one time.
type Evaluation struct {
	ID       string  `json:"id,omitempty"`
	TenantID string  `json:"tenant_id,omitempty"`
	WorkID   string  `json:"work_id,omitempty"`
	SpecHash string  `json:"spec_hash"`
	Verdict  Verdict `json:"verdict"`
	// RawVerdict is the verdict before the stabilization rule.
	RawVerdict Verdict `json:"raw_verdict"`
	// SatisfiedSince is when the raw verdict became SATISFIED (continuously).
	SatisfiedSince *time.Time        `json:"satisfied_since,omitempty"`
	EvaluatedAt    time.Time         `json:"evaluated_at"`
	Objectives     []ObjectiveResult `json:"objectives"`
	Reasons        []string          `json:"reasons,omitempty"`
	Missing        []string          `json:"missing,omitempty"`
	EvidenceRefs   []string          `json:"evidence_refs,omitempty"`
	Stage          string            `json:"stage,omitempty"`
	Generation     int               `json:"generation,omitempty"`
}
