package eventing

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go-sre-harness/internal/cpkit"
)

// Predicate selects events. It is a whitelisted AST evaluated in Go — no
// expression language, no code execution — so it is safe to persist and to
// accept from operators and templates.
type Predicate struct {
	// EventTypes matches exactly, or by prefix with a trailing ".*".
	EventTypes []string `json:"event_types,omitempty"`
	// SubjectRefs are ref patterns (cpkit.RefMatches); empty matches any.
	SubjectRefs []string `json:"subject_refs,omitempty"`
	Sources     []string `json:"sources,omitempty"`
	Where       *Cond    `json:"where,omitempty"`
}

// Cond is one node of the where-tree.
type Cond struct {
	All []Cond `json:"all,omitempty"`
	Any []Cond `json:"any,omitempty"`
	Not *Cond  `json:"not,omitempty"`
	// Field is one of: type, source, subject_ref, correlation_id, dedup_key,
	// confidence, labels.<key>.
	Field  string   `json:"field,omitempty"`
	Op     string   `json:"op,omitempty"` // eq ne in prefix exists gt gte lt lte
	Value  string   `json:"value,omitempty"`
	Values []string `json:"values,omitempty"`
}

// Predicate limits.
const (
	maxCondNodes = 64
	maxCondDepth = 8
	maxListItems = 64
)

// ErrPredicate wraps predicate validation errors.
var ErrPredicate = errors.New("invalid predicate")

// Normalize validates the predicate and canonicalizes subject patterns.
func (p *Predicate) Normalize() error {
	if len(p.EventTypes) > maxListItems || len(p.SubjectRefs) > maxListItems || len(p.Sources) > maxListItems {
		return fmt.Errorf("%w: too many list items", ErrPredicate)
	}
	for i, t := range p.EventTypes {
		t = strings.ToLower(strings.TrimSpace(t))
		base := strings.TrimSuffix(t, ".*")
		if !typeRe.MatchString(base) {
			return fmt.Errorf("%w: event type %q", ErrPredicate, t)
		}
		p.EventTypes[i] = t
	}
	for i, r := range p.SubjectRefs {
		wild := strings.HasSuffix(r, "/*")
		n, err := cpkit.NormalizeRef(strings.TrimSuffix(r, "/*"))
		if err != nil {
			return fmt.Errorf("%w: subject %v", ErrPredicate, err)
		}
		if wild {
			n += "/*"
		}
		p.SubjectRefs[i] = n
	}
	if p.Where != nil {
		nodes := 0
		if err := p.Where.validate(0, &nodes); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cond) validate(depth int, nodes *int) error {
	*nodes++
	if *nodes > maxCondNodes || depth > maxCondDepth {
		return fmt.Errorf("%w: condition too large", ErrPredicate)
	}
	kinds := 0
	if len(c.All) > 0 {
		kinds++
	}
	if len(c.Any) > 0 {
		kinds++
	}
	if c.Not != nil {
		kinds++
	}
	if c.Field != "" {
		kinds++
	}
	if kinds != 1 {
		return fmt.Errorf("%w: a condition is exactly one of all, any, not or a field test", ErrPredicate)
	}
	for i := range c.All {
		if err := c.All[i].validate(depth+1, nodes); err != nil {
			return err
		}
	}
	for i := range c.Any {
		if err := c.Any[i].validate(depth+1, nodes); err != nil {
			return err
		}
	}
	if c.Not != nil {
		return c.Not.validate(depth+1, nodes)
	}
	if c.Field == "" {
		return nil
	}
	switch c.Field {
	case "type", "source", "subject_ref", "correlation_id", "dedup_key", "confidence":
	default:
		if k, ok := strings.CutPrefix(c.Field, "labels."); !ok || !labelRe.MatchString(k) {
			return fmt.Errorf("%w: unknown field %q", ErrPredicate, c.Field)
		}
	}
	if len(c.Values) > maxListItems {
		return fmt.Errorf("%w: too many values", ErrPredicate)
	}
	switch c.Op {
	case "eq", "ne", "prefix":
	case "in":
		if len(c.Values) == 0 {
			return fmt.Errorf("%w: in needs values", ErrPredicate)
		}
	case "exists":
	case "gt", "gte", "lt", "lte":
		if _, err := strconv.ParseFloat(c.Value, 64); err != nil {
			return fmt.Errorf("%w: %s needs a numeric value", ErrPredicate, c.Op)
		}
	default:
		return fmt.Errorf("%w: unknown op %q", ErrPredicate, c.Op)
	}
	return nil
}

// Match reports whether e satisfies the predicate. p must be normalized.
func (p *Predicate) Match(e *Envelope) bool {
	if p == nil {
		return true
	}
	if len(p.EventTypes) > 0 && !matchType(p.EventTypes, e.Type) {
		return false
	}
	if len(p.SubjectRefs) > 0 {
		ok := false
		for _, s := range p.SubjectRefs {
			if cpkit.RefMatches(s, e.SubjectRef) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(p.Sources) > 0 {
		ok := false
		for _, s := range p.Sources {
			if s == e.Source {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return p.Where == nil || p.Where.eval(e)
}

func matchType(patterns []string, t string) bool {
	for _, p := range patterns {
		if p == t {
			return true
		}
		if base, ok := strings.CutSuffix(p, ".*"); ok && strings.HasPrefix(t, base+".") {
			return true
		}
	}
	return false
}

func (c *Cond) eval(e *Envelope) bool {
	switch {
	case len(c.All) > 0:
		for i := range c.All {
			if !c.All[i].eval(e) {
				return false
			}
		}
		return true
	case len(c.Any) > 0:
		for i := range c.Any {
			if c.Any[i].eval(e) {
				return true
			}
		}
		return false
	case c.Not != nil:
		return !c.Not.eval(e)
	}
	v, present := field(e, c.Field)
	switch c.Op {
	case "exists":
		return present
	case "eq":
		return present && v == c.Value
	case "ne":
		return v != c.Value
	case "prefix":
		return present && strings.HasPrefix(v, c.Value)
	case "in":
		for _, x := range c.Values {
			if present && v == x {
				return true
			}
		}
		return false
	case "gt", "gte", "lt", "lte":
		a, err1 := strconv.ParseFloat(v, 64)
		b, err2 := strconv.ParseFloat(c.Value, 64)
		if !present || err1 != nil || err2 != nil {
			return false
		}
		switch c.Op {
		case "gt":
			return a > b
		case "gte":
			return a >= b
		case "lt":
			return a < b
		default:
			return a <= b
		}
	}
	return false
}

func field(e *Envelope, f string) (string, bool) {
	switch f {
	case "type":
		return e.Type, true
	case "source":
		return e.Source, true
	case "subject_ref":
		return e.SubjectRef, true
	case "correlation_id":
		return e.CorrelationID, e.CorrelationID != ""
	case "dedup_key":
		return e.DedupKey, e.DedupKey != ""
	case "confidence":
		return strconv.FormatFloat(e.Confidence, 'f', -1, 64), true
	}
	if k, ok := strings.CutPrefix(f, "labels."); ok {
		v, ok := e.Labels[k]
		return v, ok
	}
	return "", false
}
