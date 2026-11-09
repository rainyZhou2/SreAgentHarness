package eventing

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Rule actions.
const (
	// ActionCreate attaches to the open Work of the correlation group, or
	// creates one.
	ActionCreate = "create"
	// ActionAttach only attaches to an open Work (e.g. a "resolved" alert):
	// it never creates one.
	ActionAttach = "attach"
	// ActionIgnore records the event without touching any Work.
	ActionIgnore = "ignore"
)

// Correlation keys.
const (
	KeyCorrelationID = "correlation_id"
	KeyDedupKey      = "dedup_key"
	KeySubject       = "subject"
	// KeyLabelPrefix + name groups by a label value ("label:service").
	KeyLabelPrefix = "label:"
)

// Rule maps matching events onto Works. Rules are operator configuration:
// static rules decide correlation; a model may suggest but never overrides
// a subject or a permission (RFC §6.4).
type Rule struct {
	Name  string    `json:"name"`
	Match Predicate `json:"match"`
	// Key decides which events are "the same problem": correlation_id,
	// dedup_key, subject or label:<name>. Events without a key value fall
	// back to the subject.
	Key    string         `json:"key,omitempty"`
	Window cpkit.Duration `json:"window,omitempty"` // default 30m
	Action string         `json:"action,omitempty"` // create | attach | ignore (default create)
	// WorkKind and Template configure created Works.
	WorkKind string `json:"work_kind,omitempty"`
	Template string `json:"template,omitempty"`
	// Title is a short template; ${type}, ${subject}, ${source} and
	// ${label.<name>} are substituted (plain substitution, no expressions).
	Title    string `json:"title,omitempty"`
	Priority int    `json:"priority,omitempty"`
	// Intent becomes the GoalSpec intent of created Works.
	Intent string `json:"intent,omitempty"`
	// Group lets several rules feed one correlation group (default: the
	// rule name). Events of rules sharing a group and a key value land in
	// one Work, e.g. a deploy (change.deployed, attach) and the alert it
	// caused (alert.firing, create).
	Group string `json:"group,omitempty"`
	// Lookback, on a creating rule, attaches the group's events from the
	// lookback window that found no Work when they arrived (the deploy
	// that preceded the alert). At most 24h.
	Lookback cpkit.Duration `json:"lookback,omitempty"`
}

// GroupName is the correlation group of the rule.
func (r *Rule) GroupName() string {
	if r.Group != "" {
		return r.Group
	}
	return r.Name
}

// DefaultWindow is the correlation window when a rule sets none.
const DefaultWindow = 30 * time.Minute

// ErrRule wraps rule validation errors.
var ErrRule = errors.New("invalid correlation rule")

// Normalize validates the rule and fills defaults.
func (r *Rule) Normalize() error {
	if r.Name == "" || len(r.Name) > 64 || strings.ContainsAny(r.Name, " /\x00") {
		return fmt.Errorf("%w: name %q", ErrRule, r.Name)
	}
	if err := r.Match.Normalize(); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrRule, r.Name, err)
	}
	switch {
	case r.Key == "":
		r.Key = KeyDedupKey
	case r.Key == KeyCorrelationID || r.Key == KeyDedupKey || r.Key == KeySubject:
	case strings.HasPrefix(r.Key, KeyLabelPrefix) && labelRe.MatchString(strings.TrimPrefix(r.Key, KeyLabelPrefix)):
	default:
		return fmt.Errorf("%w: %s: unknown key %q", ErrRule, r.Name, r.Key)
	}
	if r.Window == 0 {
		r.Window = cpkit.Duration(DefaultWindow)
	}
	if r.Group != "" && (len(r.Group) > 64 || strings.ContainsAny(r.Group, " /|\x00")) {
		return fmt.Errorf("%w: %s: group %q", ErrRule, r.Name, r.Group)
	}
	if r.Lookback < 0 || r.Lookback.D() > 24*time.Hour {
		return fmt.Errorf("%w: %s: lookback must be within [0, 24h]", ErrRule, r.Name)
	}
	if r.Window < 0 || r.Window.D() > 7*24*time.Hour {
		return fmt.Errorf("%w: %s: window out of range", ErrRule, r.Name)
	}
	switch r.Action {
	case "":
		r.Action = ActionCreate
	case ActionCreate, ActionAttach, ActionIgnore:
	default:
		return fmt.Errorf("%w: %s: unknown action %q", ErrRule, r.Name, r.Action)
	}
	if len(r.Title) > 200 {
		return fmt.Errorf("%w: %s: title too long", ErrRule, r.Name)
	}
	return nil
}

// GroupKey is the correlation group of e under r. Events with the same group
// key within the window belong to one Work.
func (r *Rule) GroupKey(e *Envelope) string {
	var v string
	switch {
	case r.Key == KeyCorrelationID:
		v = e.CorrelationID
	case r.Key == KeyDedupKey:
		v = e.DedupKey
	case strings.HasPrefix(r.Key, KeyLabelPrefix):
		v = e.Labels[strings.TrimPrefix(r.Key, KeyLabelPrefix)]
	}
	if v == "" {
		v = "subject:" + e.SubjectRef
	} else {
		v = r.Key + ":" + v
	}
	return r.GroupName() + "|" + cpkit.HashBytes([]byte(v))[7:39]
}

// RenderTitle substitutes ${...} placeholders from the event. Unknown
// placeholders are left empty; nothing is evaluated.
func (r *Rule) RenderTitle(e *Envelope) string {
	t := r.Title
	if t == "" {
		t = "${type} on ${subject}"
	}
	var b strings.Builder
	for {
		i := strings.Index(t, "${")
		if i < 0 {
			b.WriteString(t)
			break
		}
		j := strings.Index(t[i:], "}")
		if j < 0 {
			b.WriteString(t)
			break
		}
		b.WriteString(t[:i])
		name := t[i+2 : i+j]
		switch {
		case name == "type":
			b.WriteString(e.Type)
		case name == "subject":
			b.WriteString(e.SubjectRef)
		case name == "source":
			b.WriteString(e.Source)
		case strings.HasPrefix(name, "label."):
			b.WriteString(e.Labels[strings.TrimPrefix(name, "label.")])
		}
		t = t[i+j+1:]
	}
	return cpkit.Truncate(strings.TrimSpace(b.String()), 200)
}

// Router picks the first matching rule.
type Router struct {
	rules []Rule
}

// NewRouter validates rules (order is significant: first match wins).
func NewRouter(rules []Rule) (*Router, error) {
	seen := map[string]bool{}
	out := make([]Rule, 0, len(rules))
	for _, r := range rules {
		if err := r.Normalize(); err != nil {
			return nil, err
		}
		if seen[r.Name] {
			return nil, fmt.Errorf("%w: %s is defined twice", ErrRule, r.Name)
		}
		seen[r.Name] = true
		out = append(out, r)
	}
	return &Router{rules: out}, nil
}

// Route returns the first rule matching e.
func (rt *Router) Route(e *Envelope) (Rule, bool) {
	if rt == nil {
		return Rule{}, false
	}
	for _, r := range rt.rules {
		if r.Match.Match(e) {
			return r, true
		}
	}
	return Rule{}, false
}

// Rules returns the configured rules.
func (rt *Router) Rules() []Rule {
	if rt == nil {
		return nil
	}
	return append([]Rule(nil), rt.rules...)
}
