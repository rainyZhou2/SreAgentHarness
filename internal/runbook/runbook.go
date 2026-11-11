// Package runbook is the registry of operational runbooks (design §12.2).
// A runbook is metadata over a Work template: who owns it, what it applies
// to, which actions it may use, until when it is valid and when it was
// last rehearsed — plus its track record. It is not a second workflow
// engine: the stages are the template's, and every action still goes
// through the action runtime.
//
// The registry answers one question before a runbook is used for a
// production action: is this runbook fit to be trusted right now? An
// expired, unrehearsed or retired runbook still explains what to do; it no
// longer justifies doing it automatically.
package runbook

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Statuses.
const (
	Draft   = "DRAFT"
	Active  = "ACTIVE"
	Expired = "EXPIRED"
	Retired = "RETIRED"
)

// Spec is the operator-authored runbook.
type Spec struct {
	ID        string    `json:"id"`
	Version   int       `json:"version"`
	Owner     string    `json:"owner"`
	AppliesTo AppliesTo `json:"applies_to"`
	Checks    Checks    `json:"checks,omitempty"`
	Safety    Safety    `json:"safety"`
	Lifecycle Lifecycle `json:"lifecycle"`
	Summary   string    `json:"summary,omitempty"`
}

// AppliesTo scopes a runbook.
type AppliesTo struct {
	SubjectKinds         []string `json:"subject_kinds,omitempty"`
	AlertTypes           []string `json:"alert_types,omitempty"`
	Environments         []string `json:"environments,omitempty"`
	Services             []string `json:"services,omitempty"` // ref patterns
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	CompatibleActions    []string `json:"compatible_actions,omitempty"`
}

// Checks name the conditions around the runbook (documentation and
// template wiring; the checks themselves are outcome specs and preflights).
type Checks struct {
	Preconditions  []string `json:"preconditions,omitempty"`
	Postconditions []string `json:"postconditions,omitempty"`
}

// Safety is how carefully a runbook must be used.
type Safety struct {
	AutoExecute   bool `json:"auto_execute"`
	HumanApproval bool `json:"human_approval"`
	// ExpireAfter is how long the runbook stays valid after its last
	// review or rehearsal (0: never expires).
	ExpireAfter cpkit.Duration `json:"expire_after,omitempty"`
	// RehearsalRequired: a production action needs a rehearsal within
	// ExpireAfter (or ever, when ExpireAfter is 0).
	RehearsalRequired bool `json:"rehearsal_required,omitempty"`
}

// Lifecycle ties the runbook to its template.
type Lifecycle struct {
	TemplateID      string `json:"template_id"`
	TemplateVersion int    `json:"template_version,omitempty"`
}

// Approval is one recorded review of a runbook.
type Approval struct {
	Approver string    `json:"approver"`
	Comment  string    `json:"comment,omitempty"`
	At       time.Time `json:"at"`
}

// Runbook is a stored runbook with its track record.
type Runbook struct {
	Spec
	TenantID        string     `json:"tenant_id"`
	Status          string     `json:"status"`
	LastRehearsedAt *time.Time `json:"last_rehearsed_at,omitempty"`
	Executions      int64      `json:"executions"`
	Successes       int64      `json:"successes"`
	// UnknownEffects counts executions that left an action effect unknown.
	UnknownEffects int64      `json:"unknown_effects"`
	Approvals      []Approval `json:"approvals,omitempty"`
	RowVersion     int64      `json:"row_version"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// SuccessRate is successes over executions (0 when never executed).
func (r Runbook) SuccessRate() float64 {
	if r.Executions == 0 {
		return 0
	}
	return float64(r.Successes) / float64(r.Executions)
}

// Errors.
var (
	ErrInvalid         = errors.New("invalid runbook")
	ErrNotFound        = errors.New("runbook not found")
	ErrVersionConflict = errors.New("runbook version conflict")
)

var idRe = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// Normalize validates a spec.
func (s *Spec) Normalize() error {
	bad := func(format string, a ...any) error {
		return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
	}
	if !idRe.MatchString(s.ID) {
		return bad("id %q", s.ID)
	}
	if s.Version <= 0 {
		s.Version = 1
	}
	if strings.TrimSpace(s.Owner) == "" || len(s.Owner) > 128 {
		return bad("owner is required")
	}
	if s.Lifecycle.TemplateID == "" {
		return bad("lifecycle.template_id is required")
	}
	if s.Safety.ExpireAfter < 0 || s.Safety.ExpireAfter.D() > 3*365*24*time.Hour {
		return bad("safety.expire_after is out of range")
	}
	if s.Safety.AutoExecute && s.Safety.HumanApproval {
		return bad("auto_execute and human_approval contradict each other")
	}
	for _, p := range s.AppliesTo.Services {
		if _, err := cpkit.NormalizeRef(strings.TrimSuffix(p, "/*")); err != nil {
			return bad("applies_to.services: %v", err)
		}
	}
	for _, l := range [][]string{s.AppliesTo.SubjectKinds, s.AppliesTo.AlertTypes, s.AppliesTo.Environments,
		s.AppliesTo.RequiredCapabilities, s.AppliesTo.CompatibleActions, s.AppliesTo.Services} {
		if len(l) > 64 {
			return bad("too many applies_to entries")
		}
	}
	s.Summary = cpkit.Truncate(s.Summary, 2000)
	return nil
}

// Applies reports whether the runbook covers a subject, alert type and
// environment (empty selectors match anything).
func (s Spec) Applies(subject, alertType, environment string) bool {
	in := func(list []string, v string) bool {
		if len(list) == 0 || v == "" {
			return true
		}
		for _, x := range list {
			if x == v || (strings.HasSuffix(x, ".*") && strings.HasPrefix(v, strings.TrimSuffix(x, "*"))) {
				return true
			}
		}
		return false
	}
	if !in(s.AppliesTo.SubjectKinds, cpkit.RefScheme(subject)) || !in(s.AppliesTo.AlertTypes, alertType) ||
		!in(s.AppliesTo.Environments, environment) {
		return false
	}
	if len(s.AppliesTo.Services) == 0 || subject == "" {
		return true
	}
	for _, p := range s.AppliesTo.Services {
		if cpkit.RefMatches(p, subject) {
			return true
		}
	}
	return false
}

// Problems lists why the runbook must not carry a production action now:
// not active, past its validity, never or too long ago rehearsed, or the
// action is not one it was written for. Nil means it is fit.
func (r Runbook) Problems(actionName string, now time.Time) []string {
	var out []string
	if r.Status != Active {
		out = append(out, fmt.Sprintf("runbook %s@%d is %s", r.ID, r.Version, r.Status))
	}
	exp := r.Safety.ExpireAfter.D()
	if exp > 0 {
		last := r.UpdatedAt
		if r.LastRehearsedAt != nil && r.LastRehearsedAt.After(last) {
			last = *r.LastRehearsedAt
		}
		for _, a := range r.Approvals {
			if a.At.After(last) {
				last = a.At
			}
		}
		if now.Sub(last) > exp {
			out = append(out, fmt.Sprintf("runbook %s@%d expired: last reviewed or rehearsed %s ago (limit %s)", r.ID, r.Version,
				now.Sub(last).Round(time.Hour), exp))
		}
	}
	if r.Safety.RehearsalRequired {
		switch {
		case r.LastRehearsedAt == nil:
			out = append(out, fmt.Sprintf("runbook %s@%d was never rehearsed", r.ID, r.Version))
		case exp > 0 && now.Sub(*r.LastRehearsedAt) > exp:
			out = append(out, fmt.Sprintf("runbook %s@%d was last rehearsed %s ago", r.ID, r.Version,
				now.Sub(*r.LastRehearsedAt).Round(time.Hour)))
		}
	}
	if actionName != "" && len(r.AppliesTo.CompatibleActions) > 0 {
		ok := false
		for _, a := range r.AppliesTo.CompatibleActions {
			ok = ok || a == actionName
		}
		if !ok {
			out = append(out, fmt.Sprintf("action %s is not one runbook %s was written for", actionName, r.ID))
		}
	}
	return out
}

// Result is how a runbook execution ended.
type Result struct {
	Succeeded     bool
	UnknownEffect bool
}

// Store is the persistence port (internal/store/cpsql).
type Store interface {
	// PutRunbook creates or CAS-updates a runbook version (expected 0:
	// create).
	PutRunbook(ctx context.Context, tenantID string, s Spec, status string, expected int64) (Runbook, error)
	// GetRunbook reads a version (0: the newest).
	GetRunbook(ctx context.Context, tenantID, id string, version int) (Runbook, error)
	ListRunbooks(ctx context.Context, tenantID string) ([]Runbook, error)
	// RecordRehearsal stamps a rehearsal and optionally an approval.
	RecordRehearsal(ctx context.Context, tenantID, id string, version int, at time.Time, approval *Approval) (Runbook, error)
	// RecordExecution adds one execution to the track record.
	RecordExecution(ctx context.Context, tenantID, id string, version int, r Result) error
}

// Match returns the active runbooks that apply, best track record first.
func Match(all []Runbook, subject, alertType, environment string, now time.Time) []Runbook {
	var out []Runbook
	for _, r := range all {
		if r.Status == Active && r.Applies(subject, alertType, environment) && len(r.Problems("", now)) == 0 {
			out = append(out, r)
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].SuccessRate() > out[j-1].SuccessRate(); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
