// Package continuous defines the durable Wait primitive of the control plane
// (RFC §9): a Work that waits for an event, a time, or a periodic check holds
// no worker, lease or model request. A watch is a persisted row; it fires
// exactly once per (watch, trigger) because firings are unique, and its
// ACTIVE → FIRED transition is a version CAS committed together with the
// Work wake-up.
//
// Due watches are found by a reconciler scan over next_check_at, so a lost
// timer or a restarted process never loses a wake-up.
package continuous

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
)

// Mode is what wakes a watch.
type Mode string

const (
	OnEvent  Mode = "event"    // a matching event
	OnTime   Mode = "time"     // a point in time (NotBefore)
	OnEither Mode = "either"   // a matching event, or the deadline as a timeout
	Periodic Mode = "periodic" // re-check every Interval until the deadline
	// OnApproval waits for a decision on a Work action (fired by the action
	// runtime, not by events).
	OnApproval Mode = "approval"
)

// Status of a watch.
type Status string

const (
	Active    Status = "ACTIVE"
	Fired     Status = "FIRED"
	Expired   Status = "EXPIRED"
	Cancelled Status = "CANCELLED"
)

// Terminal reports whether the watch can no longer fire.
func (s Status) Terminal() bool { return s != Active }

// Limits.
const (
	MaxInterval     = 24 * time.Hour
	MinInterval     = 5 * time.Second
	MaxWaitDuration = 30 * 24 * time.Hour
)

// Spec is what a stage asks to wait for.
type Spec struct {
	Mode      Mode                `json:"mode"`
	Predicate *eventing.Predicate `json:"predicate,omitempty"`
	Delay     cpkit.Duration      `json:"delay,omitempty"`    // OnTime: wake after Delay
	Interval  cpkit.Duration      `json:"interval,omitempty"` // Periodic
	Timeout   cpkit.Duration      `json:"timeout,omitempty"`  // deadline relative to registration
	Purpose   string              `json:"purpose,omitempty"`  // stage id / reason (audit)
	Meta      map[string]string   `json:"meta,omitempty"`
}

// Watch is a persisted wait.
type Watch struct {
	ID          string              `json:"id"`
	TenantID    string              `json:"tenant_id"`
	WorkID      string              `json:"work_id"`
	Mode        Mode                `json:"mode"`
	Predicate   *eventing.Predicate `json:"predicate,omitempty"`
	Subjects    []string            `json:"subjects,omitempty"`
	Purpose     string              `json:"purpose,omitempty"`
	Generation  int                 `json:"generation"`
	NotBefore   time.Time           `json:"not_before"`
	Deadline    *time.Time          `json:"deadline,omitempty"`
	NextCheckAt *time.Time          `json:"next_check_at,omitempty"`
	Interval    time.Duration       `json:"interval,omitempty"`
	Checks      int                 `json:"checks"`
	Cursor      string              `json:"cursor,omitempty"`
	Status      Status              `json:"status"`
	FiredBy     string              `json:"fired_by,omitempty"`
	Version     int64               `json:"version"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

// ErrInvalid wraps spec validation errors.
var ErrInvalid = errors.New("invalid watch")

// Build validates spec and turns it into a new ACTIVE watch for a Work.
func Build(spec Spec, tenantID, workID string, generation int, subjects []string, now time.Time) (Watch, error) {
	w := Watch{ID: cpkit.NewID(), TenantID: tenantID, WorkID: workID, Mode: spec.Mode, Purpose: spec.Purpose,
		Generation: generation, NotBefore: now, Status: Active, Version: 1, CreatedAt: now, UpdatedAt: now}
	if spec.Timeout < 0 || spec.Timeout.D() > MaxWaitDuration || spec.Delay < 0 || spec.Delay.D() > MaxWaitDuration {
		return w, fmt.Errorf("%w: timeout/delay out of range", ErrInvalid)
	}
	if spec.Timeout > 0 {
		d := now.Add(spec.Timeout.D())
		w.Deadline = &d
	}
	switch spec.Mode {
	case OnEvent, OnEither:
		if spec.Predicate == nil {
			return w, fmt.Errorf("%w: %s watches need a predicate", ErrInvalid, spec.Mode)
		}
		p := *spec.Predicate
		if len(p.SubjectRefs) == 0 {
			p.SubjectRefs = subjects
		}
		if err := p.Normalize(); err != nil {
			return w, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		w.Predicate, w.Subjects = &p, p.SubjectRefs
		if spec.Mode == OnEither && w.Deadline == nil {
			return w, fmt.Errorf("%w: either watches need a timeout", ErrInvalid)
		}
		w.NextCheckAt = w.Deadline // the timeout is the only timed check
	case OnTime:
		if spec.Delay <= 0 {
			return w, fmt.Errorf("%w: time watches need a delay", ErrInvalid)
		}
		at := now.Add(spec.Delay.D())
		w.NotBefore, w.NextCheckAt = at, &at
	case Periodic:
		iv := spec.Interval.D()
		if iv < MinInterval || iv > MaxInterval {
			return w, fmt.Errorf("%w: interval must be between %s and %s", ErrInvalid, MinInterval, MaxInterval)
		}
		if w.Deadline == nil {
			return w, fmt.Errorf("%w: periodic watches need a timeout", ErrInvalid)
		}
		at := now.Add(iv)
		w.Interval, w.NextCheckAt = iv, &at
	case OnApproval:
		w.NextCheckAt = w.Deadline
	default:
		return w, fmt.Errorf("%w: unknown mode %q", ErrInvalid, spec.Mode)
	}
	return w, nil
}

// TimerFiringID is the deterministic firing id of the n-th timed check, so a
// re-scanned due watch never records two firings for the same check.
func TimerFiringID(watchID string, check int) string {
	return "timer:" + watchID + ":" + strconv.Itoa(check)
}

// DueKind tells the orchestrator why a due watch woke up.
type DueKind string

const (
	DueTime     DueKind = "time"     // OnTime reached
	DuePeriodic DueKind = "periodic" // periodic re-check
	DueTimeout  DueKind = "timeout"  // deadline passed without a match
)

// DueReason computes why w is due at now (false: not due).
func DueReason(w Watch, now time.Time) (DueKind, bool) {
	if w.Status != Active || w.NextCheckAt == nil || w.NextCheckAt.After(now) {
		return "", false
	}
	if w.Deadline != nil && !w.Deadline.After(now) {
		return DueTimeout, true
	}
	switch w.Mode {
	case OnTime:
		return DueTime, true
	case Periodic:
		return DuePeriodic, true
	}
	return DueTimeout, true
}

// Firing records one wake-up of a watch.
type Firing struct {
	TenantID string          `json:"tenant_id"`
	WatchID  string          `json:"watch_id"`
	WorkID   string          `json:"work_id"`
	FiringID string          `json:"firing_id"` // event id or TimerFiringID
	Kind     string          `json:"kind"`      // event | time | periodic | timeout | approval
	Payload  json.RawMessage `json:"payload,omitempty"`
	FiredAt  time.Time       `json:"fired_at"`
}
