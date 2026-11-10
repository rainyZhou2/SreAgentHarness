// Package work is the long-lived goal aggregate of the agent control plane
// (RFC §5). A Work outlives runs: it can span many runs, restarts and days,
// waits on durable watches instead of holding workers, and only succeeds
// when an independent outcome evaluation (or an explicit human
// confirmation) says the goal was reached — never because one run finished
// or a model said so.
//
// Every change is a Mutation applied with a version CAS; the store writes
// the new state, a gap-free work event, run links, watches and outcome
// records in one transaction. This package holds no SQL or HTTP code.
package work

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
)

// Kind classifies a Work. It selects a default template; it does not
// hard-code a workflow.
type Kind string

const (
	KindIncident   Kind = "incident"
	KindChange     Kind = "change"
	KindRoutine    Kind = "routine"
	KindRequest    Kind = "request"
	KindMonitoring Kind = "monitoring"
	KindCapacity   Kind = "capacity"
	KindAIInfra    Kind = "ai_infra"
	KindGeneral    Kind = "general"
)

// ValidKind reports whether k is known.
func ValidKind(k Kind) bool {
	switch k {
	case KindIncident, KindChange, KindRoutine, KindRequest, KindMonitoring, KindCapacity, KindAIInfra, KindGeneral:
		return true
	}
	return false
}

// Status is the lifecycle state.
type Status string

const (
	StatusNew       Status = "NEW"
	StatusPlanning  Status = "PLANNING"
	StatusActive    Status = "ACTIVE"
	StatusWaiting   Status = "WAITING"
	StatusVerifying Status = "VERIFYING"
	StatusBlocked   Status = "BLOCKED"
	StatusSuspended Status = "SUSPENDED"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusCancelled Status = "CANCELLED"
)

// AllStatuses lists every status (metrics, validation).
var AllStatuses = []Status{StatusNew, StatusPlanning, StatusActive, StatusWaiting, StatusVerifying,
	StatusBlocked, StatusSuspended, StatusSucceeded, StatusFailed, StatusCancelled}

// Terminal reports whether s is final.
func (s Status) Terminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusCancelled
}

// Valid reports whether s is known.
func (s Status) Valid() bool {
	for _, x := range AllStatuses {
		if x == s {
			return true
		}
	}
	return false
}

// Constraints bound what a Work may do. They come from the creator and the
// template; a model cannot change them.
type Constraints struct {
	// ProductionWrites: approval_required (default) | forbidden | preauthorized.
	ProductionWrites string `json:"production_writes,omitempty"`
	MaxBlastRadius   string `json:"max_blast_radius,omitempty"`
	// RequireFreshData bounds the age of world facts a write action relies on.
	RequireFreshData cpkit.Duration `json:"require_fresh_data,omitempty"`
	MaxCostUSD       float64        `json:"max_cost_usd,omitempty"`
	// DataClass classifies the Work's data (public | internal |
	// confidential | restricted; default internal). Data above the
	// deployment's model limit is never sent to a model provider.
	DataClass string `json:"data_class,omitempty"`
}

// Production write modes.
const (
	WritesApprovalRequired = "approval_required"
	WritesForbidden        = "forbidden"
	WritesPreauthorized    = "preauthorized"
)

// GoalSpec is the immutable goal of a Work generation.
type GoalSpec struct {
	Intent      string            `json:"intent"`
	Description string            `json:"description,omitempty"`
	SubjectRefs []string          `json:"subject_refs"`
	Success     *outcome.Spec     `json:"success,omitempty"`
	SuccessRef  string            `json:"success_ref,omitempty"`
	Constraints Constraints       `json:"constraints"`
	Deadline    *time.Time        `json:"deadline,omitempty"`
	ProfileRef  string            `json:"profile_ref,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
}

// Goal limits.
const (
	MaxSubjects    = 32
	MaxDescription = 8000
	MaxIntent      = 200
	MaxTitle       = 200
)

// ErrInvalid wraps validation failures (HTTP 422).
var ErrInvalid = errors.New("invalid work")

func invalidf(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, a...))
}

// Normalize validates the goal and canonicalizes refs and the success spec.
func (g *GoalSpec) Normalize() error {
	if g.Intent == "" || len(g.Intent) > MaxIntent {
		return invalidf("goal.intent is required (max %d chars)", MaxIntent)
	}
	if len(g.Description) > MaxDescription {
		return invalidf("goal.description exceeds %d chars", MaxDescription)
	}
	if len(g.SubjectRefs) > MaxSubjects {
		return invalidf("more than %d subjects", MaxSubjects)
	}
	refs, err := cpkit.NormalizeRefs(g.SubjectRefs)
	if err != nil {
		return invalidf("goal.subject_refs: %v", err)
	}
	g.SubjectRefs = refs
	if g.Success != nil {
		if err := g.Success.Normalize(); err != nil {
			return invalidf("goal.success: %v", err)
		}
	}
	switch g.Constraints.ProductionWrites {
	case "":
		g.Constraints.ProductionWrites = WritesApprovalRequired
	case WritesApprovalRequired, WritesForbidden, WritesPreauthorized:
	default:
		return invalidf("unknown production_writes %q", g.Constraints.ProductionWrites)
	}
	if g.Constraints.MaxCostUSD < 0 {
		return invalidf("max_cost_usd must not be negative")
	}
	switch g.Constraints.DataClass {
	case "":
		g.Constraints.DataClass = "internal"
	case "public", "internal", "confidential", "restricted":
	default:
		return invalidf("unknown data_class %q", g.Constraints.DataClass)
	}
	if len(g.Labels) > 32 {
		return invalidf("too many labels")
	}
	return nil
}

// PrimarySubject is the first subject ref ("" when none).
func (g *GoalSpec) PrimarySubject() string {
	if len(g.SubjectRefs) == 0 {
		return ""
	}
	return g.SubjectRefs[0]
}

// Work is the aggregate.
type Work struct {
	ID         string   `json:"id"`
	TenantID   string   `json:"tenant_id"`
	Kind       Kind     `json:"kind"`
	Status     Status   `json:"status"`
	Title      string   `json:"title"`
	Goal       GoalSpec `json:"goal"`
	GoalHash   string   `json:"goal_hash"`
	Template   Template `json:"template"`
	Generation int      `json:"generation"`
	// Stage is the index of the current template stage.
	Stage       int             `json:"stage"`
	StageState  json.RawMessage `json:"stage_state,omitempty"`
	Replans     int             `json:"replans"`
	RunsStarted int             `json:"runs_started"`
	CostMicros  int64           `json:"cost_micros"`
	Reason      string          `json:"reason,omitempty"`
	Priority    int             `json:"priority"`
	Queue       Queue           `json:"queue"`
	OwnerActor  string          `json:"owner_actor,omitempty"`
	SourceEvent string          `json:"source_event,omitempty"`
	Version     int64           `json:"version"`
	LastSeq     int64           `json:"last_event_seq"`
	WakeAt      *time.Time      `json:"wake_at,omitempty"`
	LeaseOwner  string          `json:"-"`
	LeaseUntil  *time.Time      `json:"-"`
	CreatedAt   time.Time       `json:"created_at"`
	UpdatedAt   time.Time       `json:"updated_at"`
	ClosedAt    *time.Time      `json:"closed_at,omitempty"`
}

// CurrentStage returns the active stage (false when past the end).
func (w *Work) CurrentStage() (Stage, bool) {
	if w.Stage < 0 || w.Stage >= len(w.Template.Stages) {
		return Stage{}, false
	}
	return w.Template.Stages[w.Stage], true
}

// RunLink roles.
const (
	RoleOwner    = "owner"
	RoleEvidence = "evidence"
)

// RunLink ties a run to a Work. A run has at most one owner Work.
type RunLink struct {
	TenantID   string `json:"tenant_id"`
	WorkID     string `json:"work_id"`
	RunID      string `json:"run_id"`
	Role       string `json:"role"`
	Phase      string `json:"phase"`
	StageID    string `json:"stage_id,omitempty"`
	AgentRole  string `json:"agent_role,omitempty"`
	Generation int    `json:"generation"`
	Required   bool   `json:"required"`
	// Status is the last observed run status (projection; the kernel is the
	// source of truth).
	Status    string    `json:"status,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	// Assignment results (owner links, RFC §13.2), set when the run ends.
	Outcome      string     `json:"outcome,omitempty"`
	CostMicros   int64      `json:"cost_micros,omitempty"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	EvidenceRefs []string   `json:"evidence_refs,omitempty"`
}

// LinkResult is the result of an assignment: how the run ended, what it
// cost and which evidence records it produced.
type LinkResult struct {
	Outcome      string
	CostMicros   int64
	FinishedAt   time.Time
	EvidenceRefs []string
}

// GoalRecord is the goal of one Work generation. A replan or reopen may
// amend the goal; earlier generations keep theirs for audit and replay.
type GoalRecord struct {
	Generation int       `json:"generation"`
	Goal       GoalSpec  `json:"goal"`
	GoalHash   string    `json:"goal_hash"`
	Reason     string    `json:"reason,omitempty"`
	ActorID    string    `json:"actor_id,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// SubjectLink maps a Work to an external subject (an existing incident, a
// correlation group, a ticket).
type SubjectLink struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Event is one committed entry of the Work timeline.
type Event struct {
	TenantID    string          `json:"tenant_id"`
	WorkID      string          `json:"work_id"`
	Seq         int64           `json:"work_seq"`
	ID          string          `json:"event_id"`
	Type        string          `json:"type"`
	ActorType   string          `json:"actor_type"`
	ActorID     string          `json:"actor_id,omitempty"`
	CausationID string          `json:"causation_id,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	OccurredAt  time.Time       `json:"occurred_at"`
}

// NewEvent is an event to append in a mutation.
type NewEvent struct {
	Type        string
	CausationID string
	Payload     any
}

// Errors returned by stores. They map onto HTTP status codes in the API.
var (
	ErrNotFound            = errors.New("work not found")                               // 404
	ErrVersionConflict     = errors.New("work version conflict")                        // 409
	ErrIdempotencyConflict = errors.New("idempotency key reused with other parameters") // 409
	ErrInvalidTransition   = errors.New("invalid work state transition")                // 422
	ErrOwnerConflict       = errors.New("run already has an owner work")                // 409
	ErrLeaseLost           = errors.New("work lease lost")
	// ErrLimit: a bound of the Work (RFC §20.3) refuses the change.
	ErrLimit = errors.New("work limit reached") // 409
)

// LimitError names the bound that refused a change; it matches ErrLimit.
type LimitError struct {
	Limit  string // max_watches, max_evidence, max_evidence_bytes, ...
	Detail string
}

func (e *LimitError) Error() string   { return "work limit reached: " + e.Limit + " (" + e.Detail + ")" }
func (e *LimitError) Is(t error) bool { return t == ErrLimit }

// LimitName returns the bound named by err, or "".
func LimitName(err error) string {
	var le *LimitError
	if errors.As(err, &le) {
		return le.Limit
	}
	return ""
}

// TransitionError explains a refused transition.
type TransitionError struct {
	From, To Status
}

func (e *TransitionError) Error() string {
	return fmt.Sprintf("%s: %s -> %s", ErrInvalidTransition, e.From, e.To)
}

func (e *TransitionError) Unwrap() error { return ErrInvalidTransition }
