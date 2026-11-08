package work

import (
	"context"
	"encoding/json"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
)

// NewWork is a create request. The store dedups on (tenant, IdempotencyKey):
// a replay with the same request hash returns the existing Work; a replay
// with different parameters is ErrIdempotencyConflict.
type NewWork struct {
	TenantID       string
	Kind           Kind
	Title          string
	Goal           GoalSpec
	Template       Template
	Priority       int
	ActorType      string
	ActorID        string
	IdempotencyKey string
	SourceEvent    string
	Subjects       []SubjectLink
	Now            time.Time
}

// RequestHash binds an idempotency key to the request content.
func (n *NewWork) RequestHash() string {
	return cpkit.MustHash(struct {
		Kind     Kind
		Title    string
		Goal     GoalSpec
		Template string
		Version  int
		Priority int
	}{n.Kind, n.Title, n.Goal, n.Template.ID, n.Template.Version, n.Priority})
}

// Prepare validates and normalizes a create request.
func (n *NewWork) Prepare() error {
	if n.TenantID == "" {
		return invalidf("tenant is required")
	}
	if !ValidKind(n.Kind) {
		return invalidf("unknown kind %q", n.Kind)
	}
	if n.Title == "" {
		n.Title = n.Goal.Intent
	}
	if len(n.Title) > MaxTitle {
		return invalidf("title exceeds %d chars", MaxTitle)
	}
	if n.IdempotencyKey == "" || len(n.IdempotencyKey) > 200 {
		return invalidf("idempotency key is required (max 200 chars)")
	}
	if err := n.Goal.Normalize(); err != nil {
		return err
	}
	if err := n.Template.Normalize(); err != nil {
		return err
	}
	if n.Priority < -100 || n.Priority > 100 {
		return invalidf("priority must be within [-100,100]")
	}
	if n.ActorType == "" {
		n.ActorType = "user"
	}
	return nil
}

// CommandRecord makes an operator command idempotent: the first accepted
// command with a key stores its result; a replay with the same request hash
// returns that result, with a different hash ErrIdempotencyConflict.
type CommandRecord struct {
	Key         string
	Type        string
	RequestHash string
}

// Mutation is one atomic change of a Work. The store checks
// ExpectedVersion (CAS), validates the status transition, appends events
// with gap-free sequence numbers, and writes links, watches and outcomes —
// all in one transaction — then bumps the version.
type Mutation struct {
	TenantID        string
	WorkID          string
	ExpectedVersion int64
	ActorType       string
	ActorID         string
	Now             time.Time
	// LeaseOwner, when set, requires the caller to still hold the Work lease.
	LeaseOwner string

	Command *CommandRecord

	Status *Status
	Reason *string
	// Stage moves to a stage index; StageState replaces the stage scratch.
	Stage        *int
	StageState   json.RawMessage
	ClearState   bool
	Generation   *int
	AddReplans   int
	AddRuns      int
	AddCost      int64
	WakeAt       *time.Time
	ClearWake    bool
	ReleaseLease bool

	Events        []NewEvent
	Links         []RunLink
	LinkStatus    map[string]string // run id -> observed status
	Watches       []continuous.Watch
	CancelWatches []string
	// CancelAllWatches cancels every ACTIVE watch of the Work.
	CancelAllWatches bool
	Outcome          *outcome.Evaluation
	Subjects         []SubjectLink
}

// CheckMutation validates the parts of a mutation that do not need the
// stored state.
func CheckMutation(m *Mutation) error {
	if m.TenantID == "" || m.WorkID == "" {
		return invalidf("tenant and work id are required")
	}
	if m.Status != nil && !m.Status.Valid() {
		return invalidf("unknown status %q", *m.Status)
	}
	if m.Status != nil && *m.Status == StatusSucceeded && m.Outcome == nil {
		// Success needs an outcome record in the same transaction.
		return invalidf("SUCCEEDED requires an outcome evaluation")
	}
	if m.Outcome != nil && m.Status != nil && *m.Status == StatusSucceeded && m.Outcome.Verdict != outcome.Satisfied {
		return invalidf("SUCCEEDED requires a SATISFIED outcome")
	}
	if m.ActorType == "" {
		return invalidf("actor type is required")
	}
	for _, l := range m.Links {
		if l.Role != RoleOwner && l.Role != RoleEvidence {
			return invalidf("unknown link role %q", l.Role)
		}
	}
	return nil
}

// Filter selects Works for listing.
type Filter struct {
	TenantID string
	Status   []Status
	Kind     Kind
	Subject  string
	Limit    int
	// Before pages by (updated_at, id) descending.
	BeforeUpdated *time.Time
	BeforeID      string
}

// Store is the persistence port of the Work aggregate. Implementations:
// internal/store/cpsql (PostgreSQL and SQLite).
type Store interface {
	CreateWork(ctx context.Context, n NewWork) (Work, bool, error)
	GetWork(ctx context.Context, tenantID, workID string) (Work, error)
	ListWorks(ctx context.Context, f Filter) ([]Work, error)
	ApplyWork(ctx context.Context, m Mutation) (Work, error)
	ListWorkEvents(ctx context.Context, tenantID, workID string, afterSeq int64, limit int) ([]Event, error)
	ListRunLinks(ctx context.Context, tenantID, workID string) ([]RunLink, error)
	WorkForRun(ctx context.Context, tenantID, runID string) (RunLink, bool, error)
	ListSubjects(ctx context.Context, tenantID, workID string) ([]SubjectLink, error)
	ListWatches(ctx context.Context, tenantID, workID string, activeOnly bool) ([]continuous.Watch, error)
	ListOutcomes(ctx context.Context, tenantID, workID string, limit int) ([]outcome.Evaluation, error)
	// ClaimDueWorks leases up to limit Works whose wake_at has passed and
	// whose lease is free or expired.
	ClaimDueWorks(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]Work, error)
	// CountByStatus is the gauge source for harness_work_active.
	CountByStatus(ctx context.Context) (map[string]map[string]int, error)
}
