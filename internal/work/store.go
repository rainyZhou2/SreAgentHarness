package work

import (
	"context"
	"encoding/json"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/outcome"
)

// NewWork is a create request. The store dedups on (tenant, IdempotencyKey):
// a replay with the same request hash returns the existing Work; a replay
// with different parameters is ErrIdempotencyConflict.
type NewWork struct {
	TenantID string
	Kind     Kind
	Title    string
	Goal     GoalSpec
	Template Template
	Priority int
	// Queue is the requested scheduling queue (empty: DefaultQueue).
	Queue          Queue
	ActorType      string
	ActorID        string
	IdempotencyKey string
	SourceEvent    string
	Subjects       []SubjectLink
	Now            time.Time
}

// RequestHash binds an idempotency key to the request content.
func (n *NewWork) RequestHash() string {
	if n.Queue != "" {
		return cpkit.MustHash(struct {
			Kind     Kind
			Title    string
			Goal     GoalSpec
			Template string
			Version  int
			Priority int
			Queue    Queue
		}{n.Kind, n.Title, n.Goal, n.Template.ID, n.Template.Version, n.Priority, n.Queue})
	}
	// Without a requested queue the hash is unchanged from before queues
	// existed, so replays of older requests still match.
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
	if err := n.Template.ValidateGoal(n.Goal); err != nil {
		return err
	}
	if n.Priority < -100 || n.Priority > 100 {
		return invalidf("priority must be within [-100,100]")
	}
	if n.ActorType == "" {
		n.ActorType = "user"
	}
	if n.Queue != "" && !ValidQueue(n.Queue) {
		return invalidf("unknown queue %q", n.Queue)
	}
	if q := n.ResolvedQueue(); !MayUseQueue(q, n.ActorType, n.ActorID) {
		return invalidf("queue %s is reserved for authenticated event sources and operators", q)
	}
	return nil
}

// ResolvedQueue is the queue the Work is created in.
func (n *NewWork) ResolvedQueue() Queue {
	if n.Queue != "" {
		return n.Queue
	}
	return DefaultQueue(n.Kind, n.Template)
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
	// Queue moves the Work to another scheduling queue.
	Queue *Queue
	// Goal amends the goal; only allowed together with a new Generation.
	Goal *GoalSpec
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
	LinkResults   map[string]LinkResult
	Watches       []continuous.Watch
	CancelWatches []string
	// CancelAllWatches cancels every ACTIVE watch of the Work.
	CancelAllWatches bool
	Outcome          *outcome.Evaluation
	CompleteCycle    bool
	Subjects         []SubjectLink
	// Findings and Hypotheses commit the adjudicated decision with the Work
	// CAS and stage transition. Hypothesis.Version is its expected version.
	Findings   []evidence.Finding
	Hypotheses []evidence.Hypothesis
}

// CheckMutation validates the parts of a mutation that do not need the
// stored state.
func CheckMutation(m *Mutation) error {
	if m.TenantID == "" || m.WorkID == "" {
		return invalidf("tenant and work id are required")
	}
	if (len(m.Findings) > 0 || len(m.Hypotheses) > 0) && m.ExpectedVersion <= 0 {
		return invalidf("a decision batch requires an expected Work version")
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
	if m.Queue != nil {
		if !ValidQueue(*m.Queue) {
			return invalidf("unknown queue %q", *m.Queue)
		}
		if !MayUseQueue(*m.Queue, m.ActorType, m.ActorID) {
			return invalidf("queue %s is reserved for authenticated event sources and operators", *m.Queue)
		}
	}
	for _, l := range m.Links {
		if l.Role != RoleOwner && l.Role != RoleEvidence {
			return invalidf("unknown link role %q", l.Role)
		}
	}
	if m.Goal != nil {
		if m.Generation == nil {
			return invalidf("a goal amendment needs a new generation")
		}
		if err := m.Goal.Normalize(); err != nil {
			return err
		}
	}
	for runID, r := range m.LinkResults {
		if r.CostMicros < 0 || len(r.Outcome) > 64 || len(r.EvidenceRefs) > 100 {
			return invalidf("link result of run %s is out of bounds", runID)
		}
	}
	return nil
}

// Filter selects Works for listing.
type Filter struct {
	TenantID string
	Status   []Status
	Kind     Kind
	Queue    Queue
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
	// ListGoals returns the goal of every generation, oldest first.
	ListGoals(ctx context.Context, tenantID, workID string) ([]GoalRecord, error)
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

// RunActivation authorizes a linked Run within the currently leased Work stage.
type RunActivation struct {
	TenantID        string
	WorkID          string
	RunID           string
	Generation      int
	StageID         string
	ExpectedVersion int64
	LeaseOwner      string
}

// PrepareOutcome binds omitted legacy identity fields without modifying the
// caller's evaluation, and rejects contradictory identity or chronology.
// It validates the evaluation contract; source authenticity remains the
// responsibility of the deterministic evaluator and its adapters.
func PrepareOutcome(w Work, input outcome.Evaluation, now time.Time, success bool) (outcome.Evaluation, error) {
	e := input
	if (e.TenantID != "" && e.TenantID != w.TenantID) || (e.WorkID != "" && e.WorkID != w.ID) {
		return e, invalidf("outcome belongs to another tenant or Work")
	}
	if e.Generation != 0 && e.Generation != w.Generation {
		return e, invalidf("outcome belongs to another generation")
	}
	if !e.Verdict.Valid() || !e.RawVerdict.Valid() || e.SpecHash == "" {
		return e, invalidf("outcome needs a spec hash and valid verdicts")
	}
	if e.EvaluatedAt.IsZero() || e.EvaluatedAt.After(now) {
		return e, invalidf("outcome evaluation time is missing or in the future")
	}
	if e.SatisfiedSince != nil && (e.SatisfiedSince.IsZero() || e.SatisfiedSince.After(e.EvaluatedAt)) {
		return e, invalidf("outcome stabilization time is invalid")
	}
	if e.Verdict == outcome.Satisfied && e.RawVerdict != outcome.Satisfied {
		return e, invalidf("SATISFIED outcome requires a SATISFIED raw verdict")
	}
	spec := w.Goal.Success
	if spec == nil && len(w.Template.Stages) > 0 {
		last := w.Template.Stages[len(w.Template.Stages)-1]
		if last.Type == StageVerify && last.Verify != nil {
			spec = last.Verify.Spec
		}
	}
	if success && spec != nil {
		if err := e.ValidateSpec(*spec); err != nil {
			return e, invalidf("SUCCEEDED requires the current goal outcome: %v", err)
		}
		if spec.StableFor > 0 && (e.SatisfiedSince == nil || e.EvaluatedAt.Sub(*e.SatisfiedSince) < spec.StableFor.D()) {
			return e, invalidf("SUCCEEDED requires the goal stabilization interval")
		}
		// An old evaluation cannot acquire freshness merely by being copied.
		ageLimit := time.Minute
		for _, list := range [][]outcome.Objective{spec.AllOf, spec.AnyOf} {
			for _, o := range list {
				if o.Optional {
					continue
				}
				limit := o.Freshness.D()
				if limit <= 0 {
					limit = o.Window.D()
				}
				if limit > 0 && limit < ageLimit {
					ageLimit = limit
				}
			}
		}
		if now.Sub(e.EvaluatedAt) > ageLimit {
			return e, invalidf("SUCCEEDED requires a fresh goal evaluation")
		}
	}
	e.TenantID, e.WorkID, e.Generation = w.TenantID, w.ID, w.Generation
	if e.ID == "" {
		e.ID = cpkit.NewID()
	}
	return e, nil
}
