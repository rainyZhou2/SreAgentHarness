package domain

import (
	"encoding/json"
	"time"
)

// Commit contracts between the kernel (worker) and the state store.

// PlannedStep is a step the kernel decided to append after a commit.
type PlannedStep struct {
	Kind              StepKind
	Executor          string
	FlowNodeID        string
	InputRef          string
	InputHash         string
	Status            StepStatus // PENDING | WAITING_APPROVAL | SKIPPED
	StatusReason      string
	Intent            *ActionIntent
	RequiredApprovals int
	PolicyDecision    json.RawMessage
}

// WaitState keeps a committed step open without making it terminal.
type WaitState string

const (
	WaitNone     WaitState = ""
	WaitChildren WaitState = "children"
	WaitInput    WaitState = "input"
)

// FlowCommit is the atomic flow state update of a flow step.
type FlowCommit struct {
	FlowRunID    string
	CompiledRef  string
	CompiledHash string
	State        json.RawMessage
	Status       string
	CurrentNode  string
	ChildRuns    int
	Children     []ChildRunSpec
	// Generation is the plan generation of State. A higher generation than
	// the stored one starts a fresh set of node attempts (a replan).
	Generation int
}

// ChildRunSpec creates one child run in the same transaction as the parent
// flow step. The worker has already uploaded the flow-node goal artifact.
type ChildRunSpec struct {
	NodeID         string
	NodeType       string
	Attempt        int
	ProfileID      string
	ProfileVersion int64
	GoalRef        string
	GoalHash       string
	IdempotencyKey string
	InitialSteps   []PlannedStep
}

// EmittedEvent is an additional run event committed atomically with a step.
type EmittedEvent struct {
	Type    string
	Payload any
}

// StepSuccess is everything a worker commits for a successful attempt. All
// artifacts referenced here must already be durably uploaded.
type StepSuccess struct {
	OutputRef    string
	OutputHash   string
	ArtifactKind string          // decision | observation
	ContextHash  string          // phase ③ binding of the context snapshot
	Summary      json.RawMessage // event payload (no sensitive content)
	Plan         []PlannedStep   // steps appended after this one
	Complete     bool            // run completes with ResultRef
	ResultRef    string
	Wait         WaitState
	Flow         *FlowCommit
	KVWrites     []KVWrite // run-private only, applied atomically with the step
	SideEffect   bool      // confirm the external effect ledger entry
	// Checkpoint manifest uploaded by the worker (§29.4). Empty = none.
	CheckpointRef  string
	CheckpointHash string
	// Compaction (§33.4): a compact step records its summary and bumps the
	// run's compaction epoch.
	Compaction *Compaction
	// MemoryHits are committed with a retrieve_memory step and pinned for the
	// following reason steps (§10.2 ①).
	MemoryHits []MemoryHit
	// Thoughts is the explicit decision trace produced by this step.
	Thoughts []Thought
	// Events are extra plan/lifecycle events committed after step.succeeded.
	Events []EmittedEvent
	// Capabilities records which capabilities the step used or considered.
	Capabilities []CapabilityRef
}

type MemoryHit struct {
	MemoryID string  `json:"memory_id"`
	Version  int64   `json:"version"`
	Score    float64 `json:"score"`
	Rank     int     `json:"rank"`
}

type Compaction struct {
	SummaryRef      string          `json:"summary_ref"`
	Summary         json.RawMessage `json:"summary"`
	CompactedBefore int64           `json:"compacted_before"`
}

type FailureClass int

const (
	FailRetryable     FailureClass = iota // safe to retry: no side effect or idempotent
	FailPermanent                         // deterministic failure: fail the run
	FailUnknownEffect                     // side effect may have happened: never auto-retry
	FailCancelled                         // cooperative cancellation completed
)

type StepFailure struct {
	Class      FailureClass
	Code       string
	Message    string
	RetryAfter time.Duration
	TimedOut   bool
}

type EffectState string

const (
	EffectDispatched EffectState = "DISPATCHED" // proceed to call the tool
	EffectConfirmed  EffectState = "CONFIRMED"  // already done: reuse ResultRef, do not call again
)

type KVKey struct {
	TenantID string
	Scope    string // run | incident | agent | service | organization
	OwnerID  string
	Name     string
}

type KVWrite struct {
	Key             KVKey
	Data            []byte
	SchemaID        string
	TTL             time.Duration // 0 = no expiry
	ExpectedVersion *int64        // nil = PutIfAbsent, otherwise CAS
	Delete          bool          // tombstone at ExpectedVersion
}

// OutboxMessage is a pending broker publication written in a state transaction.
type OutboxMessage struct {
	ID       string
	TenantID string
	Subject  string
	Payload  json.RawMessage
	Attempt  int
}

// CreateRunRequest creates a run whose goal artifact is already stored.
type CreateRunRequest struct {
	TenantID       string
	IncidentID     *string
	ProfileID      string
	ProfileVersion int64
	GoalRef        string
	GoalHash       string
	IdempotencyKey string
	InitiatorType  string
	InitiatorID    string
	// v1.1 extensions
	SessionID         *string
	BranchID          *string
	TurnID            *string
	ParentRunID       *string
	ParentStepID      *string
	FlowNodeID        string
	FlowDepth         int
	PendingActivation bool    // queued conversation turn: not schedulable until activated (§29.3)
	ForkedFrom        *string // run_checkpoints.id
	RolloutID         *string
	InitialSteps      []PlannedStep // default: one reason step on the goal
	StepSeqStart      int64         // forks continue the source's step numbering
}
