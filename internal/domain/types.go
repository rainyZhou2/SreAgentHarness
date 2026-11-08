// Package domain holds the versioned entities, enums and contracts shared by
// every layer of the harness. It must not import database drivers, model SDKs
// or transport packages (GoAgentHarness.md §15.3, §20.1).
package domain

import (
	"encoding/json"
	"time"
)

// RunStatus is the persisted, worker-committed state of a Run (§4.2).
// PAUSE_REQUESTED / CANCELLING are not stored: they are derived from
// Status=RUNNING plus DesiredState.
type RunStatus string

const (
	RunPendingActivation RunStatus = "PENDING_ACTIVATION"
	RunQueued            RunStatus = "QUEUED"
	RunRunning           RunStatus = "RUNNING"
	RunPaused            RunStatus = "PAUSED"
	RunWaitingApproval   RunStatus = "WAITING_APPROVAL"
	RunWaitingChildren   RunStatus = "WAITING_CHILDREN"
	RunWaitingInput      RunStatus = "WAITING_INPUT"
	RunNeedsHuman        RunStatus = "NEEDS_HUMAN"
	RunCancelled         RunStatus = "CANCELLED"
	RunCompleted         RunStatus = "COMPLETED"
	RunFailed            RunStatus = "FAILED"
)

// Terminal reports whether no further transition is allowed.
func (s RunStatus) Terminal() bool {
	return s == RunCancelled || s == RunCompleted || s == RunFailed
}

type DesiredState string

const (
	DesiredRunning   DesiredState = "RUNNING"
	DesiredPaused    DesiredState = "PAUSED"
	DesiredCancelled DesiredState = "CANCELLED"
)

type StepKind string

const (
	StepReason   StepKind = "reason"
	StepTool     StepKind = "tool"
	StepRetrieve StepKind = "retrieve_memory"
	StepCompact  StepKind = "compact"
	StepFlow     StepKind = "flow"
	StepPlan     StepKind = "plan"
	// StepPlanReview is the human approval gate between a generated plan and
	// its execution. Its input hash is the plan hash.
	StepPlanReview StepKind = "plan_review"
)

type StepStatus string

const (
	StepPending         StepStatus = "PENDING"
	StepRunning         StepStatus = "RUNNING"
	StepWaitingApproval StepStatus = "WAITING_APPROVAL"
	StepWaitingChildren StepStatus = "WAITING_CHILDREN"
	StepWaitingInput    StepStatus = "WAITING_INPUT"
	StepRetryWait       StepStatus = "RETRY_WAIT"
	StepSucceeded       StepStatus = "SUCCEEDED"
	StepFailed          StepStatus = "FAILED"
	StepSkipped         StepStatus = "SKIPPED"
	StepCancelled       StepStatus = "CANCELLED"
	StepUnknown         StepStatus = "UNKNOWN"
	StepNeedsHuman      StepStatus = "NEEDS_HUMAN"
)

type AttemptStatus string

const (
	AttemptRunning   AttemptStatus = "RUNNING"
	AttemptSucceeded AttemptStatus = "SUCCEEDED"
	AttemptFailed    AttemptStatus = "FAILED"
	AttemptTimedOut  AttemptStatus = "TIMED_OUT"
	AttemptCancelled AttemptStatus = "CANCELLED"
	AttemptLost      AttemptStatus = "LOST"
	AttemptUnknown   AttemptStatus = "UNKNOWN"
)

// RiskClass follows §4.6.
type RiskClass string

const (
	RiskR0 RiskClass = "R0" // read-only
	RiskR1 RiskClass = "R1" // low-risk reversible change
	RiskR2 RiskClass = "R2" // production write with side effects
	RiskR3 RiskClass = "R3" // destructive / irreversible
)

// HasSideEffects reports whether a retry could duplicate an external effect.
func (r RiskClass) HasSideEffects() bool { return r != RiskR0 }

type Run struct {
	ID                string       `json:"id"`
	TenantID          string       `json:"tenant_id"`
	IncidentID        *string      `json:"incident_id,omitempty"`
	ProfileID         string       `json:"profile_id"`
	ProfileVersion    int64        `json:"profile_version"`
	Status            RunStatus    `json:"status"`
	DesiredState      DesiredState `json:"desired_state"`
	Version           int64        `json:"version"`
	LastEventSeq      int64        `json:"last_event_seq"`
	NextStepSeq       int64        `json:"next_step_seq"`
	NotBefore         *time.Time   `json:"not_before,omitempty"`
	ContextSnapshotID *string      `json:"context_snapshot_id,omitempty"`
	LeaseOwner        *string      `json:"lease_owner,omitempty"`
	LeaseUntil        *time.Time   `json:"lease_until,omitempty"`
	FenceToken        int64        `json:"fence_token"`
	GoalRef           string       `json:"goal_ref"`
	ResultRef         *string      `json:"result_ref,omitempty"`
	FailureReason     *string      `json:"failure_reason,omitempty"`
	InitiatorType     string       `json:"initiator_type"`
	InitiatorID       *string      `json:"initiator_id,omitempty"`
	SessionID         *string      `json:"session_id,omitempty"`
	BranchID          *string      `json:"branch_id,omitempty"`
	TurnID            *string      `json:"turn_id,omitempty"`
	ParentRunID       *string      `json:"parent_run_id,omitempty"`
	ParentStepID      *string      `json:"parent_step_id,omitempty"`
	FlowNodeID        string       `json:"flow_node_id,omitempty"`
	FlowDepth         int          `json:"flow_depth,omitempty"`
	ForkedFrom        *string      `json:"forked_from_checkpoint_id,omitempty"`
	CompactionEpoch   int64        `json:"compaction_epoch"`
	RolloutID         *string      `json:"rollout_id,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
	UpdatedAt         time.Time    `json:"updated_at"`
}

// DisplayStatus derives the transitional "pausing"/"cancelling" view (§4.2).
func (r Run) DisplayStatus() string {
	if r.Status == RunRunning {
		switch r.DesiredState {
		case DesiredPaused:
			return "PAUSE_REQUESTED"
		case DesiredCancelled:
			return "CANCELLING"
		}
	}
	return string(r.Status)
}

type Step struct {
	ID                string     `json:"id"`
	TenantID          string     `json:"tenant_id"`
	RunID             string     `json:"run_id"`
	Seq               int64      `json:"seq"`
	Kind              StepKind   `json:"kind"`
	ExecutorName      string     `json:"executor_name"`
	FlowNodeID        string     `json:"flow_node_id,omitempty"`
	Status            StepStatus `json:"status"`
	InputRef          string     `json:"input_ref"`
	InputHash         string     `json:"input_hash"`
	OutputRef         *string    `json:"output_ref,omitempty"`
	OutputHash        *string    `json:"output_hash,omitempty"`
	StatusReason      *string    `json:"status_reason,omitempty"`
	ContextSnapshotID *string    `json:"context_snapshot_id,omitempty"`
	CurrentAttempt    int        `json:"current_attempt"`
	NextAttemptAt     *time.Time `json:"next_attempt_at,omitempty"`
	OutputRevision    int        `json:"output_revision"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type Event struct {
	TenantID    string          `json:"tenant_id"`
	RunID       string          `json:"run_id"`
	Seq         int64           `json:"event_seq"`
	ID          string          `json:"event_id"`
	StepID      *string         `json:"step_id,omitempty"`
	AttemptID   *string         `json:"attempt_id,omitempty"`
	Type        string          `json:"event_type"`
	ActorType   string          `json:"actor_type"`
	ActorID     *string         `json:"actor_id,omitempty"`
	Payload     json.RawMessage `json:"payload"`
	OccurredAt  time.Time       `json:"occurred_at"`
	CommittedAt time.Time       `json:"committed_at"`
}

// ActionIntent is the exact operation identity an approval binds to (§4.6).
type ActionIntent struct {
	ID            string          `json:"id"`
	Environment   string          `json:"environment"`
	Target        string          `json:"target"`
	ToolName      string          `json:"tool_name"`
	ToolVersion   string          `json:"tool_version"`
	InputHash     string          `json:"input_hash"`
	ActionHash    string          `json:"action_hash"`
	RiskClass     RiskClass       `json:"risk_class"`
	PolicyVersion string          `json:"policy_version"`
	Effects       json.RawMessage `json:"effects"`
}

// Lease is what a successful claim hands to the worker (§14.2).
type Lease struct {
	TenantID          string
	RunID             string
	StepID            string
	StepSeq           int64
	AttemptID         string
	AttemptNo         int
	WorkerID          string
	FenceToken        int64
	Kind              StepKind
	ExecutorName      string
	FlowNodeID        string
	InputRef          string
	InputHash         string
	GoalRef           string
	ContextSnapshotID string
	IdempotencyKey    string
	Deadline          time.Time
	Intent            *ActionIntent
	ProfileID         string
	ProfileVersion    int64
	ParentRunID       string
	ParentStepID      string
	FlowDepth         int
}

// ToolCall is a complete, schema-validated tool intent produced by a model.
type ToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
	// Followup is set only on steps the kernel plans after an action
	// (verification, compensation); model decisions never carry it.
	Followup *ToolFollowup `json:"followup,omitempty"`
}

type Hypothesis struct {
	ID                 string   `json:"id"`
	Statement          string   `json:"statement"`
	Status             string   `json:"status"` // UNVERIFIED | VERIFIED | REFUTED
	ConfidenceEstimate float64  `json:"confidence_estimate"`
	Supports           []string `json:"supports,omitempty"`
	Refutes            []string `json:"refutes,omitempty"`
}

type DecisionKind string

const (
	DecisionFinal DecisionKind = "final"
	DecisionTool  DecisionKind = "tool"
)

// Intent is what the user asked for in this turn. It shapes the answer the
// operator sees; it never relaxes the policy gate.
type Intent string

const (
	IntentQuestion      Intent = "question"
	IntentTask          Intent = "task"
	IntentClarification Intent = "clarification"
)

// Decision is the auditable Decision Trace artifact of a reason step (§7.3).
// It is an explicitly produced explanation, not the model's hidden reasoning.
type Decision struct {
	SchemaVersion int          `json:"schema_version"`
	Kind          DecisionKind `json:"kind"`
	Intent        Intent       `json:"intent,omitempty"`
	ReasonSummary string       `json:"reason_summary"`
	Hypotheses    []Hypothesis `json:"hypotheses,omitempty"`
	Alternatives  []string     `json:"alternatives,omitempty"`
	Answer        string       `json:"answer,omitempty"`
	Tool          *ToolCall    `json:"tool,omitempty"`
	// Output is the structured result of a deterministic flow tool node.
	Output            json.RawMessage `json:"flow_output,omitempty"`
	EvidenceRefs      []string        `json:"evidence_refs,omitempty"`
	ContextSnapshotID string          `json:"context_snapshot_id"`
	// Model provenance (P0-B/P0-C): which ledger request and prompt manifest
	// produced this decision. Empty for non-model strategies.
	ModelRequestID      string `json:"model_request_id,omitempty"`
	ManifestRef         string `json:"manifest_ref,omitempty"`
	RenderedRequestHash string `json:"rendered_request_hash,omitempty"`
	Model               string `json:"model,omitempty"`
	// Additional tool calls the model proposed in the same response. V1
	// executes only the first; the rest are kept in the trace (§35.3).
	DeferredToolCalls []ToolCall    `json:"deferred_tool_calls,omitempty"`
	MemoryRefs        []string      `json:"memory_refs,omitempty"`
	Thought           *ThoughtChain `json:"thought,omitempty"`
	// Consistency is set when the conclusion was sampled several times.
	Consistency *Consistency `json:"consistency,omitempty"`
}

// Consistency records a self-consistency vote over sampled conclusions.
type Consistency struct {
	Samples int `json:"samples"`
	// Agreement is the share of samples in the chosen conclusion's cluster.
	Agreement float64 `json:"agreement"`
	// Alternatives summarizes the conclusions of the other clusters.
	Alternatives []string `json:"alternatives,omitempty"`
	// ModelRequestIDs are the extra samples' ledger requests.
	ModelRequestIDs []string `json:"model_request_ids,omitempty"`
}

// Observation is a tool result (or a policy/approval outcome) fed back to reasoning.
type Observation struct {
	StepSeq  int64           `json:"step_seq"`
	Tool     string          `json:"tool"`
	Args     json.RawMessage `json:"args,omitempty"`
	Outcome  string          `json:"outcome"` // ok | denied | rejected | expired | error
	Content  json.RawMessage `json:"content,omitempty"`
	Note     string          `json:"note,omitempty"`
	TrustTag string          `json:"trust"` // T2 trusted API result, T4 untrusted text, ...
	// Artifact is set when the full result was offloaded; Content then holds
	// a structured summary and the handle.
	Artifact *ObservationArtifact `json:"artifact,omitempty"`
	// Verification is set on verify steps.
	Verification *VerificationResult `json:"verification,omitempty"`
	// Injection lists instruction-like spans found in the full tool result
	// (also when it was offloaded and only a summary is shown).
	Injection []string `json:"injection_suspected,omitempty"`
}

// Evidence injected by an operator through the inject_evidence command.
type Evidence struct {
	Key       string    `json:"key"`
	Version   int64     `json:"version"`
	Content   string    `json:"content"`
	Source    string    `json:"source"`
	ActorID   string    `json:"actor_id"`
	CreatedAt time.Time `json:"created_at"`
}

type CommandType string

const (
	CmdPause          CommandType = "pause"
	CmdResume         CommandType = "resume"
	CmdCancel         CommandType = "cancel"
	CmdApproveTool    CommandType = "approve_tool"
	CmdRejectTool     CommandType = "reject_tool"
	CmdInjectEvidence CommandType = "inject_evidence"
	CmdResolveUnknown CommandType = "resolve_unknown"
	// v1.1 (§29.5, §35.6)
	CmdOverrideResult      CommandType = "override_result"
	CmdReplacePendingInput CommandType = "replace_pending_input"
	CmdRetry               CommandType = "retry"
	CmdMigrateProfile      CommandType = "migrate_profile"
	CmdProvideInput        CommandType = "provide_input"
)

type CommandStatus string

const (
	CommandPending  CommandStatus = "PENDING"
	CommandApplied  CommandStatus = "APPLIED"
	CommandRejected CommandStatus = "REJECTED"
)

type CommandRequest struct {
	Type            CommandType     `json:"type"`
	IdempotencyKey  string          `json:"-"`
	ActorID         string          `json:"-"`
	ExpectedVersion *int64          `json:"expected_version,omitempty"`
	Reason          string          `json:"reason,omitempty"`
	Payload         json.RawMessage `json:"payload,omitempty"`
	// Internal is computed server-side (artifact uploads, intents) and is
	// never accepted from the client.
	Internal *CommandInternal `json:"-"`
}

// CommandInternal carries values the API layer derived before the command
// transaction: uploaded artifacts and a server-computed action intent.
type CommandInternal struct {
	OutputRef         string
	OutputHash        string
	InputRef          string
	InputHash         string
	Intent            *ActionIntent
	PolicyDecision    json.RawMessage
	RequiredApprovals int
	Denied            string // policy denial reason for a replaced input
}

type Command struct {
	ID              string          `json:"id"`
	TenantID        string          `json:"tenant_id"`
	RunID           string          `json:"run_id"`
	Type            CommandType     `json:"type"`
	ActorID         string          `json:"actor_id"`
	ExpectedVersion *int64          `json:"expected_version,omitempty"`
	Status          CommandStatus   `json:"status"`
	RejectReason    *string         `json:"reject_reason,omitempty"`
	Payload         json.RawMessage `json:"payload"`
	RunVersionAfter *int64          `json:"run_version_after,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	AppliedAt       *time.Time      `json:"applied_at,omitempty"`
}

type ApprovalRequest struct {
	ID                string     `json:"id"`
	ActionIntentID    string     `json:"action_intent_id"`
	StepID            string     `json:"step_id"`
	ActionHash        string     `json:"action_hash"`
	RiskClass         RiskClass  `json:"risk_class"`
	ToolName          string     `json:"tool_name"`
	Target            string     `json:"target"`
	RequiredApprovals int        `json:"required_approvals"`
	Status            string     `json:"status"`
	ExpiresAt         time.Time  `json:"expires_at"`
	CreatedAt         time.Time  `json:"created_at"`
	DecidedAt         *time.Time `json:"decided_at,omitempty"`
}
