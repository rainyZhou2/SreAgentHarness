// Package action is the controlled-operation runtime of the control plane
// (RFC §10). A Tool is a capability; an Action is one governed use of a
// side-effecting capability against one resource, with its own lifecycle:
//
//	PROPOSED → (preflight) → REJECTED | WAITING_APPROVAL | READY
//	READY → DISPATCHED (ledger written BEFORE the call) → APPLIED | EFFECT_UNKNOWN | FAILED
//	EFFECT_UNKNOWN → APPLIED (inspect / human) | MANUAL_RECOVERY | FAILED (human: not applied)
//	APPLIED → VERIFYING → VERIFIED | DEGRADED
//	DEGRADED → COMPENSATION_PENDING → COMPENSATED
//
// Invariants:
//   - the plan is built by the server; a model or client never submits an
//     intent, and every approval binds the plan's intent hash (v2), so any
//     change of target, target version, args, policy, risk, snapshot or
//     generation invalidates it;
//   - the risk class comes from the operator-owned descriptor and the
//     policy gate floor (R3 denied, R2 needs approval) cannot be relaxed;
//   - DISPATCHED is committed before the external call; an unknown result is
//     never retried automatically — it is inspected (read-only) or handed to
//     a human ("may have been dispatched");
//   - one resource has at most one in-flight action (target lock), and the
//     target version is re-checked right before dispatch.
package action

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/outcome"
)

// State of an action.
type State string

const (
	StateProposed            State = "PROPOSED"
	StateRejected            State = "REJECTED"
	StateWaitingApproval     State = "WAITING_APPROVAL"
	StateReady               State = "READY"
	StateDispatched          State = "DISPATCHED"
	StateApplied             State = "APPLIED"
	StateFailed              State = "FAILED"
	StateEffectUnknown       State = "EFFECT_UNKNOWN"
	StateManualRecovery      State = "MANUAL_RECOVERY"
	StateVerifying           State = "VERIFYING"
	StateVerified            State = "VERIFIED"
	StateDegraded            State = "DEGRADED"
	StateCompensationPending State = "COMPENSATION_PENDING"
	StateCompensated         State = "COMPENSATED"
)

var transitions = map[State][]State{
	StateProposed:            {StateRejected, StateWaitingApproval, StateReady},
	StateWaitingApproval:     {StateReady, StateRejected},
	StateReady:               {StateDispatched, StateRejected},
	StateDispatched:          {StateApplied, StateEffectUnknown, StateFailed},
	StateEffectUnknown:       {StateApplied, StateManualRecovery, StateFailed},
	StateManualRecovery:      {StateApplied, StateFailed},
	StateApplied:             {StateVerifying, StateVerified, StateDegraded},
	StateVerifying:           {StateVerified, StateDegraded},
	StateDegraded:            {StateCompensationPending},
	StateCompensationPending: {StateCompensated, StateDegraded},
}

// CanTransition reports whether from → to is allowed.
func CanTransition(from, to State) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Terminal reports whether no automatic progress is possible. MANUAL_RECOVERY
// and DEGRADED wait for humans or a compensation and are not terminal.
func (s State) Terminal() bool {
	switch s {
	case StateRejected, StateFailed, StateVerified, StateCompensated:
		return true
	}
	return false
}

// Settled reports whether the action no longer holds its target (no effect
// can still be in flight).
func (s State) Settled() bool {
	switch s {
	case StateRejected, StateFailed, StateVerified, StateCompensated, StateDegraded, StateWaitingApproval, StateProposed, StateReady:
		return true
	}
	return false
}

// Idempotency modes of a downstream system.
const (
	IdemNative           = "NATIVE"             // the executor passes the key and the downstream dedups
	IdemQueryBeforeRetry = "QUERY_BEFORE_RETRY" // the effect can be inspected
	IdemNone             = "NONE"               // neither: unknown results always go to a human
)

// Descriptor is the operator-owned contract of an action.
type Descriptor struct {
	Name    string `json:"name"`
	Version string `json:"version"`
	// ResourceKinds are the ref schemes the action may target ("k8s", "service").
	ResourceKinds []string         `json:"resource_kinds"`
	Risk          domain.RiskClass `json:"risk"`
	Description   string           `json:"description,omitempty"`
	InputSchema   json.RawMessage  `json:"input_schema,omitempty"`
	// IdempotencyMode: NATIVE | QUERY_BEFORE_RETRY | NONE.
	IdempotencyMode  string `json:"idempotency_mode"`
	SupportsDryRun   bool   `json:"supports_dry_run,omitempty"`
	SupportsRollback bool   `json:"supports_rollback,omitempty"`
	// Compensation names the action that undoes this one (when supported).
	Compensation string `json:"compensation,omitempty"`
	// RequiredFacts must be present and fresh for the target before dispatch.
	RequiredFacts []string `json:"required_facts,omitempty"`
	// VersionFact is the predicate holding the target's resource version
	// (resourceVersion, revision); it is pinned into the plan and re-checked
	// before dispatch.
	VersionFact string `json:"version_fact,omitempty"`
	// BlastRadius is declared by the operator (single_resource, one_service…).
	BlastRadius string `json:"blast_radius,omitempty"`
	// Timeout bounds Execute (default 60s).
	Timeout time.Duration `json:"-"`
}

// PreflightResult is a read-only check of a plan.
type PreflightResult struct {
	Allowed        bool      `json:"allowed"`
	Reasons        []string  `json:"reasons,omitempty"`
	SnapshotID     string    `json:"snapshot_id,omitempty"`
	SnapshotDigest string    `json:"snapshot_digest,omitempty"`
	TargetVersion  string    `json:"target_version,omitempty"`
	EvidenceRefs   []string  `json:"evidence_refs,omitempty"`
	CheckedAt      time.Time `json:"checked_at"`
}

// InspectResult is a read-only reconciliation of an uncertain effect.
type InspectResult struct {
	// Applied is nil when the executor cannot tell.
	Applied *bool           `json:"applied,omitempty"`
	Detail  json.RawMessage `json:"detail,omitempty"`
}

// Executor errors.
var (
	// ErrNotApplied wraps executor errors that prove nothing was changed
	// (validation, 4xx before any write). Every other error is unknown.
	ErrNotApplied = errors.New("action was not applied")
	// ErrUnsupported: the executor cannot inspect or compensate.
	ErrUnsupported = errors.New("operation not supported by this action")
)

// Executor implements one action. Execute is called only by the runtime,
// after the gate and the ledger; Preflight and Inspect must be read-only.
type Executor interface {
	Descriptor() Descriptor
	Preflight(ctx context.Context, p Plan) (PreflightResult, error)
	Execute(ctx context.Context, p Plan, idempotencyKey string) (json.RawMessage, error)
	Inspect(ctx context.Context, p Plan) (InspectResult, error)
}

// Plan is the server-built, immutable description of one action.
type Plan struct {
	ID             string           `json:"id"`
	TenantID       string           `json:"tenant_id"`
	WorkID         string           `json:"work_id"`
	RunID          string           `json:"run_id,omitempty"`
	StageID        string           `json:"stage_id,omitempty"`
	Generation     int              `json:"generation"`
	ActorScope     string           `json:"actor_scope"`
	ActionName     string           `json:"action_name"`
	ActionVersion  string           `json:"action_version"`
	TargetRef      string           `json:"target_ref"`
	TargetVersion  string           `json:"target_version,omitempty"`
	Args           json.RawMessage  `json:"args"`
	ArgsHash       string           `json:"args_hash"`
	Risk           domain.RiskClass `json:"risk"`
	BlastRadius    string           `json:"blast_radius,omitempty"`
	PolicyVersion  string           `json:"policy_version"`
	PolicyVerdict  string           `json:"policy_verdict"`
	PolicyReason   string           `json:"policy_reason,omitempty"`
	Approvals      int              `json:"required_approvals"`
	SnapshotID     string           `json:"snapshot_id,omitempty"`
	SnapshotDigest string           `json:"snapshot_digest,omitempty"`
	Effect         *outcome.Spec    `json:"effect,omitempty"`
	CompensatesID  string           `json:"compensates_id,omitempty"`
	ProposedBy     string           `json:"proposed_by"`
	CreatedAt      time.Time        `json:"created_at"`
	ExpiresAt      time.Time        `json:"expires_at"`
	// PlanHash identifies the plan content (dedup per Work); IntentHash is
	// ActionHashV2, the value approvals bind to.
	PlanHash   string `json:"plan_hash"`
	IntentHash string `json:"intent_hash"`
	// EffectKey is the stable idempotency key passed downstream.
	EffectKey string `json:"effect_key"`
}

// IntentSchemaVersion of IntentHash.
const IntentSchemaVersion = 2

// Seal computes the hashes and the effect key. Any field an approval relies
// on is inside IntentHash (RFC §10.5).
func (p *Plan) Seal() {
	p.ArgsHash = cpkit.MustHash(p.Args)
	p.IntentHash = cpkit.MustHash(struct {
		V              int    `json:"intent_schema_version"`
		Tenant         string `json:"tenant_id"`
		ActorScope     string `json:"actor_scope"`
		Work           string `json:"work_id"`
		Run            string `json:"run_id"`
		Name           string `json:"action_name"`
		Version        string `json:"action_version"`
		Target         string `json:"target_ref"`
		TargetVersion  string `json:"target_resource_version"`
		Args           string `json:"args_hash"`
		Policy         string `json:"policy_version"`
		Risk           string `json:"risk_class"`
		Blast          string `json:"blast_radius"`
		SnapshotDigest string `json:"preflight_snapshot_digest"`
		Generation     int    `json:"plan_generation"`
		Expiry         int64  `json:"approval_expiry"`
	}{IntentSchemaVersion, p.TenantID, p.ActorScope, p.WorkID, p.RunID, p.ActionName, p.ActionVersion, p.TargetRef,
		p.TargetVersion, p.ArgsHash, p.PolicyVersion, string(p.Risk), p.BlastRadius, p.SnapshotDigest, p.Generation,
		p.ExpiresAt.Unix()})
	p.PlanHash = cpkit.MustHash(struct {
		Intent  string
		Stage   string
		Effect  *outcome.Spec
		Compens string
	}{p.IntentHash, p.StageID, p.Effect, p.CompensatesID})
	p.EffectKey = "action:" + p.TenantID + ":" + p.PlanHash[7:39]
}

// BreakGlassGrant records an emergency release of a held action.
type BreakGlassGrant struct {
	Actor         string    `json:"actor"`
	Justification string    `json:"justification"`
	IntentHash    string    `json:"intent_hash"`
	At            time.Time `json:"at"`
}

// Approval is one decision on an action.
type Approval struct {
	ApproverID string    `json:"approver_id"`
	Decision   string    `json:"decision"` // approve | reject
	IntentHash string    `json:"intent_hash"`
	Comment    string    `json:"comment,omitempty"`
	At         time.Time `json:"at"`
}

// Record is the persisted action (the Work-level effect ledger entry).
type Record struct {
	Plan
	State          State               `json:"state"`
	Version        int64               `json:"version"`
	Preflight      *PreflightResult    `json:"preflight,omitempty"`
	ApprovalLog    []Approval          `json:"approvals,omitempty"`
	Result         json.RawMessage     `json:"result,omitempty"`
	Inspect        *InspectResult      `json:"inspect,omitempty"`
	EffectEval     *outcome.Evaluation `json:"effect_evaluation,omitempty"`
	LastError      string              `json:"last_error,omitempty"`
	Reason         string              `json:"reason,omitempty"`
	DispatchedAt   *time.Time          `json:"dispatched_at,omitempty"`
	UpdatedAt      time.Time           `json:"updated_at"`
	CompensationID string              `json:"compensation_id,omitempty"`
	// BreakGlass is set when the action was released by an emergency
	// approval instead of the regular approval quorum.
	BreakGlass *BreakGlassGrant `json:"break_glass,omitempty"`
	// MayHaveDispatched is true once DISPATCHED was committed: the external
	// call may have reached the downstream even if no result was recorded.
	MayHaveDispatched bool `json:"may_have_dispatched"`
}

// Store errors.
var (
	ErrNotFound        = errors.New("action not found")
	ErrVersionConflict = errors.New("action version conflict")
	ErrTargetBusy      = errors.New("another action holds the target")
)

// Store is the persistence port (internal/store/cpsql).
type Store interface {
	// CreateAction inserts r; an existing record with the same (tenant,
	// work, plan hash) is returned with created=false.
	CreateAction(ctx context.Context, r Record) (Record, bool, error)
	GetAction(ctx context.Context, tenantID, id string) (Record, error)
	ListActions(ctx context.Context, tenantID, workID string) ([]Record, error)
	// UpdateAction writes r if the stored version is expectedVersion and
	// returns it with the bumped version.
	UpdateAction(ctx context.Context, r Record, expectedVersion int64) (Record, error)
	// ListStaleDispatched returns DISPATCHED actions older than before
	// (crashed between dispatch and result).
	ListStaleDispatched(ctx context.Context, before time.Time, limit int) ([]Record, error)
	// LockTarget takes the per-resource lock for actionID until the given
	// time (re-entrant for the same action); false when another unexpired
	// action holds it.
	LockTarget(ctx context.Context, tenantID, targetRef, actionID string, until, now time.Time) (bool, string, error)
	UnlockTarget(ctx context.Context, tenantID, targetRef, actionID string) error
}

// KernelEffect is an entry of the kernel's tool effect ledger
// (external_tool_effects): a side-effecting tool call made by an agent run.
type KernelEffect struct {
	ID                  string     `json:"id"`
	RunID               string     `json:"run_id"`
	StepID              string     `json:"step_id"`
	Target              string     `json:"target"`
	State               string     `json:"state"` // PREPARED | DISPATCHED | CONFIRMED | UNKNOWN | COMPENSATED
	ExternalOperationID string     `json:"external_operation_id,omitempty"`
	LastCheckAt         *time.Time `json:"last_check_at,omitempty"`
}

// Open reports whether the effect may still change the target.
func (e KernelEffect) Open() bool {
	return e.State == "PREPARED" || e.State == "DISPATCHED" || e.State == "UNKNOWN"
}

// EffectLedger reads the kernel's effect ledger. Work actions and agent
// tool calls are two ledgers over the same resources (RFC §17.4); the
// runtime consults this one so an action never dispatches over an effect
// whose result is not known yet.
type EffectLedger interface {
	// KernelEffects lists effects on any of targets; openOnly limits the
	// result to PREPARED, DISPATCHED and UNKNOWN effects.
	KernelEffects(ctx context.Context, tenantID string, targets []string, openOnly bool, limit int) ([]KernelEffect, error)
}

// EffectTargets are the kernel effect targets a resource ref corresponds
// to: the kernel keys effects by the tool's target arguments joined with
// "/" (tools.Target), e.g. "prod/checkout" for deployment://prod/checkout.
func EffectTargets(ref string) []string {
	out := []string{ref}
	if i := strings.Index(ref, "://"); i > 0 && i+3 < len(ref) {
		out = append(out, ref[i+3:])
	}
	return out
}

// ErrPendingEffect blocks a dispatch while a kernel effect on the same
// target is unresolved.
var ErrPendingEffect = errors.New("a kernel tool effect on the target is unresolved")

func errState(r Record, want ...State) error {
	return fmt.Errorf("%w: action %s is %s (want %v)", ErrInvalidState, r.ID, r.State, want)
}

// ErrInvalidState is returned for operations in the wrong state.
var ErrInvalidState = errors.New("invalid action state")
