// Package protocol is the stable wire contract of Work, Run, Agent and
// Action events (design §13). Desktop, CLI, web, IDE and ChatOps clients
// read this contract; the server converts its internal timeline into it,
// so internal event names can evolve without breaking a client.
//
// Rules of the contract:
//
//   - events of one Work carry a gap-free, increasing sequence. A client
//     resumes from the last sequence it saw (SSE Last-Event-ID or
//     after_seq) and receives every persisted event exactly once in order;
//   - durable events are the truth. Ephemeral events (model token deltas)
//     are best effort, carry no sequence, and may be dropped at any time —
//     a client never derives Work state from them;
//   - the tenant is the server's, never a client's claim;
//   - a client ignores event types and payload fields it does not know.
//     New types and fields are added without a version bump; a type is
//     never renamed and a field never changes meaning within a version.
package protocol

import (
	"encoding/json"
	"strings"
	"time"

	"go-sre-harness/internal/work"
)

// SchemaVersion of the event contract.
const SchemaVersion = 1

// MediaType selects the contract on the event stream (Accept header).
const MediaType = "application/vnd.harness.events.v1+json"

// Event types of the contract.
const (
	WorkCreated        = "work.created"
	WorkStageStarted   = "work.stage.started"
	WorkStageCompleted = "work.stage.completed"
	WorkWaiting        = "work.waiting"
	WorkWoken          = "work.woken"
	WorkBlocked        = "work.blocked"
	WorkCompleted      = "work.completed"
	WorkReplanned      = "work.replanned"
	WorkChildCreated   = "work.child.created"
	WorkChildCompleted = "work.child.completed"

	AssignmentCreated   = "agent.assignment.created"
	AssignmentCompleted = "agent.assignment.completed"

	RunStarted   = "run.started"
	RunCompleted = "run.completed"
	RunFailed    = "run.failed"

	ModelDelta   = "model.delta" // ephemeral
	ToolStarted  = "tool.started"
	ToolObserved = "tool.observed"

	EvidenceAdded     = "evidence.added"
	HypothesisChanged = "hypothesis.changed"

	WorldSnapshotPinned = "world.snapshot.pinned"
	WorldChanged        = "world.changed"

	ActionProposed          = "action.proposed"
	ActionApprovalRequested = "action.approval.requested"
	ActionDispatched        = "action.dispatched"
	ActionApplied           = "action.applied"
	ActionEffectUnknown     = "action.effect_unknown"
	ActionVerified          = "action.verified"

	OutcomeEvaluated = "outcome.evaluated"
	OutcomeSatisfied = "outcome.satisfied"

	PostmortemGenerated = "postmortem.generated"
	ImprovementCreated  = "improvement.created"
)

// Event is one entry of the contract.
type Event struct {
	SchemaVersion        int       `json:"schema_version"`
	ContractVersion      int       `json:"contract_version,omitempty"`
	MinimumReaderVersion int       `json:"minimum_reader_version,omitempty"`
	EventID              string    `json:"event_id"`
	Sequence             int64     `json:"sequence,omitempty"`
	OccurredAt           time.Time `json:"occurred_at"`
	TenantID             string    `json:"tenant_id"`
	WorkID               string    `json:"work_id,omitempty"`
	RunID                string    `json:"run_id,omitempty"`
	StepID               string    `json:"step_id,omitempty"`
	ParentEventID        string    `json:"parent_event_id,omitempty"`
	Type                 string    `json:"type"`
	// Ephemeral events are not persisted and have no sequence.
	Ephemeral bool            `json:"ephemeral,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// Durable reports whether the event is part of the persisted timeline.
func (e Event) Durable() bool { return !e.Ephemeral && e.Sequence > 0 }

// FromWork converts one internal timeline event. The mapping is total: an
// internal type without a contract name is passed through under its own
// name, so a newer server never hides events from an older client — the
// client skips what it does not know.
func FromWork(e work.Event) Event {
	out := Event{SchemaVersion: SchemaVersion, ContractVersion: ContractVersion, MinimumReaderVersion: MinimumReaderVersion, EventID: e.ID, Sequence: e.Seq, OccurredAt: e.OccurredAt, TenantID: e.TenantID,
		WorkID: e.WorkID, ParentEventID: e.CausationID, Type: e.Type, Payload: e.Payload}
	var p map[string]any
	if len(e.Payload) > 0 {
		_ = json.Unmarshal(e.Payload, &p)
	}
	str := func(k string) string { s, _ := p[k].(string); return s }
	set := func(k string, v any) {
		if p == nil {
			p = map[string]any{}
		}
		p[k] = v
	}
	switch e.Type {
	case "work.created":
		out.Type = WorkCreated
	case "work.started":
		out.Type = WorkStageStarted
		set("stage_index", 0)
	case "work.status_changed":
		switch work.Status(str("to")) {
		case work.StatusWaiting:
			out.Type = WorkWaiting
		case work.StatusBlocked, work.StatusSuspended:
			out.Type = WorkBlocked
		case work.StatusSucceeded, work.StatusFailed, work.StatusCancelled:
			out.Type = WorkCompleted
			set("status", str("to"))
		default:
			out.Type = WorkWoken
		}
	case "work.blocked":
		out.Type = WorkBlocked
	case "work.wake", "work.released":
		out.Type = WorkWoken
	case "work.stage_completed":
		out.Type = WorkStageCompleted
		set("stage", str("stage_id"))
		if skipped, _ := p["skipped"].(bool); skipped {
			set("status", "SKIPPED")
		} else {
			set("status", "SUCCEEDED")
		}
	case "work.stage_failed":
		out.Type = WorkStageCompleted
		set("stage", str("stage_id"))
		set("status", "FAILED")
	case "work.replanned":
		out.Type = WorkReplanned
	case "work.runs_started":
		out.Type = AssignmentCreated
		set("stage", str("stage_id"))
	case "work.children_started", "work.child_created":
		out.Type = WorkChildCreated
	case "work.child_completed":
		out.Type = WorkChildCompleted
	case "work.action_proposed":
		out.Type = ActionProposed
		if str("state") == "WAITING_APPROVAL" {
			out.Type = ActionApprovalRequested
		}
	case "work.action_executed":
		switch str("state") {
		case "APPLIED", "VERIFYING":
			out.Type = ActionApplied
		case "EFFECT_UNKNOWN", "MANUAL_RECOVERY":
			out.Type = ActionEffectUnknown
			// The honest statement about an unknown effect.
			set("may_have_dispatched", true)
		case "VERIFIED":
			out.Type = ActionVerified
		default:
			out.Type = ActionDispatched
		}
	case "work.confirmed", "work.outcome_recorded":
		out.Type = OutcomeEvaluated
		if strings.EqualFold(str("verdict"), "SATISFIED") {
			out.Type = OutcomeSatisfied
		}
	case "work.evidence_added":
		out.Type = EvidenceAdded
	case "work.goal_amended":
		out.Type = WorkReplanned
	case "work.event_attached", "work.related_event":
		out.Type = WorldChanged
	}
	if id := str("run_id"); id != "" {
		out.RunID = id
	}
	if p != nil {
		if b, err := json.Marshal(p); err == nil {
			out.Payload = b
		}
	}
	return out
}

// FromWorkEvents converts a page of timeline events in order.
func FromWorkEvents(evs []work.Event) []Event {
	out := make([]Event, len(evs))
	for i, e := range evs {
		out[i] = FromWork(e)
	}
	return out
}

// Delta builds an ephemeral model delta event. It has no sequence: a
// client that reconnects does not get it again and must not need it.
func Delta(tenantID, workID, runID, text string, at time.Time) Event {
	b, _ := json.Marshal(map[string]string{"text": text})
	return Event{SchemaVersion: SchemaVersion, ContractVersion: ContractVersion, MinimumReaderVersion: MinimumReaderVersion, OccurredAt: at, TenantID: tenantID, WorkID: workID, RunID: runID,
		Type: ModelDelta, Ephemeral: true, Payload: b}
}

// View is the unified lifecycle view of a Work (design §8.5): who works on
// what, what it waits for, what was dispatched and may have had an effect,
// and what the last independent outcome said.
type View struct {
	SchemaVersion        int       `json:"schema_version,omitempty"`
	ContractVersion      int       `json:"contract_version,omitempty"`
	MinimumReaderVersion int       `json:"minimum_reader_version,omitempty"`
	Work                 work.Work `json:"work"`
	WaitingOn            string    `json:"waiting_on,omitempty"`
	// Controls say what an operator may do now. Cancelling stops further
	// scheduling; it never undoes an action that was already dispatched.
	Controls   []string         `json:"controls"`
	Stages     []StageView      `json:"stages"`
	Runs       []work.RunLink   `json:"runs,omitempty"`
	Actions    []ActionView     `json:"actions,omitempty"`
	Outcome    *OutcomeView     `json:"last_outcome,omitempty"`
	Children   []work.GraphNode `json:"children,omitempty"`
	LastSeq    int64            `json:"last_sequence"`
	CancelNote string           `json:"cancel_note,omitempty"`
}

// StageView is one template stage and where the Work is relative to it.
type StageView struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Phase  string `json:"phase,omitempty"`
	Status string `json:"status"` // done | current | pending
}

// ActionView is what a user needs to know about one action.
type ActionView struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Target string `json:"target"`
	State  string `json:"state"`
	Risk   string `json:"risk"`
	// MayHaveDispatched is true once the external call may have happened,
	// whatever was recorded afterwards.
	MayHaveDispatched bool   `json:"may_have_dispatched"`
	DispatchClass     string `json:"dispatch_class,omitempty"`
	NeedsHuman        bool   `json:"needs_human,omitempty"`
}

// OutcomeView is the last independent evaluation.
type OutcomeView struct {
	Verdict     string    `json:"verdict"`
	EvaluatedAt time.Time `json:"evaluated_at"`
	Reasons     []string  `json:"reasons,omitempty"`
	EvidenceID  string    `json:"evaluation_id,omitempty"`
}

// Controls lists the operator commands that apply to a status.
func Controls(s work.Status) []string {
	switch {
	case s.Terminal():
		return []string{work.CmdReopen}
	case s == work.StatusSuspended:
		return []string{work.CmdResume, work.CmdCancel}
	case s == work.StatusBlocked:
		return []string{work.CmdResolve, work.CmdReplan, work.CmdCancel}
	}
	return []string{work.CmdPause, work.CmdCancel, work.CmdReplan, work.CmdWake}
}
