package domain

import (
	"errors"
	"time"
)

// Conversation domain (§29, §35.1). Session != goroutine; Turn != Run; a Turn
// owns exactly one main Run in V1.

type TurnStatus string

const (
	TurnAccepted  TurnStatus = "ACCEPTED"
	TurnQueued    TurnStatus = "QUEUED"
	TurnRunning   TurnStatus = "RUNNING"
	TurnCompleted TurnStatus = "COMPLETED"
	TurnFailed    TurnStatus = "FAILED"
	TurnCancelled TurnStatus = "CANCELLED"
)

// BranchPolicy decides what happens when a turn arrives while another one is
// active on the same branch (§29.2).
type BranchPolicy string

const (
	BranchQueue  BranchPolicy = "single_writer_queue"
	BranchReject BranchPolicy = "single_writer_reject"
)

var (
	ErrBranchBusy       = errors.New("branch has an active turn")
	ErrTurnQueueFull    = errors.New("too many pending turns on branch")
	ErrQuotaExceeded    = errors.New("tenant quota exceeded")
	ErrInvalidCursor    = errors.New("message cursor is not committed on the parent branch")
	ErrForbidden        = errors.New("forbidden")
	ErrApproverConflict = errors.New("approver must differ from the run initiator")
)

type Session struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	CreatedBy       string    `json:"created_by"`
	Title           *string   `json:"title,omitempty"`
	ProfileID       string    `json:"profile_id"`
	ProfileVersion  int64     `json:"profile_version"`
	Status          string    `json:"status"`
	DefaultBranchID string    `json:"default_branch_id"`
	IncidentID      *string   `json:"incident_id,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
}

type Branch struct {
	ID                  string  `json:"id"`
	SessionID           string  `json:"session_id"`
	ParentBranchID      *string `json:"parent_branch_id,omitempty"`
	ParentMessageCursor *int64  `json:"parent_message_cursor,omitempty"`
	ForkedFrom          *string `json:"forked_from_checkpoint_id,omitempty"`
	Version             int64   `json:"version"`
	LastTurnSeq         int64   `json:"last_turn_seq"`
	LastMessageSeq      int64   `json:"last_message_seq"`
	ActiveTurnID        *string `json:"active_turn_id,omitempty"`
}

type Turn struct {
	ID                 string     `json:"id"`
	SessionID          string     `json:"session_id"`
	BranchID           string     `json:"branch_id"`
	RunID              *string    `json:"run_id,omitempty"`
	Seq                int64      `json:"seq"`
	InputRef           string     `json:"input_ref"`
	InputMessageCursor *int64     `json:"input_message_cursor,omitempty"`
	ClientRequestID    string     `json:"client_request_id"`
	Status             TurnStatus `json:"status"`
	CreatedAt          time.Time  `json:"created_at"`
	ActivatedAt        *time.Time `json:"activated_at,omitempty"`
}

type Message struct {
	ID           string    `json:"id"`
	BranchID     string    `json:"branch_id"`
	TurnID       string    `json:"turn_id"`
	Seq          int64     `json:"seq"`
	Role         string    `json:"role"`
	ContentRef   string    `json:"content_ref"`
	ContentHash  string    `json:"content_hash"`
	Kind         string    `json:"message_kind"`
	SourceStepID *string   `json:"source_step_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type CreateSessionRequest struct {
	TenantID       string
	ActorID        string
	ProfileID      string
	ProfileVersion int64
	Title          string
	IncidentID     *string
	IdempotencyKey string
}

type SubmitTurnRequest struct {
	TenantID        string
	ActorID         string
	SessionID       string
	BranchID        string // empty = session default branch
	ClientRequestID string
	InputRef        string
	InputHash       string
	ExpectedVersion *int64
	// Run creation parameters resolved by the application layer (profile channel, auto-routing, canary).
	ProfileID      string
	ProfileVersion int64
	RolloutID      *string
	InitialSteps   []PlannedStep
}

type SubmittedTurn struct {
	TurnID        string     `json:"turn_id"`
	RunID         string     `json:"run_id"`
	BranchID      string     `json:"branch_id"`
	Seq           int64      `json:"turn_seq"`
	BranchVersion int64      `json:"branch_version"`
	Status        TurnStatus `json:"status"`
	Queued        bool       `json:"queued"`
	Created       bool       `json:"created"`
}

type ForkBranchRequest struct {
	TenantID       string
	ActorID        string
	SessionID      string
	ParentBranchID string
	MessageCursor  *int64 // nil = parent's last committed message
}

type SessionView struct {
	Session    Session   `json:"session"`
	Branches   []Branch  `json:"branches"`
	ActiveTurn *Turn     `json:"active_turn,omitempty"`
	Queued     []Turn    `json:"queued_turns"`
	Messages   []Message `json:"messages"` // default branch history (resolved through ancestors)
}
