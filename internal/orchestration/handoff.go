package orchestration

import (
	"time"

	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/work"
)

// Assignment is what one agent of a stage is asked to do (design §11.2):
// a role, a goal, the pinned world snapshot every agent of the stage
// shares, and a budget. Agents hand back Findings — references and short
// statements — never free text that another agent or the ledger takes on
// trust.
type Assignment struct {
	WorkID        string           `json:"work_id"`
	StageID       string           `json:"stage_id"`
	Role          string           `json:"role"` // metrics|changes|dependencies|infra|gpu|synthesis
	Goal          string           `json:"goal"`
	RunID         string           `json:"run_id,omitempty"`
	SnapshotID    string           `json:"snapshot_id,omitempty"`
	EvidenceScope []string         `json:"evidence_scope,omitempty"`
	AllowedTools  []string         `json:"allowed_tools,omitempty"`
	Budget        work.AgentBudget `json:"budget"`
	Deadline      time.Time        `json:"deadline,omitempty"`
	Required      bool             `json:"required,omitempty"`
}

// Finding and Synthesis are defined next to the evidence ledger they are
// validated against.
type (
	Finding   = evidence.Finding
	Synthesis = evidence.Synthesis
)

// Synthesize validates agent results against the Work's evidence ledger.
func Synthesize(envs []evidence.ResultEnvelope, ledger []evidence.Record, at time.Time) Synthesis {
	return evidence.Synthesize(envs, ledger, at)
}
