package domain

import (
	"encoding/json"
	"time"
)

// FlowSnapshot is the read model of one coordinator run. It is shared by the
// store, the kernel executor and the API.
type FlowSnapshot struct {
	ID           string          `json:"id"`
	RunID        string          `json:"run_id"`
	CompiledRef  string          `json:"compiled_ref"`
	CompiledHash string          `json:"compiled_hash"`
	State        json.RawMessage `json:"state"`
	Status       string          `json:"status"`
	CurrentNode  string          `json:"current_node"`
	Depth        int             `json:"depth"`
	ChildRuns    int             `json:"child_runs"`
	// Generation is the plan generation the state belongs to; Nodes holds
	// only that generation's attempts.
	Generation int           `json:"generation,omitempty"`
	Nodes      []FlowNodeRun `json:"nodes"`
}

// FlowNodeRun is one execution attempt of a flow node.
type FlowNodeRun struct {
	NodeID     string    `json:"node_id"`
	NodeType   string    `json:"node_type"`
	Attempt    int       `json:"attempt"`
	ChildRunID string    `json:"child_run_id,omitempty"`
	Status     string    `json:"status"`
	OutputRef  string    `json:"output_ref,omitempty"`
	OutputHash string    `json:"output_hash,omitempty"`
	Error      string    `json:"error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}
