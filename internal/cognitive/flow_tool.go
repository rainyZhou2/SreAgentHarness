package cognitive

import (
	"context"
	"encoding/json"
	"fmt"

	"go-sre-harness/internal/domain"
)

// FlowTool is the deterministic strategy used by a flow tool node's child
// run. It proposes the configured tool once, then reports the observation.
// Policy, approvals and the effect ledger still run in the kernel.
type FlowTool struct{}

func (FlowTool) Name() string { return "flow_tool" }

func (FlowTool) Decide(_ context.Context, in ReasoningInput) (domain.Decision, error) {
	d := domain.Decision{SchemaVersion: 1, ContextSnapshotID: in.ContextSnapshotID, Intent: domain.IntentTask}
	if in.FlowNode == nil || in.FlowNode.Tool == "" {
		return d, fmt.Errorf("flow tool node without a configured tool")
	}
	n := in.FlowNode
	for _, o := range in.Observations {
		if o.Tool != n.Tool {
			continue
		}
		d.Kind = domain.DecisionFinal
		d.ReasonSummary = "Report the flow tool result."
		if o.Outcome != "ok" {
			return d, fmt.Errorf("flow tool %q did not run (%s): %s", n.Tool, o.Outcome, o.Note)
		}
		d.Output = o.Content
		d.Answer = string(o.Content)
		return d, nil
	}
	args, err := json.Marshal(n.Args)
	if err != nil {
		return d, err
	}
	d.Kind = domain.DecisionTool
	d.ReasonSummary = "Run the configured flow tool."
	d.Tool = &domain.ToolCall{Name: n.Tool, Args: args}
	return d, nil
}
