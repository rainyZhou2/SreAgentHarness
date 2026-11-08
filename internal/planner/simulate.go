package planner

import (
	"fmt"
	"strings"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/tools"
)

// Simulation is a dry run of a flow through the real engine with mock
// results: every child succeeds with a placeholder output, every approval
// and input gate is granted. It catches plans that cannot finish on their
// default path (runtime limits, broken expressions, missing inputs) and
// records the order in which side effects and approvals are reached, before
// anything runs for real.
type Simulation struct {
	Completed bool     `json:"completed"`
	Failure   string   `json:"failure,omitempty"`
	Path      []string `json:"path"`
	Steps     int      `json:"steps"`
	ChildRuns int      `json:"child_runs"`
	// Approvals lists approval nodes in the order they were reached.
	Approvals []string `json:"approvals,omitempty"`
	// SideEffects lists side-effecting nodes in the order they were reached.
	SideEffects []string `json:"side_effects,omitempty"`
	// Unapproved lists side-effecting nodes reached before any approval on
	// the simulated path.
	Unapproved []string `json:"unapproved,omitempty"`
}

// simulatedOutput stands in for a child's result.
const simulatedOutput = "(simulated)"

// Simulate dry-runs spec on its default path.
func Simulate(spec flow.Spec, inputs map[string]any, catalog []tools.Descriptor) Simulation {
	risks := map[string]domain.RiskClass{}
	anyRisky := false
	for _, d := range catalog {
		risks[d.Name] = d.Risk
		anyRisky = anyRisky || d.Risk.HasSideEffects()
	}
	index := map[string]*flow.Node{}
	for i := range spec.Nodes {
		index[spec.Nodes[i].ID] = &spec.Nodes[i]
	}
	sideEffect := func(n *flow.Node) bool {
		switch n.Type {
		case flow.NodeTool:
			r, ok := risks[n.Tool]
			return !ok || r.HasSideEffects()
		case flow.NodeAgent:
			if len(n.Tools) == 0 {
				return anyRisky
			}
			for _, t := range n.Tools {
				if r, ok := risks[t]; !ok || r.HasSideEffects() {
					return true
				}
			}
		case flow.NodeSubflow:
			return anyRisky
		}
		return false
	}

	var sim Simulation
	in := map[string]any{}
	for k, v := range inputs {
		in[k] = v
	}
	for _, decl := range spec.Inputs {
		if _, ok := in[decl.Name]; !ok && decl.Required && decl.Default == nil {
			in[decl.Name] = simulatedOutput
		}
	}
	st, err := flow.NewState(spec, in, 0)
	if err != nil {
		sim.Failure = err.Error()
		return sim
	}
	approved := false
	visit := func(id string) {
		sim.Path = append(sim.Path, id)
		n := index[id]
		if n == nil {
			return
		}
		if sideEffect(n) {
			sim.SideEffects = append(sim.SideEffects, id)
			if !approved {
				sim.Unapproved = append(sim.Unapproved, id)
			}
		}
	}
	results := map[string]flow.NodeResult{}
	for i := 0; i < 2000; i++ {
		action, err := flow.Step(spec, st, results)
		results = map[string]flow.NodeResult{}
		if err != nil {
			sim.Failure = err.Error()
			return sim
		}
		switch action.Kind {
		case flow.ActionAgent, flow.ActionTool, flow.ActionSubflow, flow.ActionParallel:
			sim.Steps++
			for _, c := range action.Children {
				sim.ChildRuns++
				visit(c.NodeID)
				results[c.NodeID] = flow.NodeResult{Status: "SUCCEEDED", Output: simulatedOutput}
			}
		case flow.ActionApproval:
			sim.Steps++
			sim.Path = append(sim.Path, action.Node.ID)
			sim.Approvals = append(sim.Approvals, action.Node.ID)
			approved = true
			results[action.Node.ID] = flow.NodeResult{Status: "SUCCEEDED", Approved: true}
		case flow.ActionInput:
			sim.Steps++
			sim.Path = append(sim.Path, action.Node.ID)
			results[action.Node.ID] = flow.NodeResult{Status: "SUCCEEDED", InputSet: true, Input: simulatedOutput}
		case flow.ActionWait:
			// Every child was answered when it was started; a wait means the
			// engine is waiting on something the simulation cannot provide.
			sim.Failure = fmt.Sprintf("flow waits on %v with no pending child", action.WaitFor)
			return sim
		case flow.ActionComplete:
			sim.Completed = true
			return sim
		case flow.ActionFail:
			sim.Failure = action.Error
			return sim
		}
	}
	sim.Failure = "simulation exceeded the transition budget"
	return sim
}

// simulationFindings turns a simulation into analysis findings. A plan that
// cannot complete on its default path is rejected; side effects reached
// before an approval are flagged for review (the static check covers every
// path, the simulation confirms the default one).
func simulationFindings(spec flow.Spec, sim Simulation) []Finding {
	var out []Finding
	if !sim.Completed {
		// A runtime limit hit with every node succeeding is a property of
		// the graph: the real run would hit it too. Other failures may come
		// from mock outputs (an expression reading a field the placeholder
		// lacks), so they are recorded without blocking the plan.
		// Loops are excluded: with placeholder outputs an until condition
		// may never hold, so the loop runs to its cap.
		sev := "info"
		if limitFailure(sim.Failure) && !specHasLoop(spec) {
			sev = "error"
		}
		out = append(out, Finding{Code: "simulation_failed", Severity: sev,
			Message: "dry run of the default path did not complete: " + sim.Failure})
	}
	for _, id := range sim.Unapproved {
		out = append(out, Finding{Code: "simulation_unapproved_side_effect", Severity: "warning", NodeID: id,
			Message: fmt.Sprintf("dry run reached side-effecting node %q before any approval", id)})
	}
	return out
}

func limitFailure(msg string) bool {
	for _, s := range []string{"exceed", "max_steps", "max_total_runs", "max_iterations", "max_children", "transition budget"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func specHasLoop(spec flow.Spec) bool {
	for _, n := range spec.Nodes {
		if n.Type == flow.NodeLoop || (n.Flow != nil && specHasLoop(*n.Flow)) {
			return true
		}
	}
	return false
}
