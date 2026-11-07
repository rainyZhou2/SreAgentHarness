package flow

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// NodeStatus is the persisted state of one flow node.
type NodeStatus string

const (
	NodePending   NodeStatus = "PENDING"
	NodeRunning   NodeStatus = "RUNNING"
	NodeWaiting   NodeStatus = "WAITING"
	NodeSucceeded NodeStatus = "SUCCEEDED"
	NodeFailed    NodeStatus = "FAILED"
	NodeSkipped   NodeStatus = "SKIPPED"
)

func (s NodeStatus) terminal() bool { return s == NodeSucceeded || s == NodeFailed || s == NodeSkipped }

// NodeState is the durable state of one node.
type NodeState struct {
	Status     NodeStatus        `json:"status"`
	Output     any               `json:"output,omitempty"`
	Error      string            `json:"error,omitempty"`
	Children   []string          `json:"children,omitempty"`
	ChildRuns  map[string]string `json:"child_runs,omitempty"`
	Iterations int               `json:"iterations,omitempty"`
	Attempts   int               `json:"attempts,omitempty"`
	Approved   bool              `json:"approved,omitempty"`
}

// State is the serializable flow state pinned on the run.
type State struct {
	Inputs    map[string]any        `json:"inputs"`
	Vars      map[string]any        `json:"vars,omitempty"`
	Nodes     map[string]*NodeState `json:"nodes"`
	Order     []string              `json:"order,omitempty"`
	Spec      *Spec                 `json:"spec,omitempty"`
	Current   string                `json:"current"`
	ChildRuns int                   `json:"child_runs,omitempty"`
	Steps     int                   `json:"steps,omitempty"`
	Depth     int                   `json:"depth,omitempty"`
	Output    any                   `json:"output,omitempty"`
	Done      bool                  `json:"done,omitempty"`
	Failed    string                `json:"failed,omitempty"`
}

// ActionKind tells the executor what to do next.
type ActionKind string

const (
	ActionAgent    ActionKind = "agent"
	ActionTool     ActionKind = "tool"
	ActionSubflow  ActionKind = "subflow"
	ActionParallel ActionKind = "parallel"
	ActionApproval ActionKind = "approval"
	ActionInput    ActionKind = "input"
	ActionWait     ActionKind = "wait"
	ActionComplete ActionKind = "complete"
	ActionFail     ActionKind = "fail"
)

// ChildSpec describes one child run to create.
type ChildSpec struct {
	NodeID     string
	Type       NodeType
	Attempt    int
	Role       string
	Prompt     string
	Tools      []string
	ModelAlias string
	MaxSteps   int
	Tool       string
	Args       map[string]any
	Flow       *Spec
	Input      map[string]any
	Depth      int
}

// Action is the next step the executor must take.
type Action struct {
	Kind     ActionKind
	Node     *Node
	Children []ChildSpec
	WaitFor  []string
	Output   any
	Error    string
}

// NodeResult is a child run's completion, or an approval/input decision.
type NodeResult struct {
	Status   string // SUCCEEDED | FAILED
	Output   any
	Error    string
	Approved bool
	Rejected bool
	Input    any
	InputSet bool
}

// NewState builds the initial state and validates required inputs.
func NewState(spec Spec, inputs map[string]any, depth int) (*State, error) {
	if inputs == nil {
		inputs = map[string]any{}
	}
	declared := map[string]bool{}
	for _, in := range spec.Inputs {
		declared[in.Name] = true
		if v, ok := inputs[in.Name]; ok && v != nil {
			continue
		}
		if in.Default != nil {
			inputs[in.Name] = in.Default
			continue
		}
		if in.Required {
			return nil, fmt.Errorf("flow: input %q is required", in.Name)
		}
	}
	st := &State{
		Inputs:  inputs,
		Vars:    map[string]any{},
		Nodes:   make(map[string]*NodeState, len(spec.Nodes)),
		Order:   make([]string, 0, len(spec.Nodes)),
		Spec:    &spec,
		Current: spec.Entry,
		Depth:   depth,
	}
	for i := range spec.Nodes {
		st.Nodes[spec.Nodes[i].ID] = &NodeState{Status: NodePending}
		st.Order = append(st.Order, spec.Nodes[i].ID)
	}
	return st, nil
}

// Step applies completed child results and advances the state machine until
// it needs an external action or reaches a terminal state.
func Step(spec Spec, st *State, results map[string]NodeResult) (Action, error) {
	if st == nil {
		return Action{}, fmt.Errorf("flow: nil state")
	}
	if st.Nodes == nil {
		st.Nodes = map[string]*NodeState{}
	}
	if st.Inputs == nil {
		st.Inputs = map[string]any{}
	}
	if st.Vars == nil {
		st.Vars = map[string]any{}
	}
	index := make(map[string]*Node, len(spec.Nodes))
	for i := range spec.Nodes {
		index[spec.Nodes[i].ID] = &spec.Nodes[i]
	}
	applyResults(index, st, results)

	for transitions := 0; transitions < 1000; transitions++ {
		if st.Done {
			return Action{Kind: ActionComplete, Output: st.Output}, nil
		}
		if st.Failed != "" {
			return Action{Kind: ActionFail, Error: st.Failed}, nil
		}
		n := index[st.Current]
		if n == nil {
			return fail(st, "node %q does not exist", st.Current)
		}
		ns := st.Nodes[n.ID]
		if ns == nil {
			ns = &NodeState{Status: NodePending}
			st.Nodes[n.ID] = ns
		}
		switch n.Type {
		case NodeAgent, NodeTool, NodeSubflow:
			switch ns.Status {
			case NodePending:
				if err := bumpStep(spec, st); err != nil {
					return fail(st, "%v", err)
				}
				if st.ChildRuns+1 > spec.Limits.MaxTotalRuns {
					return fail(st, "flow would exceed max_total_runs=%d", spec.Limits.MaxTotalRuns)
				}
				st.ChildRuns++
				ns.Status = NodeRunning
				ns.Attempts++
				child, err := childForNode(spec, n, st)
				if err != nil {
					return fail(st, "node %q input: %v", n.ID, err)
				}
				child.Attempt = ns.Attempts
				return Action{Kind: ActionKind(n.Type), Node: n, Children: []ChildSpec{child}}, nil
			case NodeRunning:
				return Action{Kind: ActionWait, Node: n, WaitFor: []string{n.ID}}, nil
			case NodeSucceeded:
				if err := advance(index, st, n, ns); err != nil {
					return Action{}, err
				}
			default:
				return fail(st, "node %q failed: %s", n.ID, ns.Error)
			}

		case NodeParallel:
			switch ns.Status {
			case NodePending:
				if err := bumpStep(spec, st); err != nil {
					return fail(st, "%v", err)
				}
				if len(n.Branches) > spec.Limits.MaxChildren {
					return fail(st, "parallel node %q has %d branches, limit is %d", n.ID, len(n.Branches), spec.Limits.MaxChildren)
				}
				if st.ChildRuns+len(n.Branches) > spec.Limits.MaxTotalRuns {
					return fail(st, "flow would exceed max_total_runs=%d", spec.Limits.MaxTotalRuns)
				}
				ns.Status = NodeRunning
				ns.Children = append([]string(nil), n.Branches...)
				if ns.ChildRuns == nil {
					ns.ChildRuns = map[string]string{}
				}
				st.ChildRuns += len(n.Branches)
				children := make([]ChildSpec, 0, len(n.Branches))
				for _, id := range n.Branches {
					bn := index[id]
					if bn == nil {
						return fail(st, "parallel branch %q does not exist", id)
					}
					bns := st.Nodes[id]
					if bns == nil {
						bns = &NodeState{Status: NodePending}
						st.Nodes[id] = bns
					}
					if bns.Status == NodePending {
						bns.Status = NodeRunning
					}
					bns.Attempts++
					child, err := childForNode(spec, bn, st)
					if err != nil {
						return fail(st, "branch %q input: %v", id, err)
					}
					child.Attempt = bns.Attempts
					children = append(children, child)
				}
				return Action{Kind: ActionParallel, Node: n, Children: children}, nil
			case NodeRunning:
				pending := make([]string, 0, len(n.Branches))
				for _, id := range n.Branches {
					bns := st.Nodes[id]
					if bns == nil || !bns.Status.terminal() {
						pending = append(pending, id)
					}
				}
				if len(pending) > 0 {
					return Action{Kind: ActionWait, Node: n, WaitFor: pending}, nil
				}
				out := map[string]any{}
				successes, failures := 0, 0
				for _, id := range n.Branches {
					bns := st.Nodes[id]
					if bns.Status == NodeSucceeded {
						successes++
						out[id] = bns.Output
					} else {
						failures++
						out[id] = map[string]any{"error": bns.Error}
					}
				}
				ns.Output = out
				switch n.OnError {
				case "continue":
					ns.Status = NodeSucceeded
				default:
					if failures > 0 {
						ns.Status = NodeFailed
						ns.Error = fmt.Sprintf("%d branch(es) failed", failures)
						return fail(st, "parallel node %q failed: %s", n.ID, ns.Error)
					}
					ns.Status = NodeSucceeded
				}
				if err := advance(index, st, n, ns); err != nil {
					return Action{}, err
				}
			case NodeSucceeded:
				if err := advance(index, st, n, ns); err != nil {
					return Action{}, err
				}
			default:
				return fail(st, "parallel node %q failed: %s", n.ID, ns.Error)
			}

		case NodeJoin:
			if ns.Status == NodePending {
				ns.Status = NodeRunning
			}
			if ns.Status == NodeRunning {
				var waiting []string
				var outputs []any
				successes, failures := 0, 0
				for _, w := range n.Wait {
					wns := st.Nodes[w]
					if wns == nil || !wns.Status.terminal() {
						waiting = append(waiting, w)
						continue
					}
					outputs = append(outputs, wns.Output)
					if wns.Status == NodeSucceeded {
						successes++
					} else {
						failures++
					}
				}
				if len(waiting) > 0 {
					return Action{Kind: ActionWait, Node: n, WaitFor: waiting}, nil
				}
				ok := joinSatisfied(n, successes, failures)
				if !ok && n.OnError != "continue" {
					ns.Status = NodeFailed
					ns.Error = fmt.Sprintf("join policy %q not satisfied (%d ok, %d failed)", n.Join, successes, failures)
					return fail(st, "join node %q failed: %s", n.ID, ns.Error)
				}
				if n.Output != "" {
					v, err := Interpolate(n.Output, evalContext(st, ""))
					if err != nil {
						return fail(st, "join node %q output: %v", n.ID, err)
					}
					ns.Output = v
				} else if len(n.Wait) == 1 {
					ns.Output = outputs[0]
				} else {
					out := map[string]any{}
					for i, w := range n.Wait {
						out[w] = outputs[i]
					}
					ns.Output = out
				}
				ns.Status = NodeSucceeded
			}
			if err := advance(index, st, n, ns); err != nil {
				return Action{}, err
			}

		case NodeApproval:
			switch ns.Status {
			case NodePending:
				if err := bumpStep(spec, st); err != nil {
					return fail(st, "%v", err)
				}
				ns.Status = NodeWaiting
				return Action{Kind: ActionApproval, Node: n}, nil
			case NodeWaiting:
				return Action{Kind: ActionWait, Node: n, WaitFor: []string{n.ID}}, nil
			case NodeSucceeded:
				if err := advance(index, st, n, ns); err != nil {
					return Action{}, err
				}
			default:
				return fail(st, "approval node %q was rejected", n.ID)
			}

		case NodeInput:
			switch ns.Status {
			case NodePending:
				if err := bumpStep(spec, st); err != nil {
					return fail(st, "%v", err)
				}
				ns.Status = NodeWaiting
				return Action{Kind: ActionInput, Node: n}, nil
			case NodeWaiting:
				return Action{Kind: ActionWait, Node: n, WaitFor: []string{n.ID}}, nil
			case NodeSucceeded:
				if err := advance(index, st, n, ns); err != nil {
					return Action{}, err
				}
			default:
				return fail(st, "input node %q failed: %s", n.ID, ns.Error)
			}

		case NodeLoop:
			if ns.Status == NodePending {
				ns.Status = NodeRunning
			}
			max := n.Iterations
			if max <= 0 {
				max = spec.Limits.MaxIterations
			}
			if ns.Iterations > 0 {
				if n.Until != "" {
					ok, err := evalBool(n.Until, evalContext(st, n.ID))
					if err != nil {
						return fail(st, "loop node %q until: %v", n.ID, err)
					}
					if ok {
						ns.Status = NodeSucceeded
						if err := advance(index, st, n, ns); err != nil {
							return Action{}, err
						}
						continue
					}
				}
				if ns.Iterations >= max {
					ns.Status = NodeFailed
					ns.Error = fmt.Sprintf("loop exceeded max_iterations=%d", max)
					return fail(st, "loop node %q exceeded max_iterations=%d", n.ID, max)
				}
			}
			ns.Iterations++
			resetBody(index, st, n.Body, n.ID)
			st.Current = n.Body
			continue

		case NodeEnd:
			var out any
			if n.Output != "" {
				v, err := Interpolate(n.Output, evalContext(st, ""))
				if err != nil {
					return fail(st, "end node %q output: %v", n.ID, err)
				}
				out = v
			} else if spec.Output != "" {
				v, err := Interpolate(spec.Output, evalContext(st, ""))
				if err != nil {
					return fail(st, "flow output: %v", err)
				}
				out = v
			}
			ns.Status = NodeSucceeded
			st.Output = out
			st.Done = true
			return Action{Kind: ActionComplete, Node: n, Output: out}, nil

		default:
			return fail(st, "node %q has unknown type %q", n.ID, n.Type)
		}
	}
	return fail(st, "flow exceeded its transition budget")
}

func bumpStep(spec Spec, st *State) error {
	st.Steps++
	if st.Steps > spec.Limits.MaxSteps {
		return fmt.Errorf("flow step budget exhausted (max_steps=%d)", spec.Limits.MaxSteps)
	}
	return nil
}

func applyResults(index map[string]*Node, st *State, results map[string]NodeResult) {
	for id, r := range results {
		n := index[id]
		if n == nil {
			continue
		}
		ns := st.Nodes[id]
		if ns == nil {
			ns = &NodeState{Status: NodePending}
			st.Nodes[id] = ns
		}
		switch n.Type {
		case NodeApproval:
			if r.Rejected || (r.Status == "FAILED" && !r.Approved) {
				ns.Status = NodeFailed
				ns.Error = "approval rejected"
			} else if r.Approved || r.Status == "SUCCEEDED" {
				ns.Status = NodeSucceeded
				ns.Approved = true
				ns.Output = true
			}
		case NodeInput:
			if r.InputSet {
				ns.Status = NodeSucceeded
				ns.Output = r.Input
				st.Vars[n.Output] = r.Input
			} else if r.Status == "FAILED" {
				ns.Status = NodeFailed
				ns.Error = r.Error
			}
		default:
			if r.Status == "FAILED" {
				ns.Status = NodeFailed
				ns.Error = r.Error
			} else {
				ns.Status = NodeSucceeded
				ns.Output = r.Output
			}
		}
	}
}

func advance(index map[string]*Node, st *State, n *Node, ns *NodeState) error {
	if n.Type != NodeEnd && len(n.Next) == 0 {
		ns.Status = NodeFailed
		ns.Error = "no next edge"
		st.Failed = fmt.Sprintf("node %q has no next edge", n.ID)
		return nil
	}
	if ns.Status != NodeFailed {
		ns.Status = NodeSucceeded
	}
	ctx := evalContext(st, "")
	for _, e := range n.Next {
		if strings.TrimSpace(e.When) == "" {
			st.Current = e.To
			return nil
		}
		ok, err := evalBool(e.When, ctx)
		if err != nil {
			return fmt.Errorf("flow: node %q edge to %q: %w", n.ID, e.To, err)
		}
		if ok {
			st.Current = e.To
			return nil
		}
	}
	ns.Status = NodeFailed
	ns.Error = "no edge matched"
	st.Failed = fmt.Sprintf("node %q: no edge matched", n.ID)
	return nil
}

func joinSatisfied(n *Node, successes, failures int) bool {
	total := successes + failures
	switch n.Join {
	case "any":
		return successes > 0
	case "n_of_m":
		return successes >= n.N
	default: // all
		return successes == total
	}
}

// resetBody clears the nodes executed in the previous loop iteration so the
// next iteration starts from a clean state. Traversal stops at the loop node.
func resetBody(index map[string]*Node, st *State, id, stop string) {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(cur string) {
		if cur == stop || seen[cur] {
			return
		}
		seen[cur] = true
		if ns := st.Nodes[cur]; ns != nil {
			ns.Status = NodePending
			ns.Output = nil
			ns.Error = ""
			ns.Children = nil
			ns.ChildRuns = nil
			ns.Iterations = 0
			ns.Approved = false
		}
		n := index[cur]
		if n == nil {
			return
		}
		for _, e := range n.Next {
			walk(e.To)
		}
		if n.Type == NodeParallel {
			for _, b := range n.Branches {
				walk(b)
			}
		}
		if n.Type == NodeJoin {
			for _, w := range n.Wait {
				walk(w)
			}
		}
		if n.Type == NodeLoop && n.Body != "" {
			walk(n.Body)
		}
	}
	walk(id)
}

func childForNode(spec Spec, n *Node, st *State) (ChildSpec, error) {
	child := ChildSpec{
		NodeID:     n.ID,
		Type:       n.Type,
		Role:       n.Role,
		Prompt:     n.Prompt,
		Tools:      append([]string(nil), n.Tools...),
		ModelAlias: n.Model,
		MaxSteps:   n.MaxSteps,
		Flow:       n.Flow,
		Input:      map[string]any{"inputs": st.Inputs, "vars": st.Vars, "nodes": nodeOutputs(st)},
		Depth:      st.Depth + 1,
	}
	if n.Type == NodeTool {
		child.Tool = n.Tool
		child.Args = map[string]any{}
		if len(n.Args) > 0 {
			v, err := interpolateArgs(n.Args, evalContext(st, ""))
			if err != nil {
				return child, err
			}
			child.Args = v
		}
	}
	return child, nil
}

func interpolateArgs(raw json.RawMessage, ctx map[string]any) (map[string]any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	iv, err := InterpolateValue(v, ctx)
	if err != nil {
		return nil, err
	}
	m, ok := iv.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("args must be an object")
	}
	return m, nil
}

func nodeOutputs(st *State) map[string]any {
	out := make(map[string]any, len(st.Nodes))
	for id, ns := range st.Nodes {
		if ns == nil {
			continue
		}
		out[id] = map[string]any{"status": string(ns.Status), "output": ns.Output, "error": ns.Error}
	}
	return out
}

func evalContext(st *State, loopID string) map[string]any {
	ctx := map[string]any{
		"input": st.Inputs,
		"vars":  st.Vars,
		"nodes": nodeOutputs(st),
	}
	if loopID != "" {
		if ns := st.Nodes[loopID]; ns != nil {
			ctx["loop"] = map[string]any{"iteration": float64(ns.Iterations)}
		}
	}
	return ctx
}

var exprCache sync.Map // string -> Expr

func compiled(src string) (Expr, error) {
	if v, ok := exprCache.Load(src); ok {
		return v.(Expr), nil
	}
	e, err := CompileExpr(src)
	if err != nil {
		return nil, err
	}
	exprCache.Store(src, e)
	return e, nil
}

func evalExpr(src string, ctx map[string]any) (any, error) {
	e, err := compiled(src)
	if err != nil {
		return nil, err
	}
	return e.Eval(ctx)
}

func evalBool(src string, ctx map[string]any) (bool, error) {
	v, err := evalExpr(src, ctx)
	if err != nil {
		return false, err
	}
	return truthy(v), nil
}

func fail(st *State, format string, args ...any) (Action, error) {
	msg := fmt.Sprintf(format, args...)
	st.Failed = msg
	return Action{Kind: ActionFail, Error: msg}, nil
}

// PendingChildren returns the sorted child node ids that are not terminal.
func (st *State) PendingChildren(nodeID string) []string {
	ns := st.Nodes[nodeID]
	if ns == nil {
		return nil
	}
	var out []string
	for _, id := range ns.Children {
		c := st.Nodes[id]
		if c == nil || !c.Status.terminal() {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}
