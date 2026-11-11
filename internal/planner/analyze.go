package planner

// Static plan analysis runs after the flow DSL compiles and before any work is
// allowed to start. It is deliberately conservative: it answers "can this plan
// make a side-effecting call before a human approved it?" and "what is the
// worst-case run/call budget?" so a generated plan cannot silently multiply
// child runs or model calls.

import (
	"fmt"
	"sort"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/flow"
	"go-sre-harness/internal/tools"
)

// Budget is the static ceiling a generated plan must fit. Zero means "use the
// flow's own limits and the planner defaults".
type Budget struct {
	MaxNodes           int
	MaxChildren        int
	MaxTotalRuns       int
	MaxDepth           int
	MaxIterations      int
	MaxModelCalls      int
	MaxOutputTokens    int64
	MaxEstimatedTokens int64
	// TypicalOutputTokens is the expected output of one model call (for
	// example the observed p50 of the planner's model); the token budget is
	// checked against calls × typical, and calls × MaxOutputTokens is
	// reported as the worst case. 0 checks the worst case.
	TypicalOutputTokens int64
}

func (b Budget) withDefaults() Budget {
	if b.MaxNodes <= 0 {
		b.MaxNodes = 20
	}
	if b.MaxChildren <= 0 {
		b.MaxChildren = flow.DefaultMaxChildren
	}
	if b.MaxTotalRuns <= 0 {
		b.MaxTotalRuns = flow.DefaultMaxTotalRuns
	}
	if b.MaxDepth <= 0 {
		b.MaxDepth = flow.DefaultMaxDepth
	}
	if b.MaxIterations <= 0 {
		b.MaxIterations = flow.DefaultMaxIterations
	}
	if b.MaxModelCalls <= 0 {
		b.MaxModelCalls = 40
	}
	return b
}

// Finding is one static-analysis result. Errors reject the plan; warnings are
// recorded and make the plan require human review.
type Finding struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // error | warning
	NodeID   string `json:"node_id,omitempty"`
	Message  string `json:"message"`
}

// ToolUse is one tool reference found in the plan.
type ToolUse struct {
	NodeID   string           `json:"node_id"`
	Tool     string           `json:"tool"`
	Risk     domain.RiskClass `json:"risk"`
	Approved bool             `json:"approved"`
}

// Analysis is the immutable safety/budget summary of a plan. It is stored in
// the plan artifact and rendered by the client.
type Analysis struct {
	Nodes                 int              `json:"nodes"`
	MaxDepth              int              `json:"max_depth"`
	ParallelWidth         int              `json:"parallel_width"`
	MaxIterations         int              `json:"max_iterations"`
	MaxSteps              int              `json:"max_steps"`
	EstimatedRuns         int              `json:"estimated_runs"`
	EstimatedSteps        int              `json:"estimated_steps"`
	EstimatedModelCalls   int              `json:"estimated_model_calls"`
	EstimatedOutputTokens int64            `json:"estimated_output_tokens,omitempty"`
	WorstCaseOutputTokens int64            `json:"worst_case_output_tokens,omitempty"`
	HighestRisk           domain.RiskClass `json:"highest_risk"`
	SideEffects           int              `json:"side_effects"`
	ApprovalNodes         int              `json:"approval_nodes"`
	Tools                 []ToolUse        `json:"tools,omitempty"`
	Findings              []Finding        `json:"findings,omitempty"`
	RequiresReview        bool             `json:"requires_review"`
	// Simulation is the dry run of the default path (nil when not run).
	Simulation *Simulation `json:"simulation,omitempty"`
}

// Errors returns the findings that must make plan validation fail.
func (a Analysis) Errors() []string {
	var out []string
	for _, f := range a.Findings {
		if f.Severity == "error" {
			out = append(out, f.Message)
		}
	}
	return out
}

// Warnings returns the findings a human should see.
func (a Analysis) Warnings() []string {
	var out []string
	for _, f := range a.Findings {
		if f.Severity == "warning" {
			out = append(out, f.Message)
		}
	}
	return out
}

// ReviewRisk is the risk class attached to the plan approval step.
func (a Analysis) ReviewRisk() domain.RiskClass {
	switch {
	case a.HighestRisk == domain.RiskR3:
		return domain.RiskR3
	case a.HighestRisk == domain.RiskR2:
		return domain.RiskR2
	case a.SideEffects > 0:
		if a.HighestRisk == domain.RiskR1 {
			return domain.RiskR1
		}
		return domain.RiskR2
	case a.RequiresReview:
		return domain.RiskR1
	default:
		return domain.RiskR0
	}
}

// AnalysisOptions supplies the effective tool catalogue (with profile risk
// overrides already applied) and the plan budget.
type AnalysisOptions struct {
	Tools    []tools.Descriptor
	Budget   Budget
	MaxSteps int
	// Simulate dry-runs the default path with mock results (see Simulate).
	Simulate bool
	Inputs   map[string]any
}

type analyzer struct {
	opts      AnalysisOptions
	risks     map[string]domain.RiskClass
	descs     map[string]tools.Descriptor
	maxRisk   domain.RiskClass
	uses      []ToolUse
	findings  []Finding
	maxWidth  int
	maxIter   int
	maxDepth  int
	totalNode int
	approvals int
}

// Analyze validates the safety and resource envelope of a compiled flow.
// entryApproved is true when an enclosing flow already passed an approval
// node before entering this graph (subflows inherit the caller's state).
func Analyze(spec flow.Spec, opts AnalysisOptions) Analysis {
	opts.Budget = opts.Budget.withDefaults()
	a := &analyzer{opts: opts, risks: map[string]domain.RiskClass{}, descs: map[string]tools.Descriptor{}}
	for _, d := range opts.Tools {
		a.risks[d.Name] = d.Risk
		a.descs[d.Name] = d
	}
	a.analyzeSpec(spec, false, 0)
	if a.maxWidth == 0 {
		a.maxWidth = 0
	}
	est := estimateFrom(spec, spec.Entry, "", nil)
	a.totalNode = totalNodes(spec)
	a.checkBudget(spec, est, opts.Budget)
	uses := append([]ToolUse(nil), a.uses...)
	sort.Slice(uses, func(i, j int) bool {
		if uses[i].NodeID == uses[j].NodeID {
			return uses[i].Tool < uses[j].Tool
		}
		return uses[i].NodeID < uses[j].NodeID
	})
	findings := append([]Finding(nil), a.findings...)
	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Severity == findings[j].Severity {
			return findings[i].Code < findings[j].Code
		}
		return findings[i].Severity == "error"
	})
	out := Analysis{
		Nodes:               a.totalNode,
		MaxDepth:            a.maxDepth,
		ParallelWidth:       a.maxWidth,
		MaxIterations:       a.maxIter,
		MaxSteps:            spec.Limits.MaxSteps,
		EstimatedRuns:       est.execs,
		EstimatedSteps:      est.steps,
		EstimatedModelCalls: est.modelCalls,
		HighestRisk:         a.maxRisk,
		SideEffects:         sideEffectCount(uses),
		ApprovalNodes:       a.approvals,
		Tools:               uses,
		Findings:            findings,
	}
	if out.HighestRisk == "" {
		out.HighestRisk = domain.RiskR0
	}
	if opts.Budget.MaxOutputTokens > 0 && est.modelCalls > 0 {
		out.WorstCaseOutputTokens = int64(est.modelCalls) * opts.Budget.MaxOutputTokens
		out.EstimatedOutputTokens = out.WorstCaseOutputTokens
		if t := opts.Budget.TypicalOutputTokens; t > 0 && t < opts.Budget.MaxOutputTokens {
			out.EstimatedOutputTokens = int64(est.modelCalls) * t
		}
	}
	if opts.Simulate {
		sim := Simulate(spec, opts.Inputs, opts.Tools)
		out.Simulation = &sim
		out.Findings = append(out.Findings, simulationFindings(spec, sim)...)
	}
	out.RequiresReview = requiresReview(out)
	return out
}

func (a *analyzer) analyzeSpec(spec flow.Spec, entryApproved bool, depth int) {
	if depth > a.maxDepth {
		a.maxDepth = depth
	}
	index := make(map[string]*flow.Node, len(spec.Nodes))
	for i := range spec.Nodes {
		index[spec.Nodes[i].ID] = &spec.Nodes[i]
	}
	approved := approvedBefore(spec, index, entryApproved)
	for i := range spec.Nodes {
		n := &spec.Nodes[i]
		switch n.Type {
		case flow.NodeAgent:
			if len(n.Tools) == 0 {
				a.addFinding("agent_tools_unbounded", "warning", n.ID,
					fmt.Sprintf("agent node %q does not pin its tools; it can use any tool the profile allows", n.ID))
				risk := a.highestVisibleRisk()
				a.observeRisk(risk)
				if risk.HasSideEffects() {
					a.uses = append(a.uses, ToolUse{NodeID: n.ID, Tool: "*", Risk: risk, Approved: approved[n.ID]})
				}
				continue
			}
			for _, name := range n.Tools {
				a.addTool(n.ID, name, approved[n.ID])
			}
		case flow.NodeTool:
			a.addTool(n.ID, n.Tool, approved[n.ID])
		case flow.NodeSubflow:
			if n.Flow != nil {
				a.analyzeSpec(*n.Flow, approved[n.ID], depth+1)
			}
		case flow.NodeParallel:
			if len(n.Branches) > a.maxWidth {
				a.maxWidth = len(n.Branches)
			}
		case flow.NodeApproval:
			a.approvals++
		case flow.NodeLoop:
			iters := n.Iterations
			if iters <= 0 {
				iters = spec.Limits.MaxIterations
			}
			if iters > a.maxIter {
				a.maxIter = iters
			}
		}
	}
}

func (a *analyzer) addTool(nodeID, name string, approved bool) {
	risk, ok := a.risks[name]
	if !ok {
		// With a catalogue, a name the model invented is a hard error: the
		// compiler only checks the profile allowlist, which may be "*". If
		// analysis is used without a catalogue, assume a write and review it.
		risk = domain.RiskR2
		if len(a.opts.Tools) > 0 {
			a.addFinding("unknown_tool", "error", nodeID,
				fmt.Sprintf("node %q uses tool %q, which is not in the available tool list", nodeID, name))
		}
	}
	a.observeRisk(risk)
	u := ToolUse{NodeID: nodeID, Tool: name, Risk: risk, Approved: approved}
	a.uses = append(a.uses, u)
	if risk.HasSideEffects() && !approved {
		a.addFinding("side_effect_without_approval", "warning", nodeID,
			fmt.Sprintf("side-effecting tool %q at node %q is reachable without an approval node", name, nodeID))
	}
	if risk == domain.RiskR3 {
		a.addFinding("r3_tool", "warning", nodeID,
			fmt.Sprintf("node %q can run the irreversible tool %q; the plan needs review", nodeID, name))
	}
}

func (a *analyzer) observeRisk(r domain.RiskClass) {
	if riskRank(r) > riskRank(a.maxRisk) {
		a.maxRisk = r
	}
}

func (a *analyzer) highestVisibleRisk() domain.RiskClass {
	out := domain.RiskR0
	for _, d := range a.opts.Tools {
		if riskRank(d.Risk) > riskRank(out) {
			out = d.Risk
		}
	}
	return out
}

func (a *analyzer) addFinding(code, severity, nodeID, msg string) {
	a.findings = append(a.findings, Finding{Code: code, Severity: severity, NodeID: nodeID, Message: msg})
}

func (a *analyzer) checkBudget(spec flow.Spec, est estimate, budget Budget) {
	if a.totalNode > budget.MaxNodes {
		a.addFinding("node_budget", "error", "",
			fmt.Sprintf("plan has %d nodes including subflows, limit is %d", a.totalNode, budget.MaxNodes))
	}
	if a.maxWidth > budget.MaxChildren {
		a.addFinding("child_budget", "error", "",
			fmt.Sprintf("plan fans out to %d parallel children, limit is %d", a.maxWidth, budget.MaxChildren))
	}
	if a.maxIter > budget.MaxIterations {
		a.addFinding("iteration_budget", "error", "",
			fmt.Sprintf("plan allows %d loop iterations, limit is %d", a.maxIter, budget.MaxIterations))
	}
	if est.execs > budget.MaxTotalRuns {
		a.addFinding("run_budget", "error", "",
			fmt.Sprintf("plan can start %d child runs, limit is %d", est.execs, budget.MaxTotalRuns))
	}
	if est.modelCalls > budget.MaxModelCalls {
		a.addFinding("model_call_budget", "error", "",
			fmt.Sprintf("plan can make %d model calls, limit is %d; lower agent max_steps or the number of agents",
				est.modelCalls, budget.MaxModelCalls))
	}
	if budget.MaxOutputTokens > 0 {
		per, what := budget.MaxOutputTokens, "can emit"
		if budget.TypicalOutputTokens > 0 && budget.TypicalOutputTokens < per {
			per, what = budget.TypicalOutputTokens, "is expected to emit"
		}
		tokens := int64(est.modelCalls) * per
		if budget.MaxEstimatedTokens > 0 && tokens > budget.MaxEstimatedTokens {
			a.addFinding("token_budget", "error", "",
				fmt.Sprintf("plan %s %d tokens, limit is %d", what, tokens, budget.MaxEstimatedTokens))
		}
	}
	if a.maxDepth > budget.MaxDepth {
		a.addFinding("depth_budget", "error", "",
			fmt.Sprintf("plan nests %d subflows, limit is %d", a.maxDepth, budget.MaxDepth))
	}
	if spec.Limits.MaxTotalRuns > budget.MaxTotalRuns {
		a.addFinding("run_limit", "error", "",
			fmt.Sprintf("flow max_total_runs=%d exceeds the plan budget %d", spec.Limits.MaxTotalRuns, budget.MaxTotalRuns))
	}
	if spec.Limits.MaxChildren > budget.MaxChildren {
		a.addFinding("child_limit", "error", "",
			fmt.Sprintf("flow max_children=%d exceeds the plan budget %d", spec.Limits.MaxChildren, budget.MaxChildren))
	}
	if spec.Limits.MaxIterations > budget.MaxIterations {
		a.addFinding("iteration_limit", "error", "",
			fmt.Sprintf("flow max_iterations=%d exceeds the plan budget %d", spec.Limits.MaxIterations, budget.MaxIterations))
	}
	if spec.Limits.MaxDepth > budget.MaxDepth {
		a.addFinding("depth_limit", "error", "",
			fmt.Sprintf("flow max_depth=%d exceeds the plan budget %d", spec.Limits.MaxDepth, budget.MaxDepth))
	}
}

func requiresReview(a Analysis) bool {
	if len(a.Errors()) > 0 {
		return true
	}
	if a.HighestRisk == domain.RiskR3 || a.HighestRisk == domain.RiskR2 || a.SideEffects > 0 {
		return true
	}
	for _, f := range a.Findings {
		if f.Severity == "warning" {
			return true
		}
	}
	return false
}

func sideEffectCount(uses []ToolUse) int {
	n := 0
	for _, u := range uses {
		if u.Risk.HasSideEffects() {
			n++
		}
	}
	return n
}

func riskRank(r domain.RiskClass) int {
	switch r {
	case domain.RiskR3:
		return 3
	case domain.RiskR2:
		return 2
	case domain.RiskR1:
		return 1
	default:
		return 0
	}
}

// approvedBefore computes, for every node, whether every path from entry has
// passed an approval node. Conditional edges take the AND because any branch
// may execute; loops do not inherit approval from their own back edge.
func approvedBefore(spec flow.Spec, index map[string]*flow.Node, entryApproved bool) map[string]bool {
	approved := make(map[string]bool, len(spec.Nodes))
	known := make(map[string]bool, len(spec.Nodes))
	for i := range spec.Nodes {
		approved[spec.Nodes[i].ID] = true // neutral for the AND
	}
	approved[spec.Entry] = entryApproved
	known[spec.Entry] = true
	backEdge := loopBackEdges(spec, index)
	// Subflow entries are implicit nodes; compile guarantees reachability.
	for pass := 0; pass < len(spec.Nodes)+2; pass++ {
		changed := false
		for i := range spec.Nodes {
			n := &spec.Nodes[i]
			out := approved[n.ID] || n.Type == flow.NodeApproval
			next := make([]string, 0, len(n.Next)+len(n.Branches)+1)
			for _, e := range n.Next {
				if backEdge[n.ID+"\x00"+e.To] {
					continue
				}
				next = append(next, e.To)
			}
			if n.Type == flow.NodeParallel {
				next = append(next, n.Branches...)
			}
			if n.Type == flow.NodeLoop && n.Body != "" {
				next = append(next, n.Body)
			}
			for _, to := range next {
				if to == spec.Entry {
					continue
				}
				if !known[to] {
					approved[to] = out
					known[to] = true
					changed = true
					continue
				}
				if approved[to] != (approved[to] && out) {
					approved[to] = approved[to] && out
					changed = true
				}
			}
		}
		if !changed {
			break
		}
	}
	return approved
}

// loopBackEdges identifies edges from a loop body back to its loop node. Those
// edges must not carry approval state across iterations.
func loopBackEdges(spec flow.Spec, index map[string]*flow.Node) map[string]bool {
	out := map[string]bool{}
	for i := range spec.Nodes {
		n := &spec.Nodes[i]
		if n.Type != flow.NodeLoop || n.Body == "" {
			continue
		}
		seen := map[string]bool{}
		var walk func(string)
		walk = func(id string) {
			if id == n.ID || seen[id] {
				return
			}
			seen[id] = true
			bn := index[id]
			if bn == nil {
				return
			}
			for _, e := range bn.Next {
				if e.To == n.ID {
					out[id+"\x00"+n.ID] = true
				}
				walk(e.To)
			}
		}
		walk(n.Body)
	}
	return out
}

type estimate struct {
	execs      int
	steps      int
	modelCalls int
	depth      int
}

// estimateFrom returns a conservative structural upper bound for one entry
// path. Conditional successors take the maximum because only one runs; a
// parallel node sums its branches.
func estimateFrom(spec flow.Spec, id, stop string, stack map[string]bool) estimate {
	if id == "" || id == stop {
		return estimate{}
	}
	if stack == nil {
		stack = map[string]bool{}
	}
	if stack[id] {
		return estimate{}
	}
	stack[id] = true
	defer delete(stack, id)
	index := make(map[string]*flow.Node, len(spec.Nodes))
	for i := range spec.Nodes {
		index[spec.Nodes[i].ID] = &spec.Nodes[i]
	}
	return estimateNode(spec, index, id, stop, stack)
}

func estimateNode(spec flow.Spec, index map[string]*flow.Node, id, stop string, stack map[string]bool) estimate {
	n := index[id]
	if n == nil {
		return estimate{}
	}
	e := estimate{steps: 1}
	switch n.Type {
	case flow.NodeAgent:
		e.execs = 1
		steps := n.MaxSteps
		if steps <= 0 {
			steps = 6
		}
		e.modelCalls = steps
	case flow.NodeTool:
		e.execs = 1
	case flow.NodeSubflow:
		e.execs = 1
		if n.Flow != nil {
			sub := estimateFrom(*n.Flow, n.Flow.Entry, "", nil)
			e.execs += sub.execs
			e.steps += sub.steps
			e.modelCalls += sub.modelCalls
			if sub.depth+1 > e.depth {
				e.depth = sub.depth + 1
			}
		}
	case flow.NodeParallel:
		for _, b := range n.Branches {
			sub := estimateFrom(spec, b, "", map[string]bool{})
			e.execs += sub.execs
			e.steps += sub.steps
			e.modelCalls += sub.modelCalls
			if sub.depth > e.depth {
				e.depth = sub.depth
			}
		}
	case flow.NodeLoop:
		iters := n.Iterations
		if iters <= 0 {
			iters = spec.Limits.MaxIterations
		}
		if iters < 1 {
			iters = 1
		}
		body := estimateFrom(spec, n.Body, n.ID, map[string]bool{})
		e.execs = body.execs * iters
		e.steps += body.steps * iters
		e.modelCalls = body.modelCalls * iters
		if body.depth > e.depth {
			e.depth = body.depth
		}
	}
	for _, next := range n.Next {
		sub := estimateFrom(spec, next.To, stop, stack)
		e.execs += sub.execs
		e.steps += sub.steps
		e.modelCalls += sub.modelCalls
		if sub.depth > e.depth {
			e.depth = sub.depth
		}
	}
	return e
}

func totalNodes(spec flow.Spec) int {
	n := len(spec.Nodes)
	for i := range spec.Nodes {
		if spec.Nodes[i].Type == flow.NodeSubflow && spec.Nodes[i].Flow != nil {
			n += totalNodes(*spec.Nodes[i].Flow)
		}
	}
	return n
}
