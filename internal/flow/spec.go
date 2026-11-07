package flow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Version is the only supported DSL schema version.
const Version = 1

// Default limits keep a malformed flow from multiplying runs.
const (
	DefaultMaxSteps      = 50
	DefaultMaxChildren   = 8
	DefaultMaxTotalRuns  = 32
	DefaultMaxDepth      = 3
	DefaultMaxIterations = 10
	MaxAllowedSteps      = 200
	MaxAllowedChildren   = 32
	MaxAllowedTotalRuns  = 128
	MaxAllowedDepth      = 5
	MaxAllowedIterations = 100
)

// NodeType is the kind of work a node performs.
type NodeType string

const (
	NodeAgent    NodeType = "agent"
	NodeTool     NodeType = "tool"
	NodeSubflow  NodeType = "subflow"
	NodeParallel NodeType = "parallel"
	NodeJoin     NodeType = "join"
	NodeApproval NodeType = "approval"
	NodeInput    NodeType = "input"
	NodeLoop     NodeType = "loop"
	NodeEnd      NodeType = "end"
)

// Spec is one immutable orchestration definition.
type Spec struct {
	Version int     `json:"version"`
	Entry   string  `json:"entry"`
	Inputs  []Input `json:"inputs,omitempty"`
	Nodes   []Node  `json:"nodes"`
	Output  string  `json:"output,omitempty"`
	Limits  Limits  `json:"limits,omitempty"`
}

// Input declares a value the caller must provide when the flow starts.
type Input struct {
	Name     string          `json:"name"`
	Required bool            `json:"required,omitempty"`
	Schema   json.RawMessage `json:"schema,omitempty"`
	Default  any             `json:"default,omitempty"`
}

// Limits bound one flow run and every child run it spawns.
type Limits struct {
	MaxSteps      int `json:"max_steps,omitempty"`
	MaxChildren   int `json:"max_children,omitempty"`
	MaxTotalRuns  int `json:"max_total_runs,omitempty"`
	MaxDepth      int `json:"max_depth,omitempty"`
	MaxIterations int `json:"max_iterations,omitempty"`
}

// Node is one vertex of the flow graph.
type Node struct {
	ID       string   `json:"id"`
	Type     NodeType `json:"type"`
	Role     string   `json:"role,omitempty"`
	Prompt   string   `json:"prompt,omitempty"`
	Tool     string   `json:"tool,omitempty"`
	Tools    []string `json:"tools,omitempty"`
	Model    string   `json:"model_alias,omitempty"`
	MaxSteps int      `json:"max_steps,omitempty"`

	Args json.RawMessage `json:"args,omitempty"`

	Branches []string `json:"branches,omitempty"`
	Wait     []string `json:"wait,omitempty"`
	Join     string   `json:"join_policy,omitempty"`
	N        int      `json:"n,omitempty"`
	OnError  string   `json:"on_error,omitempty"`

	Risk      string `json:"risk,omitempty"`
	Approvers int    `json:"approvers,omitempty"`

	InputSchema json.RawMessage `json:"input_schema,omitempty"`
	Output      string          `json:"output,omitempty"`

	Body       string `json:"body,omitempty"`
	Until      string `json:"until,omitempty"`
	Iterations int    `json:"max_iterations,omitempty"`

	Flow *Spec  `json:"flow,omitempty"`
	Next []Edge `json:"next,omitempty"`
}

// Edge is a conditional transition. An empty When is the default edge and
// must be unique.
type Edge struct {
	To   string `json:"to"`
	When string `json:"when,omitempty"`
}

// CompileOptions validates a flow against the profile that owns it.
type CompileOptions struct {
	// AllowTool reports whether the profile allows the named tool. Nil means
	// tools are not checked (tests and flows without a profile).
	AllowTool func(name string) bool
}

// Parse decodes a flow spec strictly.
func Parse(raw json.RawMessage) (Spec, error) {
	var s Spec
	if len(bytes.TrimSpace(raw)) == 0 {
		return s, fmt.Errorf("flow spec is empty")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return s, fmt.Errorf("flow: %w", err)
	}
	return s, nil
}

// Compile parses, validates, applies defaults and returns the canonical bytes
// and their SHA-256. The canonical plan is what a run pins and replays.
func Compile(raw json.RawMessage, opts CompileOptions) (Spec, []byte, string, error) {
	s, err := Parse(raw)
	if err != nil {
		return s, nil, "", err
	}
	if err := s.applyDefaults(); err != nil {
		return s, nil, "", err
	}
	if err := s.Validate(opts); err != nil {
		return s, nil, "", err
	}
	canon, err := canonicalJSON(s)
	if err != nil {
		return s, nil, "", err
	}
	sum := sha256.Sum256(canon)
	return s, canon, hex.EncodeToString(sum[:]), nil
}

// Hash returns the canonical hash of an already validated spec.
func (s Spec) Hash() string {
	canon, err := canonicalJSON(s)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:])
}

func (s *Spec) applyDefaults() error {
	if s.Version == 0 {
		s.Version = Version
	}
	if s.Version != Version {
		return fmt.Errorf("flow: unsupported version %d (want %d)", s.Version, Version)
	}
	if s.Limits.MaxSteps <= 0 {
		s.Limits.MaxSteps = DefaultMaxSteps
	}
	if s.Limits.MaxChildren <= 0 {
		s.Limits.MaxChildren = DefaultMaxChildren
	}
	if s.Limits.MaxTotalRuns <= 0 {
		s.Limits.MaxTotalRuns = DefaultMaxTotalRuns
	}
	if s.Limits.MaxDepth <= 0 {
		s.Limits.MaxDepth = DefaultMaxDepth
	}
	if s.Limits.MaxIterations <= 0 {
		s.Limits.MaxIterations = DefaultMaxIterations
	}
	if s.Limits.MaxSteps > MaxAllowedSteps {
		return fmt.Errorf("flow: max_steps %d exceeds %d", s.Limits.MaxSteps, MaxAllowedSteps)
	}
	if s.Limits.MaxChildren > MaxAllowedChildren {
		return fmt.Errorf("flow: max_children %d exceeds %d", s.Limits.MaxChildren, MaxAllowedChildren)
	}
	if s.Limits.MaxTotalRuns > MaxAllowedTotalRuns {
		return fmt.Errorf("flow: max_total_runs %d exceeds %d", s.Limits.MaxTotalRuns, MaxAllowedTotalRuns)
	}
	if s.Limits.MaxDepth > MaxAllowedDepth {
		return fmt.Errorf("flow: max_depth %d exceeds %d", s.Limits.MaxDepth, MaxAllowedDepth)
	}
	if s.Limits.MaxIterations > MaxAllowedIterations {
		return fmt.Errorf("flow: max_iterations %d exceeds %d", s.Limits.MaxIterations, MaxAllowedIterations)
	}
	if len(s.Nodes) == 0 {
		return fmt.Errorf("flow: at least one node is required")
	}
	return nil
}

var idRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$`)
var varRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

// Validate checks structure, expressions, edges and limits.
func (s Spec) Validate(opts CompileOptions) error {
	return s.validate(opts, 0, map[string]bool{})
}

func (s Spec) validate(opts CompileOptions, depth int, seen map[string]bool) error {
	if depth > MaxAllowedDepth+1 {
		return fmt.Errorf("flow: subflow nesting exceeds %d", MaxAllowedDepth+1)
	}
	if s.Entry == "" {
		return fmt.Errorf("flow: entry is required")
	}
	index := make(map[string]*Node, len(s.Nodes))
	for i := range s.Nodes {
		n := &s.Nodes[i]
		if n.ID == "" || !idRE.MatchString(n.ID) {
			return fmt.Errorf("flow: invalid node id %q", n.ID)
		}
		if _, dup := index[n.ID]; dup {
			return fmt.Errorf("flow: duplicate node id %q", n.ID)
		}
		index[n.ID] = n
	}
	// Branch nodes are leaves of the graph: the parallel node owns their
	// execution and output, so they do not need next edges.
	branchNodes := map[string]bool{}
	for i := range s.Nodes {
		if s.Nodes[i].Type == NodeParallel {
			for _, b := range s.Nodes[i].Branches {
				branchNodes[b] = true
			}
		}
	}
	if _, ok := index[s.Entry]; !ok {
		return fmt.Errorf("flow: entry %q does not exist", s.Entry)
	}
	inputs := make(map[string]bool, len(s.Inputs))
	for _, in := range s.Inputs {
		if in.Name == "" || !varRE.MatchString(in.Name) {
			return fmt.Errorf("flow: invalid input name %q", in.Name)
		}
		if inputs[in.Name] {
			return fmt.Errorf("flow: duplicate input %q", in.Name)
		}
		inputs[in.Name] = true
		if len(in.Schema) > 0 && !json.Valid(in.Schema) {
			return fmt.Errorf("flow: input %q schema is not valid JSON", in.Name)
		}
	}

	refCheck := func(path []string) error {
		if len(path) == 0 {
			return nil
		}
		switch path[0] {
		case "input", "vars", "loop":
			return nil
		case "nodes":
			if len(path) < 2 {
				return fmt.Errorf("flow: nodes reference needs a node id")
			}
			if _, ok := index[path[1]]; !ok {
				return fmt.Errorf("flow: expression references unknown node %q", path[1])
			}
			return nil
		default:
			return fmt.Errorf("flow: unknown expression root %q", path[0])
		}
	}
	checkExpr := func(where, src string) error {
		if strings.TrimSpace(src) == "" {
			return nil
		}
		if err := ValidateExpr(src, refCheck); err != nil {
			return fmt.Errorf("flow: %s: %w", where, err)
		}
		return nil
	}
	checkInterpolations := func(where string, raw json.RawMessage) error {
		if len(raw) == 0 {
			return nil
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return fmt.Errorf("flow: %s: %w", where, err)
		}
		if err := ValidateInterpolations(v, refCheck); err != nil {
			return fmt.Errorf("flow: %s: %w", where, err)
		}
		return nil
	}

	toolAllowed := func(name string) bool {
		if name == "" {
			return false
		}
		if opts.AllowTool == nil {
			return true
		}
		return opts.AllowTool(name)
	}

	for i := range s.Nodes {
		n := &s.Nodes[i]
		switch n.Type {
		case NodeAgent:
			if n.Role == "" && n.Prompt == "" {
				return fmt.Errorf("flow: agent node %q needs role or prompt", n.ID)
			}
			for _, t := range n.Tools {
				if strings.Contains(t, "*") {
					return fmt.Errorf("flow: agent node %q tools must be exact names, got %q", n.ID, t)
				}
				if !toolAllowed(t) {
					return fmt.Errorf("flow: agent node %q uses tool %q, which the profile does not allow", n.ID, t)
				}
			}
			if n.MaxSteps < 0 || n.MaxSteps > MaxAllowedSteps {
				return fmt.Errorf("flow: agent node %q max_steps out of range", n.ID)
			}
		case NodeTool:
			if n.Tool == "" {
				return fmt.Errorf("flow: tool node %q needs tool", n.ID)
			}
			if !toolAllowed(n.Tool) {
				return fmt.Errorf("flow: tool node %q uses tool %q, which the profile does not allow", n.ID, n.Tool)
			}
			if len(n.Tools) > 0 && !containsString(n.Tools, n.Tool) {
				return fmt.Errorf("flow: tool node %q must include its tool %q in tools", n.ID, n.Tool)
			}
			if len(n.Args) > 0 {
				var obj map[string]any
				if err := json.Unmarshal(n.Args, &obj); err != nil {
					return fmt.Errorf("flow: tool node %q args must be a JSON object: %w", n.ID, err)
				}
			}
			if err := checkInterpolations("tool node "+n.ID+" args", n.Args); err != nil {
				return err
			}
		case NodeSubflow:
			if n.Flow == nil {
				return fmt.Errorf("flow: subflow node %q needs flow", n.ID)
			}
			if err := n.Flow.applyDefaults(); err != nil {
				return fmt.Errorf("flow: subflow node %q: %w", n.ID, err)
			}
			if err := n.Flow.validate(opts, depth+1, seen); err != nil {
				return fmt.Errorf("flow: subflow node %q: %w", n.ID, err)
			}
		case NodeParallel:
			if len(n.Branches) < 2 {
				return fmt.Errorf("flow: parallel node %q needs at least two branches", n.ID)
			}
			seenBranch := map[string]bool{}
			for _, b := range n.Branches {
				bn, ok := index[b]
				if !ok {
					return fmt.Errorf("flow: parallel node %q branch %q does not exist", n.ID, b)
				}
				if seenBranch[b] {
					return fmt.Errorf("flow: parallel node %q has duplicate branch %q", n.ID, b)
				}
				seenBranch[b] = true
				switch bn.Type {
				case NodeAgent, NodeTool, NodeSubflow:
				default:
					return fmt.Errorf("flow: parallel node %q branch %q must be agent, tool or subflow", n.ID, b)
				}
				if len(bn.Next) > 0 {
					return fmt.Errorf("flow: parallel branch %q must not declare next edges; wrap it in a subflow", b)
				}
			}
		case NodeJoin:
			if len(n.Wait) == 0 {
				return fmt.Errorf("flow: join node %q needs wait", n.ID)
			}
			for _, w := range n.Wait {
				wn, ok := index[w]
				if !ok {
					return fmt.Errorf("flow: join node %q waits for unknown node %q", n.ID, w)
				}
				if wn.Type != NodeParallel {
					return fmt.Errorf("flow: join node %q can only wait for parallel nodes, got %q", n.ID, w)
				}
			}
			switch n.Join {
			case "", "all", "any", "n_of_m":
			default:
				return fmt.Errorf("flow: join node %q has invalid join_policy %q", n.ID, n.Join)
			}
			if n.Join == "n_of_m" && (n.N < 1 || n.N > len(n.Wait)*MaxAllowedChildren) {
				return fmt.Errorf("flow: join node %q needs a valid n", n.ID)
			}
			if n.OnError != "" && n.OnError != "fail" && n.OnError != "continue" {
				return fmt.Errorf("flow: join node %q has invalid on_error %q", n.ID, n.OnError)
			}
		case NodeApproval:
			switch n.Risk {
			case "", "R1", "R2", "R3":
			default:
				return fmt.Errorf("flow: approval node %q has invalid risk %q", n.ID, n.Risk)
			}
			if n.Approvers < 0 || n.Approvers > 5 {
				return fmt.Errorf("flow: approval node %q approvers must be 0..5", n.ID)
			}
		case NodeInput:
			if n.Output == "" || !varRE.MatchString(n.Output) {
				return fmt.Errorf("flow: input node %q needs a valid output variable", n.ID)
			}
			if len(n.InputSchema) > 0 {
				var obj map[string]any
				if err := json.Unmarshal(n.InputSchema, &obj); err != nil {
					return fmt.Errorf("flow: input node %q input_schema must be an object: %w", n.ID, err)
				}
			}
		case NodeLoop:
			if n.Body == "" {
				return fmt.Errorf("flow: loop node %q needs body", n.ID)
			}
			if _, ok := index[n.Body]; !ok {
				return fmt.Errorf("flow: loop node %q body %q does not exist", n.ID, n.Body)
			}
			if n.Body == n.ID {
				return fmt.Errorf("flow: loop node %q cannot be its own body", n.ID)
			}
			if n.Iterations < 0 || n.Iterations > MaxAllowedIterations {
				return fmt.Errorf("flow: loop node %q max_iterations out of range", n.ID)
			}
			if err := checkExpr("loop node "+n.ID+" until", n.Until); err != nil {
				return err
			}
			if !s.reaches(index, n.Body, n.ID, map[string]bool{}) {
				return fmt.Errorf("flow: loop node %q body %q never returns to the loop", n.ID, n.Body)
			}
		case NodeEnd:
			if len(n.Next) > 0 {
				return fmt.Errorf("flow: end node %q must not have next edges", n.ID)
			}
		default:
			return fmt.Errorf("flow: node %q has unknown type %q", n.ID, n.Type)
		}

		for _, e := range n.Next {
			if _, ok := index[e.To]; !ok {
				return fmt.Errorf("flow: node %q edge targets unknown node %q", n.ID, e.To)
			}
			if err := checkExpr("node "+n.ID+" edge to "+e.To, e.When); err != nil {
				return err
			}
		}
		if n.Type != NodeEnd && !branchNodes[n.ID] && len(n.Next) == 0 {
			return fmt.Errorf("flow: node %q needs next edges", n.ID)
		}
		defaults := 0
		for _, e := range n.Next {
			if strings.TrimSpace(e.When) == "" {
				defaults++
			}
		}
		if defaults > 1 {
			return fmt.Errorf("flow: node %q has more than one default edge", n.ID)
		}
		if n.Type != NodeInput && strings.TrimSpace(n.Output) != "" {
			if err := ValidateInterpolation(n.Output, refCheck); err != nil {
				return fmt.Errorf("flow: node %s output: %w", n.ID, err)
			}
		}
	}

	if strings.TrimSpace(s.Output) != "" {
		if err := ValidateInterpolation(s.Output, refCheck); err != nil {
			return fmt.Errorf("flow: flow output: %w", err)
		}
	}

	// Every node must be reachable from entry. Branch nodes are reached via
	// their parallel node; loop bodies via their loop node.
	reachable := map[string]bool{}
	s.walkReachable(index, s.Entry, reachable)
	for i := range s.Nodes {
		if !reachable[s.Nodes[i].ID] {
			return fmt.Errorf("flow: node %q is not reachable from entry", s.Nodes[i].ID)
		}
	}
	// Cycles are only allowed through a loop node.
	if err := s.checkCycles(index, map[string]int{}, ""); err != nil {
		return err
	}
	return nil
}

func (s Spec) walkReachable(index map[string]*Node, id string, seen map[string]bool) {
	if seen[id] {
		return
	}
	seen[id] = true
	n := index[id]
	if n == nil {
		return
	}
	for _, e := range n.Next {
		s.walkReachable(index, e.To, seen)
	}
	if n.Type == NodeParallel {
		for _, b := range n.Branches {
			s.walkReachable(index, b, seen)
		}
	}
	if n.Type == NodeLoop {
		s.walkReachable(index, n.Body, seen)
	}
}

// reaches reports whether from can reach target following next/body/branch
// edges. It is used to require loop bodies to return to their loop.
func (s Spec) reaches(index map[string]*Node, from, target string, seen map[string]bool) bool {
	if from == target {
		return true
	}
	if seen[from] {
		return false
	}
	seen[from] = true
	n := index[from]
	if n == nil {
		return false
	}
	for _, e := range n.Next {
		if s.reaches(index, e.To, target, seen) {
			return true
		}
	}
	if n.Type == NodeLoop && n.Body != "" {
		if s.reaches(index, n.Body, target, seen) {
			return true
		}
	}
	return false
}

func (s Spec) checkCycles(index map[string]*Node, state map[string]int, stack string) error {
	// state: 0 unvisited, 1 in stack, 2 done.
	for i := range s.Nodes {
		id := s.Nodes[i].ID
		if state[id] == 2 {
			continue
		}
		if err := s.dfsCycle(index, id, state); err != nil {
			return err
		}
	}
	return nil
}

func (s Spec) dfsCycle(index map[string]*Node, id string, state map[string]int) error {
	state[id] = 1
	n := index[id]
	if n == nil {
		state[id] = 2
		return nil
	}
	next := make([]string, 0, len(n.Next)+len(n.Branches)+1)
	for _, e := range n.Next {
		next = append(next, e.To)
	}
	if n.Type == NodeParallel {
		next = append(next, n.Branches...)
	}
	if n.Type == NodeLoop && n.Body != "" {
		next = append(next, n.Body)
	}
	for _, to := range next {
		switch state[to] {
		case 1:
			src := index[id]
			dst := index[to]
			if (src != nil && src.Type == NodeLoop) || (dst != nil && dst.Type == NodeLoop) {
				continue // a bounded loop is the only allowed cycle
			}
			return fmt.Errorf("flow: cycle %q -> %q does not pass through a loop node", id, to)
		case 0:
			if err := s.dfsCycle(index, to, state); err != nil {
				return err
			}
		}
	}
	state[id] = 2
	return nil
}

func containsString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return nil, err
	}
	return json.Marshal(generic)
}
