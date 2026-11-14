package work

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
)

// A Work graph ties long-lived goals together (design §8). It is separate
// from a kernel Flow, which orders tasks inside one run:
//
//   - ownership: a Work has at most one parent and any number of children.
//     The parent is the ceiling of what a child may do — its subjects,
//     write mode, data class, blast radius and budget — and a child can
//     only narrow it. A model cannot widen a child beyond its parent.
//   - dependencies: an edge upstream → downstream holds the downstream Work
//     until the upstream succeeded, ended, or signalled. Edges form a DAG;
//     a retry or replan is a new generation of a Work, never a back edge.
//
// A parent does not succeed because a child finished or wrote a report:
// it succeeds like every Work, through its own satisfied outcome.

// DependencyKind is when an edge is satisfied.
type DependencyKind string

const (
	// AfterSuccess: the upstream Work reached SUCCEEDED.
	AfterSuccess DependencyKind = "after_success"
	// AfterTerminal: the upstream Work ended, however it ended.
	AfterTerminal DependencyKind = "after_terminal"
	// AfterSignal: the upstream Work completed the stage named by
	// PredicateRef ("stage:<id>").
	AfterSignal DependencyKind = "after_signal"
)

// ValidDependencyKind reports whether k is known.
func ValidDependencyKind(k DependencyKind) bool {
	return k == AfterSuccess || k == AfterTerminal || k == AfterSignal
}

// Dependency is one edge: ChildWorkID waits for ParentWorkID. (The field
// names follow the store schema; "parent" here is the upstream of the edge,
// which need not be the owning parent of the Work.)
type Dependency struct {
	ParentWorkID string         `json:"parent_work_id"`
	ChildWorkID  string         `json:"child_work_id"`
	Kind         DependencyKind `json:"kind"`
	PredicateRef string         `json:"predicate_ref,omitempty"`
	// Required edges hold the downstream Work; an upstream that can no
	// longer satisfy a required edge fails the downstream. Optional edges
	// only order: they are released when the upstream ends in any way.
	Required    bool       `json:"required"`
	SatisfiedAt *time.Time `json:"satisfied_at,omitempty"`
}

// Normalize validates an edge.
func (d *Dependency) Normalize() error {
	if !cpkit.ValidID(d.ParentWorkID) || !cpkit.ValidID(d.ChildWorkID) {
		return invalidf("a dependency needs two Work ids")
	}
	if d.ParentWorkID == d.ChildWorkID {
		return invalidf("a Work cannot depend on itself")
	}
	if d.Kind == "" {
		d.Kind = AfterSuccess
	}
	if !ValidDependencyKind(d.Kind) {
		return invalidf("unknown dependency kind %q", d.Kind)
	}
	if d.Kind == AfterSignal && (d.PredicateRef == "" || len(d.PredicateRef) > 128) {
		return invalidf("an after_signal dependency needs a predicate_ref such as stage:<id>")
	}
	if d.Kind != AfterSignal && d.PredicateRef != "" {
		return invalidf("predicate_ref only applies to after_signal")
	}
	return nil
}

// GraphLimits bound a Work tree. They are taken from the root's template
// and cannot be raised by a child or a model.
type GraphLimits struct {
	MaxDepth    int `json:"max_depth,omitempty"`
	MaxChildren int `json:"max_children,omitempty"`
	// MaxTotalRuns caps the run budgets (limits.max_runs) of the whole tree.
	MaxTotalRuns int `json:"max_total_runs,omitempty"`
	// MaxActiveActions caps the action budgets (limits.max_actions) of the
	// whole tree.
	MaxActiveActions int `json:"max_active_actions,omitempty"`
	// MaxLifetime bounds the tree from the root's creation; a child's own
	// max_duration is clamped to what is left.
	MaxLifetime cpkit.Duration `json:"max_lifetime,omitempty"`
	// MaxCostUSD caps the model spend of the tree (0: each Work's own cap).
	MaxCostUSD float64 `json:"max_cost_usd,omitempty"`
}

// Graph defaults and bounds.
const (
	DefaultMaxDepth         = 3
	DefaultMaxChildren      = 12
	DefaultMaxTotalRuns     = 400
	DefaultMaxActiveActions = 64
	MaxGraphNodes           = 256
)

// WithDefaults fills unset limits.
func (l GraphLimits) WithDefaults() GraphLimits {
	l.MaxDepth = defInt(l.MaxDepth, DefaultMaxDepth)
	l.MaxChildren = defInt(l.MaxChildren, DefaultMaxChildren)
	l.MaxTotalRuns = defInt(l.MaxTotalRuns, DefaultMaxTotalRuns)
	l.MaxActiveActions = defInt(l.MaxActiveActions, DefaultMaxActiveActions)
	return l
}

func (l GraphLimits) validate() error {
	if l.MaxDepth < 0 || l.MaxDepth > 8 || l.MaxChildren < 0 || l.MaxChildren > 64 || l.MaxTotalRuns < 0 || l.MaxTotalRuns > 5000 ||
		l.MaxActiveActions < 0 || l.MaxActiveActions > 200 || l.MaxLifetime < 0 || l.MaxLifetime.D() > 180*24*time.Hour ||
		l.MaxCostUSD < 0 || l.MaxCostUSD > 100000 {
		return fmt.Errorf("graph limits are out of range")
	}
	return nil
}

// ChildSpec declares one child Work of a template graph.
type ChildSpec struct {
	// Key names the child inside the graph (stage-id syntax).
	Key      string `json:"key"`
	Kind     Kind   `json:"kind,omitempty"`
	Title    string `json:"title,omitempty"`
	Template string `json:"template"`
	// Intent and Description are goal templates rendered against the
	// parent (${goal.intent}, ${subject}, ${title}, ...).
	Intent      string `json:"intent,omitempty"`
	Description string `json:"description,omitempty"`
	// InheritSuccess copies the parent's machine goal into this child. Missing
	// or human goals are errors; they never fall back to human confirmation.
	InheritSuccess bool `json:"inherit_success,omitempty"`
	// Constraints narrow the parent's; unset fields inherit.
	Constraints Constraints `json:"constraints,omitempty"`
	// After lists sibling keys this child waits for.
	After []string `json:"after,omitempty"`
	// AfterKind is the kind of those edges (default after_success).
	AfterKind DependencyKind `json:"after_kind,omitempty"`
	// Required children must succeed for the parent's children stage.
	Required bool  `json:"required,omitempty"`
	Queue    Queue `json:"queue,omitempty"`
}

// InheritedSuccess returns an independent copy, so normalizing a child cannot
// mutate its parent's goal. Children opt in explicitly (for example mitigation,
// but not RCA). Validate both when admitting the parent and when spawning.
func (c ChildSpec) InheritedSuccess(parent GoalSpec) (*outcome.Spec, error) {
	if !c.InheritSuccess {
		return nil, nil
	}
	spec, err := machineSuccess(parent.Success)
	if err != nil {
		return nil, invalidf("child %s: %v", c.Key, err)
	}
	return spec, nil
}

// machineSuccess validates and copies a goal that cannot be satisfied by a
// human acknowledgement or by omitting every objective.
func machineSuccess(input *outcome.Spec) (*outcome.Spec, error) {
	if input == nil {
		return nil, invalidf("a machine success goal is required")
	}
	b, err := json.Marshal(input)
	if err != nil {
		return nil, invalidf("invalid machine success goal: %v", err)
	}
	var spec outcome.Spec
	if err := json.Unmarshal(b, &spec); err != nil {
		return nil, invalidf("invalid machine success goal: %v", err)
	}
	if err := spec.Normalize(); err != nil {
		return nil, invalidf("invalid machine success goal: %v", err)
	}
	required := false
	for _, objectives := range [][]outcome.Objective{spec.AllOf, spec.AnyOf} {
		for _, objective := range objectives {
			if objective.Kind == outcome.KindHuman {
				return nil, invalidf("machine success goals cannot contain human objectives")
			}
			required = required || !objective.Optional
		}
	}
	if !required {
		return nil, invalidf("a machine success goal needs a required objective")
	}
	return &spec, nil
}

// GraphSpec is the optional graph of a template (template_version 2): the
// children a children stage starts, and the limits of the tree. Templates
// without it run exactly as before.
type GraphSpec struct {
	Children []ChildSpec `json:"children,omitempty"`
	Limits   GraphLimits `json:"limits,omitempty"`
}

// ChildrenSpec configures a children stage.
type ChildrenSpec struct {
	// Keys selects children of the template graph (empty: all of them).
	Keys []string `json:"keys,omitempty"`
	// Timeout bounds the wait for the required children (0: the Work's own
	// max_duration applies).
	Timeout cpkit.Duration `json:"timeout,omitempty"`
}

func (g *GraphSpec) normalize() error {
	if err := g.Limits.validate(); err != nil {
		return err
	}
	if len(g.Children) > 64 {
		return fmt.Errorf("more than 64 children")
	}
	index := map[string]int{}
	for i := range g.Children {
		c := &g.Children[i]
		if !stageIDRe.MatchString(c.Key) {
			return fmt.Errorf("child key %q is invalid", c.Key)
		}
		if _, dup := index[c.Key]; dup {
			return fmt.Errorf("child key %q is duplicated", c.Key)
		}
		index[c.Key] = i
		if c.Kind != "" && !ValidKind(c.Kind) {
			return fmt.Errorf("child %s: unknown kind %q", c.Key, c.Kind)
		}
		if !stageIDRe.MatchString(c.Template) {
			return fmt.Errorf("child %s: template id is required", c.Key)
		}
		if c.AfterKind == "" {
			c.AfterKind = AfterSuccess
		}
		if c.AfterKind != AfterSuccess && c.AfterKind != AfterTerminal {
			return fmt.Errorf("child %s: after_kind must be after_success or after_terminal", c.Key)
		}
		if c.Queue != "" && !ValidQueue(c.Queue) {
			return fmt.Errorf("child %s: unknown queue %q", c.Key, c.Queue)
		}
		if len(c.Intent) > MaxIntent || len(c.Description) > MaxDescription || len(c.Title) > MaxTitle {
			return fmt.Errorf("child %s: goal text is too long", c.Key)
		}
	}
	// Sibling edges must name earlier-declared or other children and be
	// acyclic; a declaration order is not required, so check by search.
	edges := map[string][]string{}
	for _, c := range g.Children {
		for _, a := range c.After {
			if _, ok := index[a]; !ok || a == c.Key {
				return fmt.Errorf("child %s: after %q is not another child", c.Key, a)
			}
			edges[a] = append(edges[a], c.Key)
		}
	}
	if cyc := findCycle(edges); cyc != "" {
		return fmt.Errorf("children form a cycle through %q", cyc)
	}
	return nil
}

// findCycle returns a node on a cycle of the directed graph, or "".
func findCycle(edges map[string][]string) string {
	const (
		visiting = 1
		done     = 2
	)
	state := map[string]int{}
	var visit func(string) string
	visit = func(n string) string {
		switch state[n] {
		case visiting:
			return n
		case done:
			return ""
		}
		state[n] = visiting
		for _, m := range edges[n] {
			if c := visit(m); c != "" {
				return c
			}
		}
		state[n] = done
		return ""
	}
	for n := range edges {
		if c := visit(n); c != "" {
			return c
		}
	}
	return ""
}

// WouldCycle reports whether adding upstream → downstream to edges
// (upstream id → downstream ids) closes a cycle: it does when upstream is
// reachable from downstream.
func WouldCycle(edges map[string][]string, upstream, downstream string) bool {
	if upstream == downstream {
		return true
	}
	seen := map[string]bool{}
	stack := []string{downstream}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n == upstream {
			return true
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		stack = append(stack, edges[n]...)
	}
	return false
}

var writesRank = map[string]int{WritesForbidden: 0, WritesApprovalRequired: 1, "": 1, WritesPreauthorized: 2}

// Narrow derives a child's constraints from its parent's: unset fields
// inherit, set fields may only tighten. Asking for more than the parent
// has is an error, not a silent clamp — the caller should know.
func Narrow(parent, child Constraints) (Constraints, error) {
	out := child
	if out.ProductionWrites == "" {
		out.ProductionWrites = parent.ProductionWrites
	}
	if writesRank[out.ProductionWrites] > writesRank[parent.ProductionWrites] {
		return out, invalidf("a child Work cannot have wider production writes (%s) than its parent (%s)",
			out.ProductionWrites, firstNonEmptyStr(parent.ProductionWrites, WritesApprovalRequired))
	}
	if out.DataClass == "" || cpkit.DataClassRank(out.DataClass) < cpkit.DataClassRank(parent.DataClass) {
		// The data does not get less sensitive by being handed down.
		out.DataClass = parent.DataClass
	}
	if out.MaxBlastRadius == "" {
		out.MaxBlastRadius = parent.MaxBlastRadius
	} else if parent.MaxBlastRadius != "" && out.MaxBlastRadius != parent.MaxBlastRadius {
		return out, invalidf("a child Work cannot change its parent's blast radius limit (%s)", parent.MaxBlastRadius)
	}
	switch {
	case out.RequireFreshData == 0:
		out.RequireFreshData = parent.RequireFreshData
	case parent.RequireFreshData > 0 && out.RequireFreshData > parent.RequireFreshData:
		return out, invalidf("a child Work cannot accept older facts than its parent")
	}
	switch {
	case parent.MaxCostUSD > 0 && out.MaxCostUSD == 0:
		out.MaxCostUSD = parent.MaxCostUSD
	case parent.MaxCostUSD > 0 && out.MaxCostUSD > parent.MaxCostUSD:
		return out, invalidf("a child Work cannot have a larger cost budget than its parent")
	}
	return out, nil
}

// NarrowSubjects checks that every child subject lies inside the parent's
// subjects; a child without subjects inherits the parent's.
func NarrowSubjects(parent, child []string) ([]string, error) {
	if len(child) == 0 {
		return append([]string(nil), parent...), nil
	}
	refs, err := cpkit.NormalizeRefs(child)
	if err != nil {
		return nil, invalidf("subject_refs: %v", err)
	}
	for _, c := range refs {
		in := false
		for _, p := range parent {
			in = in || cpkit.RefMatches(p, c)
		}
		if !in {
			return nil, invalidf("subject %s is outside the parent Work's subjects", c)
		}
	}
	return refs, nil
}

func firstNonEmptyStr(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// NewChild is a request to create a child Work.
type NewChild struct {
	Work NewWork
	// ParentID owns the child. DependsOn are upstream Works the child
	// waits for (same tenant; typically siblings).
	ParentID  string
	DependsOn []Dependency
	// Key is the child's key in the parent's template graph ("" for a
	// child created through the API).
	Key string
}

// GraphNode is one Work of a graph view.
type GraphNode struct {
	ID       string `json:"id"`
	ParentID string `json:"parent_id,omitempty"`
	Depth    int    `json:"depth"`
	Kind     Kind   `json:"kind"`
	Status   Status `json:"status"`
	Title    string `json:"title"`
	Queue    Queue  `json:"queue"`
	Reason   string `json:"reason,omitempty"`
	Stage    string `json:"stage,omitempty"`
	// Held is true while required dependencies keep the Work from starting.
	Held bool `json:"held,omitempty"`
	// WaitingOn says what the Work waits for, in the user's terms: a timer,
	// an approval, another Work, a run, a human, or nothing.
	WaitingOn string `json:"waiting_on,omitempty"`
	// CostMicros and RunsStarted roll up nothing: they are this Work's own.
	CostMicros  int64 `json:"cost_micros"`
	RunsStarted int   `json:"runs_started"`
}

// Graph is a Work tree with its dependency edges.
type Graph struct {
	RootID string       `json:"root_id"`
	Nodes  []GraphNode  `json:"nodes"`
	Edges  []Dependency `json:"edges"`
	Limits GraphLimits  `json:"limits"`
}

// WorkGraphStore is the persistence port of the graph
// (internal/store/cpsql). Every method verifies tenant, bounds and
// acyclicity in the transaction that writes.
type WorkGraphStore interface {
	// CreateChild creates a child Work under its parent (idempotent on the
	// child's idempotency key) with its dependency edges. A child with
	// unsatisfied required dependencies is held until they are met.
	CreateChild(ctx context.Context, n NewChild) (Work, bool, error)
	// AddDependency adds an edge between two existing Works. expectedVersion
	// is the downstream Work's version (0: any). A cycle, a foreign tenant
	// or a started downstream is refused and nothing changes.
	AddDependency(ctx context.Context, tenantID string, dep Dependency, expectedVersion int64) error
	// ReadyChildren lists held children of a parent whose required
	// dependencies are all satisfied.
	ReadyChildren(ctx context.Context, tenantID, parentID string) ([]string, error)
	// CompleteChild settles the edges leaving a terminal Work: downstream
	// Works are released or failed, and the owning parent is woken. version
	// is the Work's version at its terminal status (0: any).
	CompleteChild(ctx context.Context, tenantID, childID string, version int64) error
	WorkGraph(ctx context.Context, tenantID, workID string) (Graph, error)
}

// WaitingOn derives the user-facing wait cause of a Work (design §8.5).
func WaitingOn(w Work, held bool) string {
	switch {
	case held:
		return "another Work"
	case w.Status.Terminal():
		return ""
	case w.Status == StatusSuspended:
		return "an operator (paused)"
	case w.Status == StatusBlocked:
		return "an operator (blocked: " + cpkit.Truncate(w.Reason, 120) + ")"
	}
	st, ok := w.CurrentStage()
	if !ok {
		return "a human confirmation"
	}
	switch st.Type {
	case StageRun, StageAgents:
		return "its runs"
	case StageWait:
		return "a timer or event"
	case StageAction:
		if w.Status == StatusWaiting {
			return "an approval or the action target"
		}
		return "an action"
	case StageVerify:
		return "the outcome check"
	case StageConfirm:
		return "a human confirmation"
	case StageChildren:
		return "its child Works"
	}
	return ""
}
