// Package cognitive defines reasoning strategies. A strategy turns a pinned
// context into a Decision artifact; it never touches the database, never
// executes tools and cannot bypass the policy gate (§7.5, §15.3).
package cognitive

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// FlowNode is the child-run view of one orchestration DSL node.
type FlowNode struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"` // agent | tool | subflow
	Role       string         `json:"role,omitempty"`
	Prompt     string         `json:"prompt,omitempty"`
	Tools      []string       `json:"tools,omitempty"`
	ModelAlias string         `json:"model_alias,omitempty"`
	MaxSteps   int            `json:"max_steps,omitempty"`
	Tool       string         `json:"tool,omitempty"`
	Args       map[string]any `json:"args,omitempty"`
	Input      map[string]any `json:"input,omitempty"`
}

type ReasoningInput struct {
	Goal              string
	StepSeq           int64
	ContextSnapshotID string
	// Instructions, when set, replaces the assembler's default role text for
	// this run (flow agent nodes). Trust rules are still appended.
	Instructions string
	// FlowNode is set for child runs created by the orchestration DSL.
	FlowNode     *FlowNode
	Observations []domain.Observation // committed results of earlier steps, in order
	Evidence     []domain.Evidence    // operator evidence pinned in the snapshot
	Tools        []tools.Descriptor   // tool schemas visible to the model
	Skills       []Skill              // tenant skills (operator-installed playbooks)
	// HasToolSteps reports that this run already executed a tool, so the
	// request is past the initial intent analysis.
	HasToolSteps bool
	// MaxSteps is the profile's step budget; 0 means unlimited.
	MaxSteps int
	// CapabilityMap is the optional [E] summary of capabilities related to
	// the services in the goal. Empty when the profile disables it.
	CapabilityMap []string
	// Time is the run context pinned in the goal artifact (received_at,
	// incident window, services). Nil for runs created before it existed.
	Time *domain.GoalContext
	// Facts are environment facts from context providers, pinned per run.
	Facts []domain.ContextFact

	// v1.1 context (§29, §33): all of it is pinned by the snapshot.
	Call            CallScope
	Conversation    []ConversationMessage // earlier turns on the branch
	Memories        []MemoryRef           // pinned retrieve_memory hits
	Summary         string                // compaction summary replacing observations before CompactedBefore
	CompactedBefore int64
	CompactionEpoch int64

	// Ledger is the hypotheses the agent stated in earlier steps of this
	// run, merged by id, rebuilt from the stored decisions.
	Ledger []LedgerEntry `json:"ledger,omitempty"`
	// CalibrationNote is the measured calibration of the model's stated
	// confidence on earlier runs of this profile ("" when unknown).
	CalibrationNote string `json:"calibration_note,omitempty"`

	// Advisories are kernel notices for this step (budget, repeated calls,
	// stalled progress, calls of the last response that did not run).
	Advisories []string `json:"advisories,omitempty"`
	// Repair is the correction for the previous response of this step,
	// which the kernel could not use ("" on a first try).
	Repair string `json:"repair,omitempty"`
	// ForceConclude tells the model it must conclude now.
	ForceConclude bool `json:"force_conclude,omitempty"`
	// Calls are the run's earlier tool calls, for the kernel's loop guard
	// (not rendered).
	Calls []CallRecord `json:"-"`
	// RecalledMemories are memories returned by in-run memory searches;
	// their refs resolve like pinned memories (the content is in the
	// search observation).
	RecalledMemories []MemoryRef `json:"recalled_memories,omitempty"`
	// LoadedTools are tools the agent loaded with harness.load_tools.
	LoadedTools []string `json:"-"`
	// MoreTools are visible tools whose schemas are not sent this step;
	// the model loads them by name.
	MoreTools []ToolHint `json:"more_tools,omitempty"`
	// OnToolCall, when set, is called as each complete tool call arrives in
	// the model's stream (the kernel starts eligible read-only calls
	// early). It must not block.
	OnToolCall func(name string, args json.RawMessage) `json:"-"`
}

// ToolHint names a tool whose schema is not in the request.
type ToolHint struct {
	Name    string `json:"name"`
	Summary string `json:"summary,omitempty"`
}

// CallRecord is one earlier tool call of the run.
type CallRecord struct {
	Step       int64
	Key        string // tool name + canonical arguments
	Outcome    string
	ResultHash string // "" when the call produced no usable result
}

// LedgerEntry is one hypothesis of the run's ledger.
type LedgerEntry struct {
	ID         string  `json:"id"`
	Statement  string  `json:"statement"`
	Status     string  `json:"status"`
	Confidence float64 `json:"confidence,omitempty"`
	Since      int64   `json:"since"`   // step that introduced it
	Updated    int64   `json:"updated"` // step of the last change
}

// TenantKey partitions prompt caches by tenant (§33.3).
func (in ReasoningInput) TenantKey() string { return in.Call.TenantID }

type Strategy interface {
	Name() string
	Decide(ctx context.Context, in ReasoningInput) (domain.Decision, error)
}

// Validate enforces structural completeness before a decision can be committed.
func Validate(d domain.Decision) error {
	switch d.Kind {
	case domain.DecisionFinal:
		if strings.TrimSpace(d.Answer) == "" {
			return fmt.Errorf("final decision without answer")
		}
	case domain.DecisionTool:
		if d.Tool == nil || d.Tool.Name == "" {
			return fmt.Errorf("tool decision without tool call")
		}
		if len(d.Tool.Args) == 0 || !json.Valid(d.Tool.Args) {
			return fmt.Errorf("tool call arguments are not valid JSON")
		}
	default:
		return fmt.Errorf("unknown decision kind %q", d.Kind)
	}
	switch d.Intent {
	case "", domain.IntentQuestion, domain.IntentTask, domain.IntentClarification:
	default:
		return fmt.Errorf("unknown intent %q", d.Intent)
	}
	return nil
}

// Scripted returns decisions produced by a function; used by tests and for
// recorded-replay evaluation.
type Scripted struct {
	mu    sync.Mutex
	Fn    func(in ReasoningInput) (domain.Decision, error)
	calls int
}

func (s *Scripted) Name() string { return "scripted" }
func (s *Scripted) Decide(_ context.Context, in ReasoningInput) (domain.Decision, error) {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Fn(in)
}
func (s *Scripted) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// RuleBased is a deterministic hypothesis-driven demo strategy. It exists so
// the harness runs end-to-end without a model provider; a model-backed
// strategy implements the same interface.
type RuleBased struct{}

func (RuleBased) Name() string { return "rule_based_demo" }

func (RuleBased) Decide(_ context.Context, in ReasoningInput) (domain.Decision, error) {
	if d, ok := explicitTool(in); ok {
		return d, nil
	}
	service := extractService(in.Goal)
	d := domain.Decision{SchemaVersion: 1, ContextSnapshotID: in.ContextSnapshotID}

	var metrics map[string]any
	restarted, restartBlocked := false, false
	for _, o := range in.Observations {
		switch {
		case o.Tool == "demo.metrics" && o.Outcome == "ok":
			_ = json.Unmarshal(o.Content, &metrics)
		case o.Tool == "demo.restart" && o.Outcome == "ok":
			restarted = true
		case o.Tool == "demo.restart":
			restartBlocked = true
		}
	}
	var evidenceNotes []string
	for _, e := range in.Evidence {
		evidenceNotes = append(evidenceNotes, e.Content)
		d.EvidenceRefs = append(d.EvidenceRefs, fmt.Sprintf("kv:%s@%d", e.Key, e.Version))
	}

	if metrics == nil {
		if !hasTool(in.Tools, "demo.metrics") {
			d.Kind = domain.DecisionFinal
			d.ReasonSummary = "No metrics tool is visible to this flow node."
			d.Answer = "No diagnostic tool is available for this flow node."
			return d, nil
		}
		d.Kind = domain.DecisionTool
		d.ReasonSummary = "Collect read-only metrics before forming any conclusion."
		d.Hypotheses = []domain.Hypothesis{
			{ID: "hyp:pool-exhaustion", Statement: "DB connection pool exhaustion", Status: "UNVERIFIED", ConfidenceEstimate: 0.4},
			{ID: "hyp:traffic-spike", Statement: "Traffic spike", Status: "UNVERIFIED", ConfidenceEstimate: 0.3},
		}
		args, _ := json.Marshal(map[string]string{"service": service})
		d.Tool = &domain.ToolCall{Name: "demo.metrics", Args: args}
		return d, nil
	}

	poolSaturated := num(metrics["db_pool_active"]) >= num(metrics["db_pool_max"]) && num(metrics["db_pool_wait_ms"]) > 500
	lowCPU := num(metrics["cpu_utilization"]) < 0.7
	status := "REFUTED"
	if poolSaturated {
		status = "VERIFIED"
	}
	d.Hypotheses = []domain.Hypothesis{
		{ID: "hyp:pool-exhaustion", Statement: "DB connection pool exhaustion", Status: status,
			ConfidenceEstimate: map[bool]float64{true: 0.8, false: 0.1}[poolSaturated], Supports: []string{"obs:demo.metrics"}},
		{ID: "hyp:traffic-spike", Statement: "Traffic spike (CPU bound)", Status: map[bool]string{true: "REFUTED", false: "UNVERIFIED"}[lowCPU],
			ConfidenceEstimate: 0.1, Refutes: []string{"obs:demo.metrics"}},
	}

	wantsRemediation := strings.Contains(strings.ToLower(in.Goal), "remediate")
	if poolSaturated && wantsRemediation && !restarted && !restartBlocked && hasTool(in.Tools, "demo.restart") {
		d.Kind = domain.DecisionTool
		d.ReasonSummary = "Pool exhaustion verified; a rolling restart releases leaked connections. Requires approval."
		args, _ := json.Marshal(map[string]string{"service": service})
		d.Tool = &domain.ToolCall{Name: "demo.restart", Args: args}
		return d, nil
	}

	d.Kind = domain.DecisionFinal
	d.ReasonSummary = "Conclude from verified evidence."
	var b strings.Builder
	fmt.Fprintf(&b, "Service %s. ", service)
	if poolSaturated {
		b.WriteString("Probable cause: DB connection pool exhaustion (pool at max, high wait time). ")
	} else {
		b.WriteString("No confirmed cause from available metrics. ")
	}
	if lowCPU {
		b.WriteString("Traffic spike refuted (CPU low). ")
	}
	switch {
	case restarted:
		b.WriteString("Action taken: simulated rolling restart (approved). ")
	case restartBlocked:
		b.WriteString("Remediation not executed (denied, rejected or approval expired). ")
	}
	if len(evidenceNotes) > 0 {
		fmt.Fprintf(&b, "Operator evidence considered: %s.", strings.Join(evidenceNotes, "; "))
	}
	d.Answer = strings.TrimSpace(b.String())
	return d, nil
}

func hasTool(ds []tools.Descriptor, name string) bool {
	for _, d := range ds {
		if d.Name == name {
			return true
		}
	}
	return false
}

func extractService(goal string) string {
	for _, f := range strings.Fields(goal) {
		if v, ok := strings.CutPrefix(f, "service="); ok {
			return strings.Trim(v, ",.;")
		}
	}
	return "unknown-service"
}

func num(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

// explicitTool lets the keyless demo model use any visible tool the user
// names explicitly ("use mcp.docs.search {\"query\": \"db pool\"}"): it
// calls the tool once with the given JSON arguments, then reports the result.
// A real model decides on its own; this keeps added tools testable offline.
func explicitTool(in ReasoningInput) (domain.Decision, bool) {
	d := domain.Decision{SchemaVersion: 1, ContextSnapshotID: in.ContextSnapshotID}
	lower := strings.ToLower(in.Goal)
	var name, rest string
	for _, t := range in.Tools {
		i := strings.Index(lower, "use "+strings.ToLower(t.Name))
		if i < 0 {
			continue
		}
		after := in.Goal[i+4+len(t.Name):]
		if after != "" && !strings.ContainsAny(after[:1], " \t\n{") {
			continue // a longer tool name that merely shares this prefix
		}
		if len(t.Name) > len(name) {
			name, rest = t.Name, after
		}
	}
	if name == "" {
		return d, false
	}
	for _, o := range in.Observations {
		if o.Tool != name {
			continue
		}
		d.Kind = domain.DecisionFinal
		d.ReasonSummary = "Report the result of the requested tool call."
		if o.Outcome != "ok" {
			d.Answer = fmt.Sprintf("I called %s as requested, but it did not run (%s). %s", name, o.Outcome, o.Note)
			return d, true
		}
		body := string(o.Content)
		var wrapped struct {
			Content any `json:"content"`
		}
		if json.Unmarshal(o.Content, &wrapped) == nil && wrapped.Content != nil {
			if s, ok := wrapped.Content.(string); ok {
				body = s
			} else {
				var b strings.Builder
				enc := json.NewEncoder(&b)
				enc.SetEscapeHTML(false) // show "->" and "<", not \u003e
				enc.SetIndent("", "  ")
				if enc.Encode(wrapped.Content) == nil {
					body = b.String()
				}
			}
		}
		if len(body) > 4000 {
			body = strings.ToValidUTF8(body[:4000], "") + "\n…[truncated]"
		}
		d.Answer = fmt.Sprintf("I called %s as requested. Result:\n\n```\n%s\n```", name, strings.TrimSpace(body))
		return d, true
	}
	args := json.RawMessage(`{}`)
	if trimmed := strings.TrimSpace(rest); strings.HasPrefix(trimmed, "{") {
		var v map[string]any
		if err := json.NewDecoder(strings.NewReader(trimmed)).Decode(&v); err == nil {
			args, _ = json.Marshal(v)
		}
	}
	d.Kind = domain.DecisionTool
	d.ReasonSummary = fmt.Sprintf("The request names %s explicitly; calling it.", name)
	d.Tool = &domain.ToolCall{Name: name, Args: args}
	return d, true
}
