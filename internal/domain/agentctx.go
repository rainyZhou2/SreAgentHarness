package domain

import (
	"encoding/json"
	"strings"
	"time"
)

// TriggerClaim is the deduplication claim of an inbound trigger key.
type TriggerClaim struct {
	ID        string
	Fresh     bool // this call created the claim
	RunID     string
	Hits      int
	ExpiresAt time.Time
}

// GoalContext is run context pinned in the goal artifact at creation time.
// Everything in it is data: it reaches the prompt and tool defaults, never
// policy decisions.
type GoalContext struct {
	// ReceivedAt is when the run (or turn) was created; prompts use it as
	// "now" so retries and offline replays see the same time.
	ReceivedAt time.Time         `json:"received_at"`
	Window     *TimeWindow       `json:"window,omitempty"`
	Services   []string          `json:"services,omitempty"`
	Incident   string            `json:"incident,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	// Source names the creator, e.g. "api", "trigger:alertmanager".
	Source string `json:"source,omitempty"`
	// Mode is an optional routing hint for auto runs: "react" or "flow".
	Mode string `json:"mode,omitempty"`
}

// TimeWindow is the time range a run is about. A zero End means "up to
// ReceivedAt".
type TimeWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitempty"`
}

// Range resolves the window against the context, falling back to
// [ReceivedAt-lookback, ReceivedAt]. ok is false without any time.
func (c *GoalContext) Range(lookback time.Duration) (start, end time.Time, ok bool) {
	if c == nil || c.ReceivedAt.IsZero() {
		if c != nil && c.Window != nil && !c.Window.Start.IsZero() {
			end = c.Window.End
			if end.IsZero() {
				end = c.Window.Start.Add(lookback)
			}
			return c.Window.Start, end, true
		}
		return time.Time{}, time.Time{}, false
	}
	end = c.ReceivedAt
	if c.Window != nil && !c.Window.End.IsZero() {
		end = c.Window.End
	}
	if c.Window != nil && !c.Window.Start.IsZero() {
		return c.Window.Start, end, true
	}
	if lookback <= 0 {
		lookback = time.Hour
	}
	return end.Add(-lookback), end, true
}

// NormalizeServices trims, drops empties and de-duplicates service names.
func NormalizeServices(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// ContextFact is an environment fact supplied by a context provider
// (topology, ownership, on-call, recent changes, ...). Facts are pinned per
// run and rendered as untrusted data.
type ContextFact struct {
	Provider string     `json:"provider"`
	Kind     string     `json:"kind"`
	Subject  string     `json:"subject,omitempty"`
	Summary  string     `json:"summary"`
	At       *time.Time `json:"at,omitempty"`
	Ref      string     `json:"ref,omitempty"`
	Trust    string     `json:"trust,omitempty"`
}

// ObservationArtifact points at a tool result offloaded because of its size.
type ObservationArtifact struct {
	Handle      string `json:"handle"`
	Bytes       int    `json:"bytes"`
	ContentType string `json:"content_type"`
	Hash        string `json:"hash"`
}

// Verdicts of a post-action verification.
const (
	VerifyVerified = "VERIFIED"
	VerifyRefuted  = "REFUTED"
	VerifyError    = "ERROR"
)

// VerificationResult is recorded on the observation of a verify step.
type VerificationResult struct {
	OfTool   string `json:"of_tool"`
	OfStep   int64  `json:"of_step"`
	Expect   string `json:"expect,omitempty"`
	Verdict  string `json:"verdict"`
	Attempts int    `json:"attempts"`
	Detail   string `json:"detail,omitempty"`
	// Compensation names the compensating tool proposed for approval.
	Compensation string `json:"compensation,omitempty"`
}

// Follow-up roles planned by the kernel after an action.
const (
	FollowupVerify     = "verify"
	FollowupCompensate = "compensate"
	// FollowupReview hands a conclusion the independent verifier did not
	// accept back to the agent.
	FollowupReview = "conclusion_review"
	// FollowupBatch marks the kernel step that runs several read-only
	// calls of one model response together.
	FollowupBatch = "batch"
)

// BatchToolName is the kernel-owned tool of a parallel batch step. Its
// arguments are {"calls":[{name,args,tool_version,risk}]} and its output
// {"observations":[...]}, one per call with Part set.
const BatchToolName = "harness.parallel"

// MemorySearchToolName is the builtin that searches long-term memory
// during a run. Its hits are citable as memory:<id>@<version>.
const MemorySearchToolName = "harness.memory_search"

// SandboxExecToolName is the builtin tool that runs commands in the run's
// sandbox. It changes the run's workspace, so it is never run
// speculatively.
const SandboxExecToolName = "harness.sandbox.exec"

// ToolFollowup marks a tool step the kernel planned after an action. The
// expectation and polling bounds are re-read from the registry at execution
// time; the artifact only identifies the action.
type ToolFollowup struct {
	Role        string          `json:"role"`
	OfTool      string          `json:"of_tool"`
	OfStep      int64           `json:"of_step"`
	OfArgs      json.RawMessage `json:"of_args,omitempty"`
	OfOutputRef string          `json:"of_output_ref,omitempty"`
}
