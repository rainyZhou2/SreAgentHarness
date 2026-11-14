// Package workbench runs Work benchmark cases and replays recorded Works
// deterministically (RFC §14.4, §20). A Case scripts everything outside the
// control plane (signals, world facts, metric series, run results, action
// outcomes, approvals and human confirmations) on a simulated clock; the
// real orchestrator, action runtime, outcome evaluator and correlation
// router run against a fresh SQLite control-plane store. The result is
// scored with workeval, so a candidate agent/profile/template can be gated
// on the same hard gates as production Works.
package workbench

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/work"
)

// Case is one benchmark scenario. Offsets ("at") are relative to Start.
type Case struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Tenant      string `json:"tenant,omitempty"` // default "bench"
	// Start is the simulated wall clock at offset 0 (default
	// 2026-01-01T00:00:00Z); a fixed start keeps runs reproducible.
	Start time.Time `json:"start,omitempty"`
	// Environment is the policy environment (default "staging"). In
	// "production" R1 actions need approval too.
	Environment string `json:"environment,omitempty"`

	Templates []work.Template `json:"templates,omitempty"`
	Rules     []eventing.Rule `json:"rules,omitempty"`
	// Work creates the Work directly; Signals may create (through Rules)
	// and attach to Works. At least one of them is required.
	Work    *WorkSpec `json:"work,omitempty"`
	Signals []Signal  `json:"signals,omitempty"`

	Facts    []Fact        `json:"facts,omitempty"`
	Metrics  []Series      `json:"metrics,omitempty"`
	Evidence []Evidence    `json:"evidence,omitempty"`
	Runs     []ScriptedRun `json:"runs,omitempty"`
	Actions  []SimAction   `json:"actions,omitempty"`
	// Approvals decide actions waiting for approval, matched by action
	// name in order. Unmatched approvals leave the Work waiting.
	Approvals []Approval  `json:"approvals,omitempty"`
	Human     []HumanStep `json:"human,omitempty"`

	// Horizon bounds simulated time (default 4h); Tick is the simulation
	// step (default 10s).
	Horizon cpkit.Duration `json:"horizon,omitempty"`
	Tick    cpkit.Duration `json:"tick,omitempty"`

	Expect Expect `json:"expect"`
}

// WorkSpec creates a Work at an offset.
type WorkSpec struct {
	At       cpkit.Duration `json:"at,omitempty"`
	Kind     work.Kind      `json:"kind"`
	Title    string         `json:"title,omitempty"`
	Template string         `json:"template,omitempty"`
	Goal     work.GoalSpec  `json:"goal"`
}

// Signal is an event ingested at an offset (tenant and times are filled).
type Signal struct {
	At    cpkit.Duration    `json:"at,omitempty"`
	Event eventing.Envelope `json:"event"`
}

// Fact is a world fact asserted at an offset.
type Fact struct {
	At        cpkit.Duration `json:"at,omitempty"`
	Subject   string         `json:"subject"`
	Predicate string         `json:"predicate"`
	Value     string         `json:"value"`
	Trust     string         `json:"trust,omitempty"` // default T2
	// Source names who reports the fact (default "bench"). Two sources
	// reporting different values for one predicate is a conflict.
	Source string `json:"source,omitempty"`
}

// Series is a scripted metric: a step function through its points.
type Series struct {
	Source string  `json:"source"`
	Query  string  `json:"query"`
	Points []Point `json:"points"`
}

// Point is one metric value from an offset on.
type Point struct {
	At cpkit.Duration `json:"at"`
	V  float64        `json:"v"`
}

// Evidence is operator/telemetry evidence added to the primary Work.
type Evidence struct {
	At                cpkit.Duration `json:"at,omitempty"`
	SourceType        string         `json:"source_type"`
	SourceRef         string         `json:"source_ref"`
	TrustTier         string         `json:"trust_tier"`
	Summary           string         `json:"summary,omitempty"`
	Quality           string         `json:"quality,omitempty"`
	Relation          string         `json:"relation,omitempty"`
	Kind              string         `json:"kind,omitempty"`
	IndependenceGroup string         `json:"independence_group,omitempty"`
	Supersedes        string         `json:"supersedes,omitempty"`
	ContentHash       string         `json:"content_hash,omitempty"`
}

// ScriptedRun answers the runs a stage starts. Runs for the same stage
// (and role) are consumed in order; the last one repeats (replans).
type ScriptedRun struct {
	Stage string `json:"stage"`
	Role  string `json:"role,omitempty"`
	// Duration is how long the run takes (default 1m).
	Duration cpkit.Duration `json:"duration,omitempty"`
	// Status: succeeded (default) | failed.
	Status        string `json:"status,omitempty"`
	FailureReason string `json:"failure_reason,omitempty"`
	// Result is the kernel result document: answer, hypotheses,
	// proposed_action, loop.cost_usd, uncertainties.
	Result    json.RawMessage `json:"result,omitempty"`
	ToolCalls int             `json:"tool_calls,omitempty"`
}

// SimAction is a simulated action executor.
type SimAction struct {
	Name          string   `json:"name"`
	Risk          string   `json:"risk"` // R0..R2 (R3 is refused by the runtime)
	ResourceKinds []string `json:"resource_kinds,omitempty"`
	// Outcome of Execute: applied (default) | not_applied | unknown.
	Outcome string `json:"outcome,omitempty"`
	// Inspect result: applied | not_applied | unknown (default follows
	// Outcome).
	Inspect string `json:"inspect,omitempty"`
	// RequiredFacts must be present, fresh and undisputed for the target
	// before the action is planned or dispatched.
	RequiredFacts []string `json:"required_facts,omitempty"`
	// Effects applied when the action takes effect.
	Facts   []FactEffect   `json:"facts,omitempty"`
	Metrics []MetricEffect `json:"metrics,omitempty"`
}

// FactEffect asserts a world fact (subject defaults to the target).
type FactEffect struct {
	Subject   string `json:"subject,omitempty"`
	Predicate string `json:"predicate"`
	Value     string `json:"value"`
}

// MetricEffect sets a metric series to V from the execution on.
type MetricEffect struct {
	Source string  `json:"source"`
	Query  string  `json:"query"`
	V      float64 `json:"v"`
}

// Approval decides one waiting action.
type Approval struct {
	Action   string `json:"action"`
	Decision string `json:"decision,omitempty"` // approve (default) | reject
	Approver string `json:"approver,omitempty"` // default "bench-approver"
	Comment  string `json:"comment,omitempty"`
}

// HumanStep sends a Work command, either at an offset or once the primary
// Work waits at Stage.
type HumanStep struct {
	At      *cpkit.Duration `json:"at,omitempty"`
	Stage   string          `json:"stage,omitempty"`
	Command string          `json:"command"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Actor   string          `json:"actor,omitempty"` // default "bench-human"
}

// Expect are the case assertions (zero values are not checked).
type Expect struct {
	Status work.Status `json:"status,omitempty"`
	// Pass asserts the scorecard verdict (no failed hard gate).
	Pass *bool `json:"pass,omitempty"`
	// Gates must all be among the failed gates.
	Gates      []string `json:"gates,omitempty"`
	MaxRuns    int      `json:"max_runs,omitempty"`
	MaxActions int      `json:"max_actions,omitempty"`
	// Applied actions (by name) must have taken effect; Forbidden must
	// never have been proposed.
	Applied   []string `json:"applied,omitempty"`
	Forbidden []string `json:"forbidden,omitempty"`
	// Works is the number of distinct Works the signals touched.
	Works int `json:"works,omitempty"`
	// RootCause is a substring of the report's root cause; "unknown"
	// asserts there is none.
	RootCause   string         `json:"root_cause,omitempty"`
	MaxDuration cpkit.Duration `json:"max_duration,omitempty"`
}

// Defaults.
const (
	DefaultTenant  = "bench"
	DefaultHorizon = 4 * time.Hour
	DefaultTick    = 10 * time.Second
	MaxHorizon     = 31 * 24 * time.Hour
	maxTicks       = 20000
)

// DefaultStart is the simulated clock origin.
var DefaultStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// ErrInvalidCase wraps validation failures.
var ErrInvalidCase = errors.New("invalid bench case")

func invalid(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCase, fmt.Sprintf(format, a...))
}

// Normalize validates the case and fills defaults.
func (c *Case) Normalize() error {
	if strings.TrimSpace(c.Name) == "" {
		return invalid("name is required")
	}
	if c.Tenant == "" {
		c.Tenant = DefaultTenant
	}
	if c.Start.IsZero() {
		c.Start = DefaultStart
	}
	c.Start = c.Start.UTC()
	if c.Environment == "" {
		c.Environment = "staging"
	}
	if c.Horizon <= 0 {
		c.Horizon = cpkit.Duration(DefaultHorizon)
	}
	if c.Tick <= 0 {
		c.Tick = cpkit.Duration(DefaultTick)
	}
	if c.Horizon.D() > MaxHorizon || c.Tick.D() < time.Second || c.Horizon.D()/c.Tick.D() > maxTicks {
		return invalid("%s: horizon/tick out of range (max %s, tick >= 1s, at most %d ticks)", c.Name, MaxHorizon, maxTicks)
	}
	if c.Work == nil && len(c.Signals) == 0 {
		return invalid("%s: work or signals are required", c.Name)
	}
	seen := map[string]bool{}
	for i, a := range c.Actions {
		if a.Name == "" || seen[a.Name] {
			return invalid("%s: action %d needs a unique name", c.Name, i)
		}
		seen[a.Name] = true
		switch domain.RiskClass(a.Risk) {
		case domain.RiskR0, domain.RiskR1, domain.RiskR2:
		default:
			return invalid("%s: action %s risk must be R0, R1 or R2", c.Name, a.Name)
		}
		for _, v := range []string{a.Outcome, a.Inspect} {
			switch v {
			case "", "applied", "not_applied", "unknown":
			default:
				return invalid("%s: action %s: outcome/inspect %q", c.Name, a.Name, v)
			}
		}
	}
	for i, r := range c.Runs {
		if r.Stage == "" {
			return invalid("%s: run %d needs a stage", c.Name, i)
		}
		switch r.Status {
		case "", "succeeded", "failed":
		default:
			return invalid("%s: run %d status %q", c.Name, i, r.Status)
		}
		if len(r.Result) > 0 && !json.Valid(r.Result) {
			return invalid("%s: run %d result is not JSON", c.Name, i)
		}
	}
	for i, h := range c.Human {
		if h.Command == "" || (h.At == nil && h.Stage == "") {
			return invalid("%s: human step %d needs a command and at or stage", c.Name, i)
		}
	}
	for i, a := range c.Approvals {
		switch a.Decision {
		case "", "approve", "reject":
		default:
			return invalid("%s: approval %d decision %q", c.Name, i, a.Decision)
		}
	}
	for _, s := range c.Metrics {
		sort.SliceStable(s.Points, func(i, j int) bool { return s.Points[i].At < s.Points[j].At })
	}
	return nil
}

// LoadCases reads one case file, or every *.json file of a directory
// (sorted), each holding one case or an array of cases.
func LoadCases(path string) ([]Case, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	files := []string{path}
	if st.IsDir() {
		files, err = filepath.Glob(filepath.Join(path, "*.json"))
		if err != nil {
			return nil, err
		}
		sort.Strings(files)
	}
	var out []Case
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		cs, err := ParseCases(b)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", f, err)
		}
		out = append(out, cs...)
	}
	return out, nil
}

// ParseCases decodes one case or an array of cases (unknown fields are
// rejected so typos do not silently weaken a case).
func ParseCases(b []byte) ([]Case, error) {
	trimmed := strings.TrimSpace(string(b))
	dec := json.NewDecoder(strings.NewReader(trimmed))
	dec.DisallowUnknownFields()
	var cs []Case
	if strings.HasPrefix(trimmed, "[") {
		if err := dec.Decode(&cs); err != nil {
			return nil, err
		}
	} else {
		var c Case
		if err := dec.Decode(&c); err != nil {
			return nil, err
		}
		cs = []Case{c}
	}
	for i := range cs {
		if err := cs[i].Normalize(); err != nil {
			return nil, err
		}
	}
	return cs, nil
}
