package work

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
)

// StageType is what a stage does.
type StageType string

const (
	// StageRun starts one owner run through the existing kernel and waits
	// for it to finish.
	StageRun StageType = "run"
	// StageAgents starts several read-only investigator runs in parallel
	// (multi-agent) sharing the Work budget; required agents must succeed.
	StageAgents StageType = "agents"
	// StageWait waits on a durable watch (event, time or periodic).
	StageWait StageType = "wait"
	// StageAction proposes, preflights, gets approval for and executes one
	// controlled action, then verifies its effect.
	StageAction StageType = "action"
	// StageVerify evaluates the outcome spec until SATISFIED (stable),
	// NOT_SATISFIED (replan) or the timeout.
	StageVerify StageType = "verify"
	// StageConfirm waits for a human confirmation.
	StageConfirm StageType = "confirm"
	// StageChildren starts child Works from the template graph and waits
	// until the required ones ended (template_version 2).
	StageChildren StageType = "children"
)

// Failure policies.
const (
	OnFailBlock = "block" // BLOCKED, a human resolves
	OnFailFail  = "fail"  // FAILED
	OnFailSkip  = "skip"  // continue with the next stage (optional stages)
)

// RunSpec configures a run stage (or one agent).
type RunSpec struct {
	// Goal is a template: ${goal.intent}, ${goal.description}, ${subject},
	// ${subjects}, ${title}, ${work.id}, ${generation}, ${label.<k>} and
	// ${stage.<id>.summary} (bounded summary of an earlier run stage).
	Goal           string `json:"goal"`
	ProfileID      string `json:"profile_id,omitempty"`
	ProfileVersion int64  `json:"profile_version,omitempty"`
	Mode           string `json:"mode,omitempty"`
	// Budget bounds this assignment (optional). The run profile bounds what
	// the kernel spends; the duration is enforced by the Work: a run past
	// it is cancelled and reported as a timeout, and the other agents'
	// results stand.
	Budget *AgentBudget `json:"budget,omitempty"`
}

// AgentBudget bounds one agent assignment.
type AgentBudget struct {
	MaxModelCalls int            `json:"max_model_calls,omitempty"`
	MaxToolCalls  int            `json:"max_tool_calls,omitempty"`
	MaxCostUSD    float64        `json:"max_cost_usd,omitempty"`
	MaxDuration   cpkit.Duration `json:"max_duration,omitempty"`
}

// AgentSpec is one investigator of an agents stage.
type AgentSpec struct {
	Role string `json:"role"`
	RunSpec
	Required bool `json:"required,omitempty"`
}

// ActionSpec configures an action stage. The action is built by the server
// from this spec (and optionally from a proposal the referenced run stage
// made, filtered through Allowed); a model never submits an action directly.
type ActionSpec struct {
	Name string `json:"name"`
	// Target is a resource ref template (default ${subject}).
	Target string          `json:"target,omitempty"`
	Args   json.RawMessage `json:"args,omitempty"`
	// FromProposal names a run stage whose result may propose
	// {"proposed_action":{"name","target","args"}}; the proposal is only
	// used when its name is in Allowed and its target is a Work subject.
	FromProposal string   `json:"from_proposal,omitempty"`
	Allowed      []string `json:"allowed,omitempty"`
	// Effect verifies that the action took effect (separate from the Work
	// outcome).
	Effect *outcome.Spec `json:"effect,omitempty"`
	// EffectTimeout bounds effect verification (default 10m).
	EffectTimeout cpkit.Duration `json:"effect_timeout,omitempty"`
	// ApprovalTimeout bounds the wait for an approval (default 30m).
	ApprovalTimeout cpkit.Duration `json:"approval_timeout,omitempty"`
}

// VerifySpec configures a verify stage.
type VerifySpec struct {
	// Spec overrides the goal's success spec for this stage.
	Spec         *outcome.Spec  `json:"spec,omitempty"`
	RecheckEvery cpkit.Duration `json:"recheck_every,omitempty"` // default 1m
	Timeout      cpkit.Duration `json:"timeout,omitempty"`       // default 30m
}

// ConfirmSpec configures a human confirmation.
type ConfirmSpec struct {
	Prompt  string         `json:"prompt,omitempty"`
	Timeout cpkit.Duration `json:"timeout,omitempty"` // 0 = wait indefinitely
}

// Stage is one step of a Work template.
type Stage struct {
	ID      string           `json:"id"`
	Type    StageType        `json:"type"`
	Phase   string           `json:"phase,omitempty"` // detect|assess|investigate|mitigate|verify|review|general
	Run     *RunSpec         `json:"run,omitempty"`
	Agents  []AgentSpec      `json:"agents,omitempty"`
	Wait    *continuous.Spec `json:"wait,omitempty"`
	Action  *ActionSpec      `json:"action,omitempty"`
	Verify  *VerifySpec      `json:"verify,omitempty"`
	Confirm *ConfirmSpec     `json:"confirm,omitempty"`
	// Children configures a children stage.
	Children *ChildrenSpec `json:"children,omitempty"`
	// OnFailure: block (default) | fail | skip | <stage id> to replan from
	// an earlier stage (bounded by Limits.MaxReplans).
	OnFailure string `json:"on_failure,omitempty"`
	// OnTimeout applies to wait/verify/confirm stages: continue | block | fail
	// | <stage id>. Default block.
	OnTimeout string `json:"on_timeout,omitempty"`
	// EvidencePlan tunes evidence planning for run and agents stages
	// (RFC §12.4; on by default once the Work has hypotheses).
	EvidencePlan *EvidencePlanSpec `json:"evidence_plan,omitempty"`
}

// EvidencePlanSpec tunes the read-only checks planned for a stage's runs.
type EvidencePlanSpec struct {
	Disabled bool `json:"disabled,omitempty"`
	// MaxChecks is the number of checks planned per run (default 3, max 8).
	MaxChecks int `json:"max_checks,omitempty"`
}

// Limits bound a Work (RFC §20.3). Zero values take the defaults.
type Limits struct {
	MaxRuns     int            `json:"max_runs,omitempty"`
	MaxReplans  int            `json:"max_replans,omitempty"`
	MaxDuration cpkit.Duration `json:"max_duration,omitempty"`
	MaxAgents   int            `json:"max_agents,omitempty"`
	MaxActions  int            `json:"max_actions,omitempty"`
	// MaxCostUSD caps the model spend of all the Work's runs together; no
	// new run starts once it is reached (0 = unlimited; per-run spend is
	// bounded by the run profile).
	MaxCostUSD float64 `json:"max_cost_usd,omitempty"`
	// MaxWatches caps the Work's active watches.
	MaxWatches int `json:"max_watches,omitempty"`
	// MaxEvidence and MaxEvidenceBytes cap the evidence records a Work
	// keeps and their stored size.
	MaxEvidence      int `json:"max_evidence,omitempty"`
	MaxEvidenceBytes int `json:"max_evidence_bytes,omitempty"`
	// MaxEventRate caps correlated events per minute that reach the Work's
	// timeline and wake it; the rest stay linked in the inbox only.
	MaxEventRate int `json:"max_event_rate,omitempty"`
}

// Watches, Evidence, EvidenceBytes and EventRate return the limit or its
// default, so Works pinned before a limit existed are bounded too.
func (l Limits) Watches() int       { return defInt(l.MaxWatches, DefaultMaxWatches) }
func (l Limits) Evidence() int      { return defInt(l.MaxEvidence, DefaultMaxEvidence) }
func (l Limits) EvidenceBytes() int { return defInt(l.MaxEvidenceBytes, DefaultMaxEvidenceBytes) }
func (l Limits) EventRate() int     { return defInt(l.MaxEventRate, DefaultMaxEventRate) }

// Defaults.
const (
	DefaultMaxRuns     = 30
	DefaultMaxReplans  = 4
	DefaultMaxDuration = 48 * time.Hour
	DefaultMaxAgents   = 4
	DefaultMaxActions  = 5
	// DefaultMaxWatches .. DefaultMaxEventRate apply when a template sets
	// none (RFC §20.3); operators tune them per template.
	DefaultMaxWatches       = 8
	DefaultMaxEvidence      = 500
	DefaultMaxEvidenceBytes = 4 << 20
	DefaultMaxEventRate     = 120
	MaxStages               = 32
)

// Template is a versioned lifecycle definition. It is pinned (copied) into
// each Work at creation, so a changed template never alters running Works.
type Template struct {
	ID      string  `json:"id"`
	Version int     `json:"version"`
	Kind    Kind    `json:"kind,omitempty"`
	Stages  []Stage `json:"stages"`
	Limits  Limits  `json:"limits"`
	// Queue is the default scheduling queue of Works created from this
	// template (empty: by kind).
	Queue Queue `json:"queue,omitempty"`
	// Graph declares child Works and the limits of the Work tree
	// (optional; templates without it behave as before).
	Graph *GraphSpec `json:"graph,omitempty"`
	// Runbook is operator metadata about the template as a runbook
	// (owner, validity, rehearsal); it never changes how stages run.
	Runbook *RunbookRef `json:"runbook,omitempty"`
}

// RunbookRef points a template at its runbook registry entry.
type RunbookRef struct {
	ID      string `json:"id"`
	Version int    `json:"version,omitempty"`
}

var stageIDRe = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,47}$`)

// Normalize validates the template and fills defaults.
func (t *Template) Normalize() error {
	if !stageIDRe.MatchString(t.ID) {
		return invalidf("template id %q is invalid", t.ID)
	}
	if t.Version <= 0 {
		t.Version = 1
	}
	if t.Queue != "" && !ValidQueue(t.Queue) {
		return invalidf("template %s: unknown queue %q", t.ID, t.Queue)
	}
	if len(t.Stages) == 0 || len(t.Stages) > MaxStages {
		return invalidf("template %s needs 1..%d stages", t.ID, MaxStages)
	}
	l := &t.Limits
	l.MaxRuns = defInt(l.MaxRuns, DefaultMaxRuns)
	l.MaxReplans = defInt(l.MaxReplans, DefaultMaxReplans)
	l.MaxAgents = defInt(l.MaxAgents, DefaultMaxAgents)
	l.MaxActions = defInt(l.MaxActions, DefaultMaxActions)
	if l.MaxDuration <= 0 {
		l.MaxDuration = cpkit.Duration(DefaultMaxDuration)
	}
	l.MaxWatches = l.Watches()
	l.MaxEvidence = l.Evidence()
	l.MaxEvidenceBytes = l.EvidenceBytes()
	l.MaxEventRate = l.EventRate()
	if l.MaxRuns > 500 || l.MaxReplans > 50 || l.MaxAgents > 16 || l.MaxActions > 50 || l.MaxDuration.D() > 90*24*time.Hour ||
		l.MaxWatches > 64 || l.MaxEvidence > 10000 || l.MaxEvidenceBytes > 64<<20 || l.MaxEventRate > 10000 ||
		l.MaxCostUSD > 10000 {
		return invalidf("template %s limits are too large", t.ID)
	}
	if l.MaxCostUSD < 0 {
		return invalidf("template %s: max_cost_usd must not be negative", t.ID)
	}
	if t.Graph != nil {
		if err := t.Graph.normalize(); err != nil {
			return invalidf("template %s graph: %v", t.ID, err)
		}
	}
	childKeys := map[string]bool{}
	if t.Graph != nil {
		for _, c := range t.Graph.Children {
			childKeys[c.Key] = true
		}
	}
	index := map[string]int{}
	for i := range t.Stages {
		s := &t.Stages[i]
		if s.Type == StageChildren {
			if len(childKeys) == 0 {
				return invalidf("stage %s: a children stage needs graph.children", s.ID)
			}
			if s.Children != nil {
				for _, k := range s.Children.Keys {
					if !childKeys[k] {
						return invalidf("stage %s: unknown child %q", s.ID, k)
					}
				}
			}
		}
		if !stageIDRe.MatchString(s.ID) {
			return invalidf("stage id %q is invalid", s.ID)
		}
		if _, dup := index[s.ID]; dup {
			return invalidf("stage id %q is duplicated", s.ID)
		}
		index[s.ID] = i
		if err := s.normalize(l); err != nil {
			return invalidf("stage %s: %v", s.ID, err)
		}
	}
	for i, s := range t.Stages {
		for _, ref := range []string{s.OnFailure, s.OnTimeout} {
			switch ref {
			case "", OnFailBlock, OnFailFail, OnFailSkip, "continue":
				continue
			}
			j, ok := index[ref]
			if !ok {
				return invalidf("stage %s: unknown stage %q", s.ID, ref)
			}
			if j > i {
				return invalidf("stage %s: can only replan from an earlier stage (%s)", s.ID, ref)
			}
		}
		if s.Type == StageAction && s.Action.FromProposal != "" {
			if j, ok := index[s.Action.FromProposal]; !ok || j >= i || (t.Stages[j].Type != StageRun && t.Stages[j].Type != StageAgents) {
				return invalidf("stage %s: from_proposal must name an earlier run or agents stage", s.ID)
			}
		}
		if s.Action != nil && s.Action.FromProposal != "" {
			j, ok := index[s.Action.FromProposal]
			if !ok || j >= i || t.Stages[j].Type != StageRun {
				return invalidf("stage %s: from_proposal must name an earlier run stage", s.ID)
			}
		}
	}
	return nil
}

func defInt(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
}

func (s *Stage) normalize(l *Limits) error {
	n := 0
	for _, set := range []bool{s.Run != nil, len(s.Agents) > 0, s.Wait != nil, s.Action != nil, s.Verify != nil, s.Confirm != nil,
		s.Children != nil} {
		if set {
			n++
		}
	}
	if p := s.EvidencePlan; p != nil {
		if s.Type != StageRun && s.Type != StageAgents {
			return fmt.Errorf("evidence_plan only applies to run and agents stages")
		}
		if p.MaxChecks < 0 || p.MaxChecks > 8 {
			return fmt.Errorf("evidence_plan.max_checks must be within [0,8]")
		}
	}
	need := map[StageType]bool{StageRun: s.Run != nil, StageAgents: len(s.Agents) > 0, StageWait: s.Wait != nil,
		StageAction: s.Action != nil, StageVerify: true, StageConfirm: true, StageChildren: true}
	ok, known := need[s.Type]
	if !known {
		return fmt.Errorf("unknown type %q", s.Type)
	}
	if !ok || n > 1 {
		return fmt.Errorf("a %s stage takes exactly its own configuration block", s.Type)
	}
	if s.Phase == "" {
		s.Phase = "general"
	}
	switch s.Type {
	case StageRun:
		if strings.TrimSpace(s.Run.Goal) == "" {
			return fmt.Errorf("run.goal is required")
		}
	case StageAgents:
		if len(s.Agents) > l.MaxAgents {
			return fmt.Errorf("%d agents exceed max_agents %d", len(s.Agents), l.MaxAgents)
		}
		roles := map[string]bool{}
		for _, a := range s.Agents {
			if !stageIDRe.MatchString(a.Role) || roles[a.Role] {
				return fmt.Errorf("agent role %q is invalid or duplicated", a.Role)
			}
			roles[a.Role] = true
			if strings.TrimSpace(a.Goal) == "" {
				return fmt.Errorf("agent %s needs a goal", a.Role)
			}
			if b := a.Budget; b != nil && (b.MaxModelCalls < 0 || b.MaxToolCalls < 0 || b.MaxCostUSD < 0 || b.MaxDuration < 0 ||
				b.MaxDuration.D() > 24*time.Hour) {
				return fmt.Errorf("agent %s: budget is out of range", a.Role)
			}
		}
	case StageWait:
		if s.Wait.Mode == continuous.OnApproval {
			return fmt.Errorf("approval waits are created by action stages")
		}
		// Validate with a placeholder subject; the real one is bound at run time.
		if _, err := continuous.Build(*s.Wait, "t", "w", 1, []string{"x://validate/only"}, time.Unix(0, 0)); err != nil {
			return err
		}
	case StageAction:
		a := s.Action
		switch {
		case a.Name == "" && a.FromProposal == "":
			return fmt.Errorf("action.name or action.from_proposal is required")
		case a.FromProposal != "" && len(a.Allowed) == 0:
			// Proposals are model output: only allow-listed actions are taken.
			return fmt.Errorf("action.allowed is required with from_proposal")
		case a.Name != "" && len(a.Allowed) > 0 && !slices.Contains(a.Allowed, a.Name):
			return fmt.Errorf("action.name is not in action.allowed")
		}
		if a.Effect != nil {
			if err := a.Effect.Normalize(); err != nil {
				return err
			}
		}
		if len(a.Args) > 0 && !json.Valid(a.Args) {
			return fmt.Errorf("action.args must be JSON")
		}
	case StageVerify:
		if s.Verify == nil {
			s.Verify = &VerifySpec{}
		}
		if s.Verify.Spec != nil {
			if err := s.Verify.Spec.Normalize(); err != nil {
				return err
			}
		}
		if s.Verify.RecheckEvery == 0 {
			s.Verify.RecheckEvery = cpkit.Duration(time.Minute)
		}
		if s.Verify.Timeout == 0 {
			s.Verify.Timeout = cpkit.Duration(30 * time.Minute)
		}
		if s.Verify.RecheckEvery.D() < continuous.MinInterval {
			return fmt.Errorf("recheck_every must be at least %s", continuous.MinInterval)
		}
	case StageConfirm:
		if s.Confirm == nil {
			s.Confirm = &ConfirmSpec{}
		}
	}
	return nil
}

// Index returns the position of a stage id.
func (t *Template) Index(id string) (int, bool) {
	for i, s := range t.Stages {
		if s.ID == id {
			return i, true
		}
	}
	return 0, false
}

// Render substitutes ${...} placeholders in a template string from the Work.
// Values are inserted verbatim; nothing is evaluated. summaries maps a run
// stage id to its bounded result summary.
func Render(tpl string, w *Work, summaries map[string]string) string {
	return RenderWith(tpl, w, summaries, nil)
}

// UntrustedField reports whether a placeholder expands to text that may
// come from outside the operator (event titles and labels, descriptions,
// earlier runs' summaries).
func UntrustedField(name string) bool {
	switch {
	case name == "title", name == "goal.description", strings.HasPrefix(name, "label."), strings.HasPrefix(name, "stage."):
		return true
	}
	return false
}

// RenderWith is Render with a hook applied to every substituted value
// (e.g. to fence untrusted text before it reaches a model).
func RenderWith(tpl string, w *Work, summaries map[string]string, quote func(name, value string) string) string {
	var b strings.Builder
	for {
		i := strings.Index(tpl, "${")
		if i < 0 {
			b.WriteString(tpl)
			break
		}
		j := strings.Index(tpl[i:], "}")
		if j < 0 {
			b.WriteString(tpl)
			break
		}
		b.WriteString(tpl[:i])
		name := tpl[i+2 : i+j]
		v := lookup(name, w, summaries)
		if quote != nil {
			v = quote(name, v)
		}
		b.WriteString(v)
		tpl = tpl[i+j+1:]
	}
	return b.String()
}

func lookup(name string, w *Work, summaries map[string]string) string {
	switch name {
	case "goal.intent":
		return w.Goal.Intent
	case "goal.description":
		return w.Goal.Description
	case "subject":
		return w.Goal.PrimarySubject()
	case "subjects":
		return strings.Join(w.Goal.SubjectRefs, ", ")
	case "title":
		return w.Title
	case "work.id":
		return w.ID
	case "generation":
		return strconv.Itoa(w.Generation)
	}
	if k, ok := strings.CutPrefix(name, "label."); ok {
		return w.Goal.Labels[k]
	}
	if rest, ok := strings.CutPrefix(name, "stage."); ok {
		if id, ok := strings.CutSuffix(rest, ".summary"); ok {
			return summaries[id]
		}
	}
	return ""
}

// RenderJSON substitutes placeholders inside the string values of a JSON
// document (keys and structure are untouched).
func RenderJSON(doc json.RawMessage, w *Work) (json.RawMessage, error) {
	if len(doc) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var v any
	if err := json.Unmarshal(doc, &v); err != nil {
		return nil, err
	}
	var walk func(any) any
	walk = func(x any) any {
		switch t := x.(type) {
		case string:
			return Render(t, w, nil)
		case []any:
			for i := range t {
				t[i] = walk(t[i])
			}
			return t
		case map[string]any:
			for k := range t {
				t[k] = walk(t[k])
			}
			return t
		}
		return x
	}
	return json.Marshal(walk(v))
}

// Builtin templates. Operators register their own (config) under other ids
// or newer versions.
func Builtin() []Template {
	ts := []Template{
		{ID: "general", Version: 1, Kind: KindGeneral, Stages: []Stage{
			{ID: "work", Type: StageRun, Phase: "general", Run: &RunSpec{Goal: "${goal.intent}: ${goal.description}\nSubjects: ${subjects}"}},
			{ID: "verify", Type: StageVerify, Phase: "verify", OnFailure: "work", OnTimeout: OnFailBlock},
		}},
		{ID: "incident", Version: 1, Kind: KindIncident, Stages: []Stage{
			{ID: "investigate", Type: StageRun, Phase: "investigate", Run: &RunSpec{
				Goal: "Investigate the incident \"${title}\" on ${subjects}. ${goal.description}\nCollect evidence, rank hypotheses and propose a mitigation. Do not perform production writes."}},
			{ID: "verify", Type: StageVerify, Phase: "verify", OnFailure: "investigate", OnTimeout: OnFailBlock},
			{ID: "review", Type: StageRun, Phase: "review", OnFailure: OnFailSkip, Run: &RunSpec{
				Goal: "Write a postmortem candidate for \"${title}\" on ${subjects}: timeline, trigger, root cause (or unknown), contributing factors, detection and recovery gaps, follow-up actions.\nInvestigation summary: ${stage.investigate.summary}"}},
		}},
		postmortemTemplate(),
		followUpTemplate(),
		{ID: "routine", Version: 1, Kind: KindRoutine, Stages: []Stage{
			{ID: "check", Type: StageRun, Phase: "investigate", Run: &RunSpec{Goal: "Routine check (${goal.intent}) of ${subjects}: report risks, missing monitoring and capacity concerns. ${goal.description}"}},
			{ID: "review", Type: StageConfirm, Phase: "review"},
		}},
	}
	for i := range ts {
		if err := ts[i].Normalize(); err != nil {
			panic(err)
		}
	}
	return ts
}

// Builtin template ids for reviews (RFC §14.5).
const (
	TemplatePostmortem = "postmortem"
	TemplateFollowUp   = "follow-up"
)

// postmortemTemplate drafts a postmortem for a closed Work. The deterministic
// ledger report (timeline, actions, outcomes, hypotheses, gaps) is put in
// the goal description by the server; the model writes prose around it and
// may not invent a root cause. A human reviews the draft.
func postmortemTemplate() Template {
	return Template{ID: TemplatePostmortem, Version: 1, Kind: KindRequest, Queue: QueueBackground, Stages: []Stage{
		{ID: "draft", Type: StageRun, Phase: "review", Run: &RunSpec{Goal: "Draft the postmortem for Work ${label.source_work} (\"${title}\") on ${subjects}.\n" +
			"Use only the ledger report below. Sections: summary, impact, timeline, trigger, root cause, contributing factors, detection gap, recovery gap, what went well, follow-ups.\n" +
			"State a root cause only if the report lists a SUPPORTED hypothesis with evidence; otherwise write \"root cause: unknown\" and list the open hypotheses. " +
			"Do not claim the incident is resolved unless the report shows a verified outcome.\n" +
			"End with a JSON object {\"follow_ups\":[{\"title\",\"why\",\"owner_hint\",\"priority\"}]}.\n\nLedger report:\n${goal.description}"}},
		{ID: "review", Type: StageConfirm, Phase: "review", Confirm: &ConfirmSpec{
			Prompt: "Review the postmortem draft for Work ${label.source_work}; confirm to publish it, or reject to send it back."}},
	}}
}

// followUpTemplate tracks one postmortem follow-up to completion: plan it,
// file a ticket through the controlled action path, wait until the ticket
// is done, then a human confirms the follow-up is actually effective.
// Subjects must include the ticket project (ticket://<PROJECT>) and the
// affected services.
func followUpTemplate() Template {
	return Template{ID: TemplateFollowUp, Version: 1, Kind: KindRequest, Queue: QueueBackground,
		Limits: Limits{MaxDuration: cpkit.Duration(60 * 24 * time.Hour)},
		Stages: []Stage{
			{ID: "plan", Type: StageRun, Phase: "assess", Run: &RunSpec{Goal: "Plan the follow-up \"${title}\" from Work ${label.source_work} on ${subjects}. ${goal.description}\n" +
				"Describe the concrete change, its acceptance check and the owning team, then propose filing it: " +
				"{\"proposed_action\":{\"name\":\"ticket.create\",\"target\":\"ticket://<PROJECT>\",\"args\":{\"summary\",\"description\",\"services\"}}}."}},
			{ID: "file", Type: StageAction, Phase: "mitigate", Action: &ActionSpec{FromProposal: "plan", Allowed: []string{"ticket.create"}}},
			{ID: "track", Type: StageWait, Phase: "verify", OnTimeout: OnFailBlock, Wait: &continuous.Spec{Mode: continuous.OnEvent,
				Purpose: "follow-up ticket done", Timeout: cpkit.Duration(30 * 24 * time.Hour),
				Predicate: &eventing.Predicate{EventTypes: []string{"ticket.updated"},
					Where: &eventing.Cond{Field: "labels.status", Op: "in", Values: []string{"Done", "Closed", "Resolved", "Fixed"}}}}},
			{ID: "confirm", Type: StageConfirm, Phase: "review", Confirm: &ConfirmSpec{
				Prompt: "The follow-up ticket is done. Confirm the change is in place and effective."}},
		}}
}
