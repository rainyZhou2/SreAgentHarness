package orchestration

import (
	"fmt"
	"strings"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/work"
)

// DryRunStep is what one stage would do.
type DryRunStep struct {
	Stage    string   `json:"stage"`
	Type     string   `json:"type"`
	Phase    string   `json:"phase,omitempty"`
	Would    string   `json:"would"`
	Runs     int      `json:"runs,omitempty"`
	Actions  []string `json:"actions,omitempty"`
	Approval string   `json:"approval,omitempty"`
	OnFail   string   `json:"on_failure,omitempty"`
}

// DryRunPlan is the simulation of a template for a goal (design §13.3
// `POST /v1/work-templates/{id}/dry-run`). Nothing is created, started or
// dispatched: it answers "what would this Work do, and could it?" from the
// template, the goal and the registered actions alone.
type DryRunPlan struct {
	Template string       `json:"template"`
	Version  int          `json:"version"`
	Queue    work.Queue   `json:"queue"`
	Steps    []DryRunStep `json:"steps"`
	Limits   work.Limits  `json:"limits"`
	MaxRuns  int          `json:"max_runs_needed"`
	// Problems would stop the Work (unknown action, writes forbidden, no
	// success criteria for a verify stage ...).
	Problems []string `json:"problems,omitempty"`
	// Closes says what ends the Work: a machine-checked outcome or a human.
	Closes string `json:"closes_by"`
	OK     bool   `json:"ok"`
}

// DryRun simulates tpl for goal. actions are the registered action
// descriptors; actionsEnabled says whether the action runtime is on.
func DryRun(tpl work.Template, kind work.Kind, goal work.GoalSpec, actions []action.Descriptor, actionsEnabled bool) DryRunPlan {
	plan := DryRunPlan{Template: tpl.ID, Version: tpl.Version}
	problem := func(format string, a ...any) { plan.Problems = append(plan.Problems, fmt.Sprintf(format, a...)) }
	if err := tpl.Normalize(); err != nil {
		problem("template: %v", err)
		return plan
	}
	if kind == "" {
		kind = tpl.Kind
	}
	if kind == "" {
		kind = work.KindGeneral
	}
	if goal.Intent == "" {
		goal.Intent = "dry run"
	}
	if err := goal.Normalize(); err != nil {
		problem("goal: %v", err)
	}
	plan.Queue, plan.Limits = work.DefaultQueue(kind, tpl), tpl.Limits
	known := map[string]action.Descriptor{}
	for _, d := range actions {
		known[d.Name] = d
	}
	scheme := ""
	if s := goal.PrimarySubject(); s != "" {
		scheme, _, _ = strings.Cut(s, "://")
	}
	hasVerify := false
	for _, st := range tpl.Stages {
		step := DryRunStep{Stage: st.ID, Type: string(st.Type), Phase: st.Phase, OnFail: st.OnFailure}
		switch st.Type {
		case work.StageRun:
			step.Runs, step.Would = 1, "start one agent run and wait for its result"
		case work.StageAgents:
			step.Runs = len(st.Agents)
			roles := make([]string, len(st.Agents))
			for i, a := range st.Agents {
				roles[i] = a.Role
			}
			step.Would = "start read-only agents in parallel (" + strings.Join(roles, ", ") + ") and synthesize their findings"
		case work.StageWait:
			step.Would = "wait on a durable watch: " + string(st.Wait.Mode)
		case work.StageAction:
			names := st.Action.Allowed
			if st.Action.Name != "" {
				names = []string{st.Action.Name}
			}
			step.Actions = names
			step.Would = "propose, preflight and — after approval — dispatch one action, then verify its effect"
			switch {
			case !actionsEnabled:
				problem("stage %s: the action runtime is off at this rollout stage; the Work would block", st.ID)
			case goal.Constraints.ProductionWrites == work.WritesForbidden:
				problem("stage %s: the goal forbids production writes", st.ID)
			}
			usable := 0
			for _, n := range names {
				d, ok := known[n]
				if !ok {
					continue
				}
				usable++
				if scheme != "" && st.Action.Target == "" {
					fits := false
					for _, k := range d.ResourceKinds {
						fits = fits || k == scheme
					}
					if !fits && len(names) == 1 {
						problem("stage %s: action %s cannot target %s resources", st.ID, n, scheme)
					}
				}
				switch d.Risk {
				case "R2":
					step.Approval = "required"
				case "R1":
					if step.Approval == "" {
						step.Approval = "by policy"
					}
				}
			}
			if usable == 0 && actionsEnabled {
				problem("stage %s: none of the actions %v is registered", st.ID, names)
			}
		case work.StageVerify:
			hasVerify = true
			step.Would = "evaluate the success criteria until they hold for their stable window"
			if goal.Success == nil && (st.Verify == nil || st.Verify.Spec == nil) {
				step.Would = "no machine-checkable success criteria: wait for a human confirmation"
			}
		case work.StageConfirm:
			step.Would = "wait for a human confirmation"
		case work.StageChildren:
			n := 0
			if tpl.Graph != nil {
				n = len(tpl.Graph.Children)
			}
			step.Would = fmt.Sprintf("start %d child Work(s) within this Work's ceiling and wait for the required ones", n)
		}
		plan.MaxRuns += step.Runs
		plan.Steps = append(plan.Steps, step)
	}
	if plan.MaxRuns > tpl.Limits.MaxRuns {
		problem("one pass needs %d runs, the template allows %d", plan.MaxRuns, tpl.Limits.MaxRuns)
	}
	switch {
	case goal.Success != nil && hasVerify:
		plan.Closes = "a satisfied outcome evaluation of the goal's success criteria"
	default:
		plan.Closes = "an explicit human confirmation (no machine-checked outcome is configured)"
	}
	plan.OK = len(plan.Problems) == 0
	return plan
}
