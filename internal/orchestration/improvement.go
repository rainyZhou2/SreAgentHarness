package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/work"
)

// Improvement is one corrective action of a postmortem turned into a Work
// (design §12.3). A follow-up is not a line in a report: it is a durable
// Work with an owner, a due date and an acceptance check, linked to the
// incident it came from, and it closes only through a verified outcome or
// an explicit human confirmation like any other Work.
type Improvement struct {
	Title    string `json:"title"`
	Why      string `json:"why,omitempty"`
	Priority string `json:"priority,omitempty"` // high | medium | low
	// Owner is the team or person accountable for the change.
	Owner string     `json:"owner,omitempty"`
	Due   *time.Time `json:"due,omitempty"`
	// Success is the machine-checkable acceptance (a loaded alert rule, an
	// SLO held over a window). Without it a human confirms the change.
	Success *outcome.Spec `json:"success,omitempty"`
	// Kind and Template pick the Work shape (default request / follow-up).
	Kind     work.Kind `json:"kind,omitempty"`
	Template string    `json:"template,omitempty"`
}

// MaxImprovements bounds the Works one postmortem may open.
const MaxImprovements = 20

func priorityOf(p string) int {
	switch strings.ToLower(p) {
	case "high":
		return 20
	case "low":
		return -20
	}
	return 0
}

// CreateImprovements opens one Improvement Work per item for a closed
// source Work. It is idempotent per (source, title): asking twice returns
// the same Works. The Works run in the background queue and never inherit
// write permissions from the incident — each starts at approval_required.
func (s *Service) CreateImprovements(ctx context.Context, src work.Work, actorID string, items []Improvement) ([]work.Work, error) {
	if !src.Status.Terminal() {
		return nil, fmt.Errorf("%w: improvements are derived from a closed Work", work.ErrInvalid)
	}
	if len(items) == 0 || len(items) > MaxImprovements {
		return nil, fmt.Errorf("%w: 1..%d improvement items", work.ErrInvalid, MaxImprovements)
	}
	now := s.Clock.Now()
	var out []work.Work
	for _, it := range items {
		title := cpkit.Truncate(strings.TrimSpace(it.Title), work.MaxTitle)
		if title == "" {
			return out, fmt.Errorf("%w: an improvement needs a title", work.ErrInvalid)
		}
		kind, tpl := it.Kind, it.Template
		if kind == "" {
			kind = work.KindRequest
		}
		if tpl == "" {
			tpl = work.TemplateFollowUp
		}
		labels := map[string]string{"source_work": src.ID, "improvement": "true", "priority": firstNonEmpty(strings.ToLower(it.Priority), "medium")}
		if it.Owner != "" {
			labels["owner"] = cpkit.Truncate(it.Owner, 128)
		}
		if env := src.Goal.Labels["environment"]; env != "" {
			labels["environment"] = env
		}
		goal := work.GoalSpec{Intent: cpkit.Truncate("improve: "+title, work.MaxIntent), Description: cpkit.Truncate(it.Why, work.MaxDescription),
			SubjectRefs: src.Goal.SubjectRefs, Success: it.Success, Deadline: it.Due, Labels: labels,
			Constraints: work.Constraints{ProductionWrites: work.WritesApprovalRequired, DataClass: src.Goal.Constraints.DataClass}}
		if it.Due != nil && !it.Due.After(now) {
			return out, fmt.Errorf("%w: improvement %q is due in the past", work.ErrInvalid, title)
		}
		w, _, err := s.Create(ctx, CreateRequest{TenantID: src.TenantID, ActorID: actorID, ActorType: "user",
			IdempotencyKey: "improvement:" + src.ID + ":" + cpkit.HashBytes([]byte(strings.ToLower(title)))[7:23],
			Kind:           kind, Title: title, TemplateID: tpl, Priority: priorityOf(it.Priority), Queue: work.QueueBackground, Goal: goal})
		if err != nil {
			return out, fmt.Errorf("improvement %q: %w", title, err)
		}
		out = append(out, w)
	}
	return out, nil
}

// Intent is how an everyday request is best served (design §12.4): not
// every request is an incident, and a plain question needs no Work at all.
type Intent struct {
	// NeedsWork is false for a direct question: answer it in a session.
	NeedsWork bool       `json:"needs_work"`
	Kind      work.Kind  `json:"kind,omitempty"`
	Template  string     `json:"template,omitempty"`
	Queue     work.Queue `json:"queue,omitempty"`
	// Writes is the ceiling of production writes the request may need.
	Writes string `json:"production_writes,omitempty"`
	// Acceptance says, in words, what closes the Work.
	Acceptance string `json:"acceptance,omitempty"`
	Reason     string `json:"reason"`
}

type intentRule struct {
	any    []string
	intent Intent
}

// intentRules are checked in order; the first match wins. Matching is on
// plain keywords: routing decides the shape and the ceiling of a Work, and
// that must not depend on a model's reading of the request.
var intentRules = []intentRule{
	{[]string{"rollback", "roll back", "回滚", "紧急", "止损", "mitigate"}, Intent{NeedsWork: true, Kind: work.KindIncident,
		Template: "incident", Queue: work.QueueIncident, Writes: work.WritesApprovalRequired,
		Acceptance: "action approved, target version verified and the SLO recovered", Reason: "a mitigation request"}},
	{[]string{"alert rule", "alerting rule", "add alert", "告警规则", "增加告警", "加告警", "p99 告警", "burn rate"}, Intent{NeedsWork: true,
		Kind: work.KindMonitoring, Template: "monitoring-rule", Queue: work.QueueRoutine, Writes: work.WritesApprovalRequired,
		Acceptance: "rule validated, pull request merged and the rule loaded", Reason: "a monitoring change"}},
	{[]string{"gpu", "xid", "ecc", "nvlink", "mig", "h200", "显卡"}, Intent{NeedsWork: true, Kind: work.KindAIInfra, Template: "",
		Queue: work.QueueRoutine, Writes: work.WritesForbidden, Acceptance: "GPU telemetry is fresh and the Xid/ECC verdict is recorded",
		Reason: "an AI infrastructure check"}},
	{[]string{"ttft", "tpot", "inference", "vllm", "sglang", "推理"}, Intent{NeedsWork: true, Kind: work.KindAIInfra, Template: "",
		Queue: work.QueueIncident, Writes: work.WritesApprovalRequired, Acceptance: "TTFT/TPOT back within the SLO over a stable window",
		Reason: "an inference latency request"}},
	{[]string{"capacity", "cost", "expand", "scale plan", "扩容", "成本", "容量"}, Intent{NeedsWork: true, Kind: work.KindCapacity,
		Queue: work.QueueRoutine, Writes: work.WritesForbidden, Acceptance: "a cost report with a budget that can be recomputed",
		Reason: "a capacity review"}},
	{[]string{"observe", "watch the release", "after release", "发布后", "观察", "灰度"}, Intent{NeedsWork: true, Kind: work.KindChange,
		Queue: work.QueueRoutine, Writes: work.WritesForbidden, Acceptance: "a stable window without SLO burn", Reason: "a release guard"}},
	{[]string{"investigate", "timeout", "error", "latency", "why is", "排查", "超时", "报错", "延迟"}, Intent{NeedsWork: true,
		Kind: work.KindIncident, Template: "", Queue: work.QueueIncident, Writes: work.WritesForbidden,
		Acceptance: "an evidenced conclusion, or an explicit INCONCLUSIVE", Reason: "an investigation"}},
	{[]string{"inspect", "patrol", "巡检", "检查"}, Intent{NeedsWork: true, Kind: work.KindRoutine, Queue: work.QueueRoutine,
		Writes: work.WritesForbidden, Acceptance: "the checks ran on fresh data and risks have follow-up Works", Reason: "a routine inspection"}},
}

// RouteIntent classifies a request. Investigations and checks start with
// production writes forbidden; a request that names a change starts at
// approval_required. Nothing is ever routed to preauthorized: that is an
// operator decision on a Work, not something a request can ask for.
func RouteIntent(text string) Intent {
	t := strings.ToLower(text)
	for _, r := range intentRules {
		for _, k := range r.any {
			if strings.Contains(t, k) {
				return r.intent
			}
		}
	}
	return Intent{NeedsWork: false, Reason: "no long-running goal: answer in a session"}
}
