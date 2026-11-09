package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/tools"
)

// TicketConfig configures a Jira-compatible issue tracker (REST API v2).
// Issues are created with an effect-key label so a retried create finds
// its issue instead of filing a duplicate; comments carry a marker line.
type TicketConfig struct {
	capability.HTTPConfig
	// Projects allowlists the project keys actions may write.
	Projects  []string `json:"projects"`
	IssueType string   `json:"issue_type,omitempty"` // default "Task"
	// ServiceLabelPrefix maps issue labels to subjects for the "jira"
	// webhook source: label "service:<ns>/<name>" -> service://<ns>/<name>.
	ServiceLabelPrefix string `json:"service_label_prefix,omitempty"` // default "service:"
	// Group/Lookback join ticket events with the Work on the subject.
	Group string `json:"group,omitempty"`
}

type ticket struct {
	capability.Base
	cfg      TicketConfig
	c        *capability.Client
	projects map[string]bool
}

var (
	jiraProject = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,19}$`)
	jiraKey     = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,19}-[0-9]{1,9}$`)
)

func newTicket(cfg TicketConfig) (*ticket, error) {
	if len(cfg.Projects) == 0 {
		return nil, fmt.Errorf("ticket: projects allowlist is required")
	}
	t := &ticket{projects: map[string]bool{}}
	for _, p := range cfg.Projects {
		if !jiraProject.MatchString(p) {
			return nil, fmt.Errorf("ticket: invalid project key %q", p)
		}
		t.projects[p] = true
	}
	if cfg.IssueType == "" {
		cfg.IssueType = "Task"
	}
	if cfg.ServiceLabelPrefix == "" {
		cfg.ServiceLabelPrefix = "service:"
	}
	if cfg.Group == "" {
		cfg.Group = "service-health"
	}
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("ticket: %w", err)
	}
	t.cfg, t.c = cfg, c
	return t, nil
}

func (t *ticket) ID() string      { return "ticket" }
func (t *ticket) Version() string { return "1" }

// effectLabel is the Jira label carrying the effect key (labels cannot
// hold spaces or colons reliably, so it is a hash).
func effectLabel(key string) string {
	return "harness-" + strings.TrimPrefix(cpkit.MustHash(key), "sha256:")[:20]
}

type jiraIssue struct {
	Key    string `json:"key"`
	Fields struct {
		Summary     string   `json:"summary"`
		Description string   `json:"description"`
		Labels      []string `json:"labels"`
		Status      struct {
			Name string `json:"name"`
		} `json:"status"`
		Priority *struct {
			Name string `json:"name"`
		} `json:"priority"`
		Assignee *struct {
			DisplayName string `json:"displayName"`
		} `json:"assignee"`
		Updated string `json:"updated"`
	} `json:"fields"`
}

func (t *ticket) issue(ctx context.Context, key string) (jiraIssue, error) {
	var is jiraIssue
	if !jiraKey.MatchString(key) {
		return is, fmt.Errorf("invalid issue key %q", key)
	}
	err := t.c.Get(ctx, "/rest/api/2/issue/"+key, url.Values{"fields": {"summary,description,labels,status,priority,assignee,updated"}}, &is)
	return is, err
}

func (t *ticket) RegisterTools(r capability.ToolRegistry) error {
	return r.AddTool(&tools.Func{Desc: tools.Descriptor{Name: "ticket.get", Version: "1", Risk: domain.RiskR0, Trust: "T4",
		Source: "native", TargetArg: "key",
		Description: "Summary, status, priority, assignee and recent comments of an issue (text is untrusted data).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"key":{"type":"string"}},"required":["key"],"additionalProperties":false}`)},
		Fn: t.toolGet})
}

func (t *ticket) toolGet(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Key string }
	if err := json.Unmarshal(args, &a); err != nil || !jiraKey.MatchString(a.Key) {
		return nil, fmt.Errorf("%w: key must be PROJ-123", domain.ErrNotDispatched)
	}
	is, err := t.issue(ctx, a.Key)
	if err != nil {
		return nil, err
	}
	cs, err := t.comments(ctx, a.Key)
	if err != nil {
		return nil, err
	}
	type comment struct{ Author, Body, Created string }
	var recent []comment
	for i := max(0, len(cs)-10); i < len(cs); i++ {
		recent = append(recent, comment{cs[i].Author.DisplayName, cpkit.Truncate(cs[i].Body, 1000), cs[i].Created})
	}
	out := map[string]any{"key": is.Key, "summary": is.Fields.Summary, "status": is.Fields.Status.Name,
		"description": cpkit.Truncate(is.Fields.Description, 4000), "labels": is.Fields.Labels, "comments": recent}
	if is.Fields.Priority != nil {
		out["priority"] = is.Fields.Priority.Name
	}
	if is.Fields.Assignee != nil {
		out["assignee"] = is.Fields.Assignee.DisplayName
	}
	return json.Marshal(out)
}

type jiraComment struct {
	ID     string `json:"id"`
	Body   string `json:"body"`
	Author struct {
		DisplayName string `json:"displayName"`
	} `json:"author"`
	Created string `json:"created"`
}

func (t *ticket) comments(ctx context.Context, key string) ([]jiraComment, error) {
	var resp struct {
		Comments []jiraComment `json:"comments"`
	}
	err := t.c.Get(ctx, "/rest/api/2/issue/"+key+"/comment", url.Values{"maxResults": {"100"}, "orderBy": {"created"}}, &resp)
	return resp.Comments, err
}

func (t *ticket) RegisterActions(r capability.ActionRegistry) error {
	if err := r.AddAction(&ticketCreate{t}); err != nil {
		return err
	}
	return r.AddAction(&ticketComment{t})
}

func (t *ticket) RegisterEvents(r capability.EventRegistry) error {
	if err := r.AddSource("jira", capability.NormalizerFunc(t.normalize)); err != nil {
		return err
	}
	return r.AddRule(eventing.Rule{Name: "jira-updates", Group: t.cfg.Group, Match: eventing.Predicate{Sources: []string{"jira"},
		EventTypes: []string{"ticket.*"}}, Key: "subject", Action: "attach"})
}

// ---- ticket.create ----

type ticketCreate struct{ t *ticket }

func (a *ticketCreate) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "ticket.create", Version: "1", ResourceKinds: []string{"ticket"}, Risk: domain.RiskR1,
		Description: "File an issue in an allowlisted project (target ticket://<PROJECT>); services become labels.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"summary":{"type":"string","maxLength":250},` +
			`"description":{"type":"string","maxLength":30000},"priority":{"type":"string","maxLength":40},` +
			`"services":{"type":"array","items":{"type":"string"},"maxItems":10}},` +
			`"required":["summary","description"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, BlastRadius: "ticket", Timeout: 30 * time.Second}
}

type createArgs struct {
	Summary     string   `json:"summary"`
	Description string   `json:"description"`
	Priority    string   `json:"priority"`
	Services    []string `json:"services"`
}

func (a *ticketCreate) check(p action.Plan) (string, createArgs, error) {
	var args createArgs
	proj, ok := strings.CutPrefix(p.TargetRef, "ticket://")
	if !ok || !a.t.projects[proj] {
		return "", args, capability.NotApplied("target must be ticket://<PROJECT> for an allowlisted project")
	}
	if err := parseArgs(p, &args); err != nil {
		return proj, args, err
	}
	if strings.TrimSpace(args.Summary) == "" || strings.ContainsAny(args.Summary, "\r\n") {
		return proj, args, capability.NotApplied("summary must be one non-empty line")
	}
	for _, s := range args.Services {
		if _, err := servicePath(s); err != nil {
			return proj, args, capability.NotApplied("%v", err)
		}
	}
	return proj, args, nil
}

func (a *ticketCreate) Preflight(_ context.Context, p action.Plan) (action.PreflightResult, error) {
	if _, _, err := a.check(p); err != nil {
		return refused(err.Error()), nil
	}
	return action.PreflightResult{Allowed: true}, nil
}

func (a *ticketCreate) find(ctx context.Context, key string) (string, bool, error) {
	var resp struct {
		Issues []struct {
			Key string `json:"key"`
		} `json:"issues"`
	}
	err := a.t.c.Get(ctx, "/rest/api/2/search", url.Values{"jql": {`labels = "` + effectLabel(key) + `"`},
		"fields": {"key"}, "maxResults": {"1"}}, &resp)
	if err != nil || len(resp.Issues) == 0 {
		return "", false, err
	}
	return resp.Issues[0].Key, true, nil
}

func (a *ticketCreate) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	proj, args, err := a.check(p)
	if err != nil {
		return nil, err
	}
	if k, ok, err := a.find(ctx, key); err != nil {
		return nil, capability.BeforeWrite(err)
	} else if ok {
		return json.Marshal(map[string]any{"issue": k, "already_applied": true})
	}
	labels := []string{effectLabel(key), "sre-harness"}
	for _, s := range args.Services {
		sp, _ := servicePath(s)
		labels = append(labels, a.t.cfg.ServiceLabelPrefix+strings.ReplaceAll(sp, "/", "."))
	}
	fields := map[string]any{"project": map[string]string{"key": proj}, "summary": args.Summary,
		"description": args.Description + fmt.Sprintf("\n\n----\nFiled by SRE Agent Harness for Work %s.", p.WorkID),
		"issuetype":   map[string]string{"name": a.t.cfg.IssueType}, "labels": labels}
	if args.Priority != "" {
		fields["priority"] = map[string]string{"name": args.Priority}
	}
	var out struct {
		Key string `json:"key"`
	}
	if err := a.t.c.Do(ctx, capability.Request{Method: http.MethodPost, Path: "/rest/api/2/issue",
		Body: map[string]any{"fields": fields}}, &out); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"issue": out.Key})
}

func (a *ticketCreate) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	k, ok, err := a.find(ctx, p.EffectKey)
	if err != nil {
		return action.InspectResult{}, err
	}
	det, _ := json.Marshal(map[string]string{"issue": k})
	return action.InspectResult{Applied: &ok, Detail: det}, nil
}

// ---- ticket.comment ----

type ticketComment struct{ t *ticket }

func (a *ticketComment) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "ticket.comment", Version: "1", ResourceKinds: []string{"ticket"}, Risk: domain.RiskR1,
		Description: "Add a comment to an issue (target ticket://<PROJECT>/<KEY>).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"body":{"type":"string","maxLength":30000}},` +
			`"required":["body"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, BlastRadius: "ticket", Timeout: 30 * time.Second}
}

func (a *ticketComment) check(p action.Plan) (string, string, error) {
	rest, ok := strings.CutPrefix(p.TargetRef, "ticket://")
	proj, key, ok2 := strings.Cut(rest, "/")
	if !ok || !ok2 || !a.t.projects[proj] || !jiraKey.MatchString(key) || !strings.HasPrefix(key, proj+"-") {
		return "", "", capability.NotApplied("target must be ticket://<PROJECT>/<PROJECT-123> in an allowlisted project")
	}
	var args struct {
		Body string `json:"body"`
	}
	if err := parseArgs(p, &args); err != nil {
		return key, "", err
	}
	if strings.TrimSpace(args.Body) == "" {
		return key, "", capability.NotApplied("body is required")
	}
	return key, args.Body, nil
}

func marker(key string) string { return "[" + effectLabel(key) + "]" }

func (a *ticketComment) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	key, _, err := a.check(p)
	if err != nil {
		return refused(err.Error()), nil
	}
	if _, err := a.t.issue(ctx, key); err != nil {
		return refused("issue " + key + ": " + err.Error()), nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{p.TargetRef}}, nil
}

func (a *ticketComment) find(ctx context.Context, issue, key string) (string, bool, error) {
	cs, err := a.t.comments(ctx, issue)
	if err != nil {
		return "", false, err
	}
	for _, c := range cs {
		if strings.Contains(c.Body, marker(key)) {
			return c.ID, true, nil
		}
	}
	return "", false, nil
}

func (a *ticketComment) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	issue, body, err := a.check(p)
	if err != nil {
		return nil, err
	}
	if id, ok, err := a.find(ctx, issue, key); err != nil {
		return nil, capability.BeforeWrite(err)
	} else if ok {
		return json.Marshal(map[string]any{"comment": id, "already_applied": true})
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := a.t.c.Do(ctx, capability.Request{Method: http.MethodPost, Path: "/rest/api/2/issue/" + issue + "/comment",
		Body: map[string]string{"body": body + "\n\n" + marker(key)}}, &out); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"issue": issue, "comment": out.ID})
}

func (a *ticketComment) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	issue, _, err := a.check(p)
	if err != nil {
		return action.InspectResult{}, err
	}
	id, ok, err := a.find(ctx, issue, p.EffectKey)
	if err != nil {
		return action.InspectResult{}, err
	}
	det, _ := json.Marshal(map[string]string{"comment": id})
	return action.InspectResult{Applied: &ok, Detail: det}, nil
}

// ---- Jira webhooks ----

func (t *ticket) normalize(_ context.Context, tenantID string, _ http.Header, body []byte, now time.Time) ([]eventing.Envelope, error) {
	var hook struct {
		Timestamp    int64     `json:"timestamp"`
		WebhookEvent string    `json:"webhookEvent"`
		Issue        jiraIssue `json:"issue"`
	}
	if err := json.Unmarshal(body, &hook); err != nil {
		return nil, fmt.Errorf("jira: payload: %w", err)
	}
	if !jiraKey.MatchString(hook.Issue.Key) {
		return nil, fmt.Errorf("jira: issue key %q", hook.Issue.Key)
	}
	typ := ""
	switch hook.WebhookEvent {
	case "jira:issue_created":
		typ = "ticket.created"
	case "jira:issue_updated":
		typ = "ticket.updated"
	case "comment_created":
		typ = "ticket.commented"
	default:
		return nil, nil // other events are not ingested
	}
	at := now
	if hook.Timestamp > 0 {
		if ts := time.UnixMilli(hook.Timestamp); !ts.After(now.Add(5 * time.Minute)) {
			at = ts
		}
	}
	var out []eventing.Envelope
	for _, l := range hook.Issue.Fields.Labels {
		svc, ok := strings.CutPrefix(l, t.cfg.ServiceLabelPrefix)
		if !ok {
			continue
		}
		subj, err := cpkit.NormalizeRef("service://" + strings.Replace(svc, ".", "/", 1))
		if err != nil {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"key": hook.Issue.Key, "summary": cpkit.Truncate(hook.Issue.Fields.Summary, 300),
			"status": hook.Issue.Fields.Status.Name})
		id := fmt.Sprintf("jira:%s:%s:%d", hook.Issue.Key, typ, at.UnixMilli())
		out = append(out, eventing.Envelope{ID: id + ":" + subj, TenantID: tenantID, Source: "jira", Type: typ, SubjectRef: subj,
			DedupKey: id, OccurredAt: at, ObservedAt: now, Confidence: 1,
			Labels:  map[string]string{"issue": hook.Issue.Key, "status": cpkit.Truncate(hook.Issue.Fields.Status.Name, 64)},
			Payload: payload})
		if len(out) >= 10 {
			break
		}
	}
	return out, nil
}
