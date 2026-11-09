package sre

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// GitOpsConfig configures the GitOps capability on a GitHub (or GitHub
// Enterprise) repository: change history and diffs as read tools, commits
// on the base branch as change events, and two actions that only ever
// open pull requests — merging stays with the repository's own review and
// branch protection:
//
//   - gitops.propose_change (R1): write one file on a new branch and open a PR;
//   - gitops.revert (R2): open a PR that restores the files of a commit.
type GitOpsConfig struct {
	capability.HTTPConfig        // url: https://api.github.com
	Owner                 string `json:"owner"`
	Repo                  string `json:"repo"`
	Base                  string `json:"base,omitempty"` // default main
	// Paths are the path prefixes actions may write (required).
	Paths []string `json:"paths"`
	// Subjects maps path prefixes to subject refs for change events, e.g.
	// {"apps/checkout/": "service://prod/checkout"}.
	Subjects map[string]string `json:"subjects,omitempty"`
	// CollectEvery polls the base branch for new commits (default 2m).
	CollectEvery capability.Duration `json:"collect_every,omitempty"`
	// ArgoCD accepts Argo CD notification webhooks as change events.
	ArgoCD *ArgoCDConfig `json:"argocd,omitempty"`
}

// ArgoCDConfig configures the "argocd" event source. The notification
// template must post JSON with: app, project, destination_namespace,
// revision, sync_status, health_status, phase, finished_at (RFC 3339) and
// optionally service (defaults to app).
type ArgoCDConfig struct {
	// Group joins deploy events with alerts on the same subject (rule
	// group, default "service-health"); Lookback is how far back an alert
	// incident picks up deploys (default 1h).
	Group    string              `json:"group,omitempty"`
	Lookback capability.Duration `json:"lookback,omitempty"`
}

type gitops struct {
	capability.Base
	cfg GitOpsConfig
	c   *capability.Client
	// seen is the newest commit already reported (process-local; the inbox
	// dedups by commit sha across restarts and replicas).
	seen string
}

var ghName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,100}$`)

func newGitOps(cfg GitOpsConfig) (*gitops, error) {
	if cfg.URL == "" {
		cfg.URL = "https://api.github.com"
	}
	if !ghName.MatchString(cfg.Owner) || !ghName.MatchString(cfg.Repo) {
		return nil, fmt.Errorf("gitops: owner and repo are required")
	}
	if len(cfg.Paths) == 0 {
		return nil, fmt.Errorf("gitops: paths (writable prefixes) are required")
	}
	for _, p := range cfg.Paths {
		if p == "" || strings.HasPrefix(p, "/") || strings.Contains(p, "..") {
			return nil, fmt.Errorf("gitops: invalid path prefix %q", p)
		}
	}
	if cfg.Base == "" {
		cfg.Base = "main"
	}
	if cfg.CollectEvery == 0 {
		cfg.CollectEvery = capability.Duration(2 * time.Minute)
	}
	if cfg.Headers == nil {
		cfg.Headers = map[string]string{}
	}
	cfg.Headers["Accept"] = "application/vnd.github+json"
	cfg.Headers["X-GitHub-Api-Version"] = "2022-11-28"
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("gitops: %w", err)
	}
	return &gitops{cfg: cfg, c: c}, nil
}

func (g *gitops) ID() string      { return "gitops" }
func (g *gitops) Version() string { return "1" }

func (g *gitops) repoPath() string { return "/repos/" + g.cfg.Owner + "/" + g.cfg.Repo }

// RepoRef is the target ref of repository-level actions.
func (g *gitops) RepoRef() string { return "git://" + g.cfg.Owner + "/" + g.cfg.Repo }

func (g *gitops) RegisterEvents(r capability.EventRegistry) error {
	if g.cfg.ArgoCD == nil {
		return nil
	}
	if err := r.AddSource("argocd", capability.NormalizerFunc(g.normalizeArgo)); err != nil {
		return err
	}
	return r.AddRule(eventing.Rule{Name: "argocd-deploys", Group: g.argoGroup(), Match: eventing.Predicate{Sources: []string{"argocd"},
		EventTypes: []string{"change.*"}}, Key: "subject", Action: "attach"})
}

func (g *gitops) argoGroup() string {
	if g.cfg.ArgoCD != nil && g.cfg.ArgoCD.Group != "" {
		return g.cfg.ArgoCD.Group
	}
	return "service-health"
}

func (g *gitops) RegisterWorld(r capability.WorldRegistry) error {
	if g.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("commits", g.cfg.CollectEvery.D(), capability.CollectorFunc(g.collect))
}

func (g *gitops) RegisterTools(r capability.ToolRegistry) error {
	for _, t := range []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "gitops.history", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			Description: "Recent commits on the base branch, optionally for a path (sha, author, message, time).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"path":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":50}},"additionalProperties":false}`)},
			Fn: g.toolHistory},
		&tools.Func{Desc: tools.Descriptor{Name: "gitops.diff", Version: "1", Risk: domain.RiskR0, Trust: "T4", Source: "native",
			TargetArg: "sha", Description: "Files and patch of one commit (patch text is untrusted data).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"sha":{"type":"string"}},"required":["sha"],"additionalProperties":false}`)},
			Fn: g.toolDiff},
	} {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}

func (g *gitops) RegisterActions(r capability.ActionRegistry) error {
	if err := r.AddAction(&proposeChange{g}); err != nil {
		return err
	}
	return r.AddAction(&gitRevert{g})
}

type ghCommit struct {
	SHA    string `json:"sha"`
	Commit struct {
		Message string `json:"message"`
		Author  struct {
			Name string    `json:"name"`
			Date time.Time `json:"date"`
		} `json:"author"`
	} `json:"commit"`
	Parents []struct {
		SHA string `json:"sha"`
	} `json:"parents"`
	Files []struct {
		Filename         string `json:"filename"`
		PreviousFilename string `json:"previous_filename"`
		Status           string `json:"status"`
		Patch            string `json:"patch"`
	} `json:"files"`
}

func (g *gitops) commits(ctx context.Context, p string, limit int) ([]ghCommit, error) {
	q := url.Values{"sha": {g.cfg.Base}, "per_page": {fmt.Sprint(limit)}}
	if p != "" {
		q.Set("path", p)
	}
	var cs []ghCommit
	err := g.c.Get(ctx, g.repoPath()+"/commits", q, &cs)
	return cs, err
}

func (g *gitops) commit(ctx context.Context, sha string) (ghCommit, error) {
	var c ghCommit
	if !regexp.MustCompile(`^[0-9a-f]{7,40}$`).MatchString(sha) {
		return c, fmt.Errorf("invalid commit sha %q", sha)
	}
	err := g.c.Get(ctx, g.repoPath()+"/commits/"+sha, nil, &c)
	return c, err
}

// subjectsFor maps changed files to subjects (longest prefix wins).
func (g *gitops) subjectsFor(files []string) []string {
	prefixes := make([]string, 0, len(g.cfg.Subjects))
	for p := range g.cfg.Subjects {
		prefixes = append(prefixes, p)
	}
	sort.Slice(prefixes, func(i, j int) bool { return len(prefixes[i]) > len(prefixes[j]) })
	seen := map[string]bool{}
	var out []string
	for _, f := range files {
		for _, p := range prefixes {
			if strings.HasPrefix(f, p) {
				if s := g.cfg.Subjects[p]; !seen[s] {
					seen[s] = true
					out = append(out, s)
				}
				break
			}
		}
	}
	return out
}

// collect reports new commits on the base branch as change.committed
// events on the subjects their files map to.
func (g *gitops) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	cs, err := g.commits(ctx, "", 20)
	if err != nil || len(cs) == 0 {
		return capability.Observation{}, err
	}
	var o capability.Observation
	first := g.seen == ""
	for _, c := range cs {
		if c.SHA == g.seen {
			break
		}
		if first && now.Sub(c.Commit.Author.Date) > time.Hour {
			continue // on start, only recent history
		}
		full, err := g.commit(ctx, c.SHA)
		if err != nil {
			return o, err
		}
		var files []string
		for _, f := range full.Files {
			files = append(files, f.Filename)
		}
		for _, subj := range g.subjectsFor(files) {
			payload, _ := json.Marshal(map[string]any{"sha": c.SHA, "author": c.Commit.Author.Name,
				"message": cpkit.Truncate(c.Commit.Message, 500), "files": files[:min(len(files), 50)]})
			o.Events = append(o.Events, eventing.Envelope{ID: "git:" + c.SHA + ":" + subj, Source: "gitops",
				Type: "change.committed", SubjectRef: subj, DedupKey: "git:" + c.SHA, OccurredAt: c.Commit.Author.Date,
				ObservedAt: now, Confidence: 1, Labels: map[string]string{"sha": c.SHA[:min(12, len(c.SHA))],
					"repo": g.cfg.Owner + "/" + g.cfg.Repo}, Payload: payload})
		}
	}
	g.seen = cs[0].SHA
	o.Facts = append(o.Facts, world.Fact{SubjectRef: g.RepoRef(), Predicate: "head", Value: cs[0].SHA,
		TrustTier: world.TrustFirstParty})
	return o, nil
}

func (g *gitops) toolHistory(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct {
		Path  string
		Limit int
	}
	_ = json.Unmarshal(args, &a)
	if a.Limit <= 0 || a.Limit > 50 {
		a.Limit = 20
	}
	cs, err := g.commits(ctx, a.Path, a.Limit)
	if err != nil {
		return nil, err
	}
	type item struct {
		SHA, Author, Message string
		At                   time.Time
	}
	out := []item{}
	for _, c := range cs {
		out = append(out, item{c.SHA, c.Commit.Author.Name, cpkit.Truncate(c.Commit.Message, 300), c.Commit.Author.Date})
	}
	return json.Marshal(map[string]any{"commits": out})
}

func (g *gitops) toolDiff(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ SHA string }
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	c, err := g.commit(ctx, a.SHA)
	if err != nil {
		return nil, err
	}
	type file struct{ Name, Status, Patch string }
	var fs []file
	for i, f := range c.Files {
		if i >= 30 {
			break
		}
		fs = append(fs, file{f.Filename, f.Status, cpkit.Truncate(f.Patch, 4000)})
	}
	return json.Marshal(map[string]any{"sha": c.SHA, "message": cpkit.Truncate(c.Commit.Message, 1000), "files": fs})
}

// WorkTemplates ships the monitoring-config scenario.
func (g *gitops) WorkTemplates() []work.Template { return []work.Template{MonitoringConfigTemplate()} }

// MonitoringConfigTemplate is the monitoring-config scenario: an agent
// drafts a PrometheusRule (or any monitoring config file) for a service,
// the change goes out as a pull request (gitops.propose_change, R1), a
// human merges it, and the Work succeeds only when Prometheus reports the
// rule loaded and healthy (metric source prometheus_rules). Create the
// Work with subjects [service://<ns>/<svc>, git://<owner>/<repo>/<rule file>].
func MonitoringConfigTemplate() work.Template {
	return work.Template{ID: "monitoring-config", Version: 1, Kind: work.KindMonitoring, Stages: []work.Stage{
		{ID: "draft", Type: work.StageRun, Phase: "assess", Run: &work.RunSpec{Goal: "Monitoring change for ${subjects}: " +
			"${goal.intent}. ${goal.description}\nInventory the existing rules (prometheus.rules), check the metrics exist " +
			"(prometheus.query) and read the current file history (gitops.history). Draft a complete PrometheusRule YAML " +
			"(alert names, expressions with for: durations, severity labels, runbook annotations) and propose it as " +
			"{\"proposed_action\":{\"name\":\"gitops.propose_change\",\"target\":\"<the git:// subject>\"," +
			"\"args\":{\"content\":<yaml>,\"title\":...,\"description\":...}}}."}},
		{ID: "propose", Type: work.StageAction, Phase: "mitigate", OnFailure: "draft", Action: &work.ActionSpec{
			FromProposal: "draft", Allowed: []string{"gitops.propose_change"}}},
		{ID: "merge", Type: work.StageConfirm, Phase: "review", Confirm: &work.ConfirmSpec{
			Prompt: "Review and merge the pull request, let the GitOps controller sync it, then confirm."}},
		{ID: "verify", Type: work.StageVerify, Phase: "verify", OnFailure: "draft", OnTimeout: work.OnFailBlock,
			Verify: &work.VerifySpec{RecheckEvery: cpkit.Duration(time.Minute), Timeout: cpkit.Duration(time.Hour)}},
	}}
}

// RuleLoadedSuccess is the success spec of a monitoring-config Work.
func RuleLoadedSuccess(group, rule string) outcome.Spec {
	return outcome.Spec{Version: 1, AllOf: []outcome.Objective{{ID: "rule_loaded", Kind: outcome.KindMetric,
		TargetRef: "promrule://" + group + "/" + rule, Window: cpkit.Duration(5 * time.Minute),
		Metric: &outcome.MetricExpr{Source: "prometheus_rules", Query: group + "/" + rule, Aggregate: outcome.AggLast,
			Operator: ">=", Threshold: 1}}}}
}

// ---- pull-request plumbing ----

// branchFor derives the PR branch from the effect key: a retry of the
// same action finds its own branch and PR instead of opening another.
func branchFor(key string) string {
	return "harness/" + strings.TrimPrefix(cpkit.MustHash(key), "sha256:")[:16]
}

type ghPR struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
	Merged  bool   `json:"merged"`
	Head    struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

func (g *gitops) findPR(ctx context.Context, branch string) (ghPR, bool, error) {
	var prs []ghPR
	if err := g.c.Get(ctx, g.repoPath()+"/pulls", url.Values{"head": {g.cfg.Owner + ":" + branch}, "state": {"all"}}, &prs); err != nil {
		return ghPR{}, false, err
	}
	if len(prs) == 0 {
		return ghPR{}, false, nil
	}
	return prs[0], true, nil
}

// ensureBranch creates branch at the base head (idempotent).
func (g *gitops) ensureBranch(ctx context.Context, branch string) error {
	var ref struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	if err := g.c.Get(ctx, g.repoPath()+"/git/ref/heads/"+g.cfg.Base, nil, &ref); err != nil {
		return err
	}
	err := g.c.Do(ctx, capability.Request{Method: http.MethodPost, Path: g.repoPath() + "/git/refs",
		Body: map[string]string{"ref": "refs/heads/" + branch, "sha": ref.Object.SHA}}, nil)
	if capability.IsStatus(err, http.StatusUnprocessableEntity) {
		return nil // exists: a retry
	}
	return err
}

type ghContent struct {
	SHA      string `json:"sha"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

func (g *gitops) content(ctx context.Context, p, ref string) (ghContent, bool, error) {
	var c ghContent
	err := g.c.Get(ctx, g.repoPath()+"/contents/"+p, url.Values{"ref": {ref}}, &c)
	if capability.IsStatus(err, http.StatusNotFound) {
		return c, false, nil
	}
	return c, err == nil, err
}

func (c ghContent) decode() ([]byte, error) {
	if c.Encoding != "base64" {
		return nil, fmt.Errorf("unsupported content encoding %q", c.Encoding)
	}
	return base64.StdEncoding.DecodeString(strings.ReplaceAll(c.Content, "\n", ""))
}

// putFile writes p on branch (create or update).
func (g *gitops) putFile(ctx context.Context, branch, p string, data []byte, msg string) error {
	cur, ok, err := g.content(ctx, p, branch)
	if err != nil {
		return err
	}
	body := map[string]string{"message": msg, "content": base64.StdEncoding.EncodeToString(data), "branch": branch}
	if ok {
		if old, err := cur.decode(); err == nil && string(old) == string(data) {
			return nil // already written by an earlier attempt
		}
		body["sha"] = cur.SHA
	}
	return g.c.Do(ctx, capability.Request{Method: http.MethodPut, Path: g.repoPath() + "/contents/" + p, Body: body}, nil)
}

func (g *gitops) deleteFile(ctx context.Context, branch, p, msg string) error {
	cur, ok, err := g.content(ctx, p, branch)
	if err != nil || !ok {
		return err
	}
	return g.c.Do(ctx, capability.Request{Method: http.MethodDelete, Path: g.repoPath() + "/contents/" + p,
		Body: map[string]string{"message": msg, "sha": cur.SHA, "branch": branch}}, nil)
}

func (g *gitops) openPR(ctx context.Context, branch, title, body string) (ghPR, error) {
	if pr, ok, err := g.findPR(ctx, branch); err != nil || ok {
		return pr, err
	}
	var pr ghPR
	err := g.c.Do(ctx, capability.Request{Method: http.MethodPost, Path: g.repoPath() + "/pulls",
		Body: map[string]any{"title": title, "head": branch, "base": g.cfg.Base, "body": body, "maintainer_can_modify": true}}, &pr)
	return pr, err
}

func (g *gitops) writable(p string) bool {
	clean := path.Clean(p)
	if clean != p || strings.HasPrefix(p, "/") || strings.Contains(p, "..") {
		return false
	}
	for _, prefix := range g.cfg.Paths {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

// filePath extracts the path from a git://owner/repo/<path> target.
func (g *gitops) filePath(ref string) (string, error) {
	rest, ok := strings.CutPrefix(ref, g.RepoRef()+"/")
	if !ok || rest == "" {
		return "", fmt.Errorf("target must be %s/<path>", g.RepoRef())
	}
	if !g.writable(rest) {
		return "", fmt.Errorf("path %q is not under a writable prefix %v", rest, g.cfg.Paths)
	}
	return rest, nil
}

func prBody(p action.Plan, extra string) string {
	return fmt.Sprintf("Opened by SRE Agent Harness for Work `%s` (action `%s`, plan `%s`).\n\n%s\n\n"+
		"Merging is a human decision under this repository's review rules.", p.WorkID, p.ID, p.PlanHash, extra)
}

// ---- gitops.propose_change ----

type proposeChange struct{ g *gitops }

func (a *proposeChange) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "gitops.propose_change", Version: "1", ResourceKinds: []string{"git"}, Risk: domain.RiskR1,
		Description: "Write one file on a new branch and open a pull request (merging stays with the repository's review).",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"content":{"type":"string","maxLength":262144},` +
			`"title":{"type":"string","maxLength":200},"description":{"type":"string","maxLength":4000}},` +
			`"required":["content","title"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsRollback: true, BlastRadius: "pull_request",
		Timeout: 2 * time.Minute}
}

type proposeArgs struct {
	Content     string `json:"content"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

func (a *proposeChange) check(p action.Plan) (string, proposeArgs, error) {
	fp, err := a.g.filePath(p.TargetRef)
	if err != nil {
		return "", proposeArgs{}, capability.NotApplied("%v", err)
	}
	var args proposeArgs
	if err := parseArgs(p, &args); err != nil {
		return "", args, err
	}
	if strings.TrimSpace(args.Title) == "" || args.Content == "" {
		return "", args, capability.NotApplied("title and content are required")
	}
	if ext := path.Ext(fp); ext == ".yaml" || ext == ".yml" {
		var doc any
		if err := yaml.Unmarshal([]byte(args.Content), &doc); err != nil {
			return "", args, capability.NotApplied("content is not valid YAML: %v", err)
		}
	}
	if path.Ext(fp) == ".json" && !json.Valid([]byte(args.Content)) {
		return "", args, capability.NotApplied("content is not valid JSON")
	}
	return fp, args, nil
}

func (a *proposeChange) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	fp, args, err := a.check(p)
	if err != nil {
		return refused(err.Error()), nil
	}
	cur, ok, err := a.g.content(ctx, fp, a.g.cfg.Base)
	if err != nil {
		return refused("read " + fp + ": " + err.Error()), nil
	}
	if ok {
		if old, err := cur.decode(); err == nil && string(old) == args.Content {
			return refused(fp + " already has this content"), nil
		}
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{p.TargetRef + "@" + a.g.cfg.Base}}, nil
}

func (a *proposeChange) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	fp, args, err := a.check(p)
	if err != nil {
		return nil, err
	}
	branch := branchFor(key)
	if err := a.g.ensureBranch(ctx, branch); err != nil {
		return nil, capability.ActionError(err)
	}
	if err := a.g.putFile(ctx, branch, fp, []byte(args.Content), args.Title); err != nil {
		return nil, err // the branch exists now: unknown, inspectable
	}
	pr, err := a.g.openPR(ctx, branch, args.Title, prBody(p, args.Description))
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"pull_request": pr.Number, "url": pr.HTMLURL, "branch": branch, "path": fp})
}

func (a *proposeChange) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	pr, ok, err := a.g.findPR(ctx, branchFor(p.EffectKey))
	if err != nil {
		return action.InspectResult{}, err
	}
	det, _ := json.Marshal(map[string]any{"pull_request": pr.Number, "state": pr.State, "merged": pr.Merged})
	return action.InspectResult{Applied: &ok, Detail: det}, nil
}

// ---- gitops.revert ----

type gitRevert struct{ g *gitops }

func (a *gitRevert) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "gitops.revert", Version: "1", ResourceKinds: []string{"git"}, Risk: domain.RiskR2,
		Description: "Open a pull request that restores the files a commit changed to their state before it.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"commit":{"type":"string"},"reason":{"type":"string","maxLength":2000}},` +
			`"required":["commit"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, BlastRadius: "pull_request", Timeout: 3 * time.Minute}
}

type revertArgs struct {
	Commit string `json:"commit"`
	Reason string `json:"reason"`
}

func (a *gitRevert) plan(ctx context.Context, p action.Plan) (ghCommit, revertArgs, error) {
	if p.TargetRef != a.g.RepoRef() {
		return ghCommit{}, revertArgs{}, capability.NotApplied("target must be %s", a.g.RepoRef())
	}
	var args revertArgs
	if err := parseArgs(p, &args); err != nil {
		return ghCommit{}, args, err
	}
	c, err := a.g.commit(ctx, args.Commit)
	if err != nil {
		return c, args, capability.BeforeWrite(err)
	}
	if len(c.Parents) != 1 {
		return c, args, capability.NotApplied("commit %s has %d parents; only single-parent commits are reverted", args.Commit, len(c.Parents))
	}
	if len(c.Files) == 0 || len(c.Files) > 50 {
		return c, args, capability.NotApplied("commit %s changes %d files (1-50 supported)", args.Commit, len(c.Files))
	}
	for _, f := range c.Files {
		if !a.g.writable(f.Filename) || (f.PreviousFilename != "" && !a.g.writable(f.PreviousFilename)) {
			return c, args, capability.NotApplied("commit touches %s outside the writable paths", f.Filename)
		}
	}
	return c, args, nil
}

func (a *gitRevert) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	c, _, err := a.plan(ctx, p)
	if err != nil {
		return refused(err.Error()), nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{a.g.RepoRef() + "@" + c.SHA}}, nil
}

func (a *gitRevert) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	c, args, err := a.plan(ctx, p)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	parent := c.Parents[0].SHA
	// Read every parent version before writing anything.
	type change struct {
		path   string
		data   []byte
		delete bool
	}
	var changes []change
	for _, f := range c.Files {
		switch f.Status {
		case "added":
			changes = append(changes, change{path: f.Filename, delete: true})
		case "renamed":
			changes = append(changes, change{path: f.Filename, delete: true})
			fallthrough
		default:
			src := f.Filename
			if f.PreviousFilename != "" {
				src = f.PreviousFilename
			}
			cur, ok, err := a.g.content(ctx, src, parent)
			if err != nil {
				return nil, capability.BeforeWrite(err)
			}
			if !ok {
				return nil, capability.NotApplied("%s is missing at %s", src, parent)
			}
			data, err := cur.decode()
			if err != nil {
				return nil, capability.BeforeWrite(err)
			}
			changes = append(changes, change{path: src, data: data})
		}
	}
	branch := branchFor(key)
	if err := a.g.ensureBranch(ctx, branch); err != nil {
		return nil, capability.ActionError(err)
	}
	msg := fmt.Sprintf("Revert %s\n\n%s", c.SHA[:12], cpkit.Truncate(args.Reason, 2000))
	for _, ch := range changes {
		if ch.delete {
			err = a.g.deleteFile(ctx, branch, ch.path, msg)
		} else {
			err = a.g.putFile(ctx, branch, ch.path, ch.data, msg)
		}
		if err != nil {
			return nil, err
		}
	}
	firstLine, _, _ := strings.Cut(c.Commit.Message, "\n")
	pr, err := a.g.openPR(ctx, branch, cpkit.Truncate("Revert \""+firstLine+"\"", 200),
		prBody(p, fmt.Sprintf("Reverts %s.\n\n%s", c.SHA, args.Reason)))
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"pull_request": pr.Number, "url": pr.HTMLURL, "branch": branch, "reverts": c.SHA,
		"files": len(changes)})
}

func (a *gitRevert) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	return (&proposeChange{a.g}).Inspect(ctx, p)
}

// ---- Argo CD notifications ----

type argoNotification struct {
	App          string    `json:"app"`
	Project      string    `json:"project"`
	Namespace    string    `json:"destination_namespace"`
	Service      string    `json:"service"`
	Revision     string    `json:"revision"`
	SyncStatus   string    `json:"sync_status"`
	HealthStatus string    `json:"health_status"`
	Phase        string    `json:"phase"`
	FinishedAt   time.Time `json:"finished_at"`
}

func (g *gitops) normalizeArgo(_ context.Context, tenantID string, _ http.Header, body []byte, now time.Time) ([]eventing.Envelope, error) {
	var n argoNotification
	if err := json.Unmarshal(body, &n); err != nil {
		return nil, fmt.Errorf("argocd: payload: %w", err)
	}
	if !dns1123.MatchString(n.App) {
		return nil, fmt.Errorf("argocd: app %q is not a valid name", n.App)
	}
	svc := n.Service
	if svc == "" {
		svc = n.App
	}
	subj := "service://" + svc
	if n.Namespace != "" {
		subj = "service://" + n.Namespace + "/" + svc
	}
	subj, err := cpkit.NormalizeRef(subj)
	if err != nil {
		return nil, fmt.Errorf("argocd: subject: %w", err)
	}
	typ := "change.deployed"
	switch strings.ToLower(n.Phase) {
	case "failed", "error":
		typ = "change.failed"
	case "running":
		typ = "change.started"
	}
	at := n.FinishedAt
	if at.IsZero() || at.After(now.Add(5*time.Minute)) {
		at = now
	}
	payload, _ := json.Marshal(n)
	rev := cpkit.Truncate(n.Revision, 64)
	return []eventing.Envelope{{ID: fmt.Sprintf("argocd:%s:%s:%s", n.App, rev, typ), TenantID: tenantID, Source: "argocd",
		Type: typ, SubjectRef: subj, DedupKey: "argocd:" + n.App + ":" + rev, OccurredAt: at, ObservedAt: now, Confidence: 1,
		Labels: map[string]string{"app": n.App, "revision": rev, "health": cpkit.Truncate(n.HealthStatus, 64),
			"sync": cpkit.Truncate(n.SyncStatus, 64)}, Payload: payload}}, nil
}
