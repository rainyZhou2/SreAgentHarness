package sre

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/world"
)

var ctx = context.Background()

func serve(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return s
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func plan(name, target string, args any) action.Plan {
	b, _ := json.Marshal(args)
	return action.Plan{ID: "act-1", WorkID: "w-1", ActionName: name, TargetRef: target, Args: b, PlanHash: "sha256:p",
		EffectKey: "effect-1"}
}

// recorder collects what a collector applied.
type recorder struct {
	mu        sync.Mutex
	facts     []world.Fact
	relations []world.Relation
	events    []eventing.Envelope
}

func (r *recorder) env() plugin.WorkEnv {
	return plugin.WorkEnv{
		AssertFact: func(_ context.Context, f world.Fact) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.facts = append(r.facts, f)
			return nil
		},
		AssertRelation: func(_ context.Context, x world.Relation) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.relations = append(r.relations, x)
			return nil
		},
		RetractRelation: func(context.Context, string, string, string, string, string, time.Time) error { return nil },
		Ingest: func(_ context.Context, e eventing.Envelope) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.events = append(r.events, e)
			return nil
		},
	}
}

func (r *recorder) fact(subject, pred string) (world.Fact, bool) {
	for _, f := range r.facts {
		if f.SubjectRef == subject && f.Predicate == pred {
			return f, true
		}
	}
	return world.Fact{}, false
}

func (r *recorder) hasRelation(from, typ, to string) bool {
	for _, x := range r.relations {
		if x.FromRef == from && x.Type == typ && x.ToRef == to {
			return true
		}
	}
	return false
}

func (r *recorder) event(typ string) (eventing.Envelope, bool) {
	for _, e := range r.events {
		if e.Type == typ {
			return e, true
		}
	}
	return eventing.Envelope{}, false
}

func notFound(w http.ResponseWriter) { http.Error(w, `{"message":"not found"}`, http.StatusNotFound) }

// ---------------- assembly and hard limits ----------------

func fullConfig(url string) Config {
	h := func() capability.HTTPConfig { return capability.HTTPConfig{URL: url} }
	f := false
	return Config{Tenant: "acme",
		Prometheus:   &PrometheusConfig{HTTPConfig: h(), Queries: map[string]string{"error_rate": `sum(rate(x{service="${name}"}[5m]))`}},
		Alertmanager: &AlertmanagerConfig{},
		Kubernetes:   &KubernetesConfig{HTTPConfig: h(), Namespaces: []string{"prod"}, DryRun: &f},
		Traffic:      &TrafficConfig{MaxStep: 50},
		OTel:         &OTelConfig{HTTPConfig: h()},
		GitOps:       &GitOpsConfig{HTTPConfig: h(), Owner: "acme", Repo: "deploy", Paths: []string{"apps/", "monitoring/"}, ArgoCD: &ArgoCDConfig{}},
		CMDB:         &CMDBConfig{HTTPConfig: h()},
		Ticket:       &TicketConfig{HTTPConfig: h(), Projects: []string{"OPS"}},
		AIInfra: &AIInfraConfig{Prometheus: h(), GPU: &GPUConfig{}, Training: &TrainingConfig{},
			Inference: []InferenceService{{Ref: "deployment://prod/llama", Engine: "vllm", Selector: `namespace="prod",model_name="llama"`, TTFTSLO: 1.5}}},
		RiskFloor:     map[string]domain.RiskClass{"deployment.scale": domain.RiskR2},
		Authoritative: []string{"cmdb"},
		Disabled:      []string{"node.drain"},
	}
}

func TestBuildEnforcesDomainLimits(t *testing.T) {
	set, err := Build(fullConfig("http://127.0.0.1:1"), capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	risks := map[string]domain.RiskClass{}
	for _, a := range set.Actions() {
		risks[a.Descriptor().Name] = a.Descriptor().Risk
	}
	for _, want := range []string{"deployment.rollback", "deployment.scale", "node.cordon", "traffic.shift", "gitops.propose_change",
		"gitops.revert", "cmdb.propose_update", "ticket.create", "ticket.comment", "training.suspend"} {
		if _, ok := risks[want]; !ok {
			t.Errorf("action %s missing (have %v)", want, risks)
		}
	}
	if _, ok := risks["node.drain"]; ok {
		t.Error("disabled action node.drain registered")
	}
	if risks["deployment.scale"] != domain.RiskR2 {
		t.Errorf("risk floor not applied: %s", risks["deployment.scale"])
	}
	for _, tl := range set.Tools() {
		if tl.Descriptor().Risk != domain.RiskR0 {
			t.Errorf("tool %s is %s", tl.Descriptor().Name, tl.Descriptor().Risk)
		}
	}
	srcs := strings.Join(set.Sources(), ",")
	for _, s := range []string{"alertmanager", "kubernetes", "argocd", "jira"} {
		if !strings.Contains(srcs, s) {
			t.Errorf("source %s missing: %s", s, srcs)
		}
	}
	tpls := map[string]bool{}
	for _, tp := range set.WorkTemplates() {
		tpls[tp.ID] = true
	}
	if !tpls["inference-latency"] || !tpls["monitoring-config"] {
		t.Errorf("templates: %v", tpls)
	}
	groups := map[string][]string{}
	for _, r := range set.EventRules() {
		groups[r.GroupName()] = append(groups[r.GroupName()], r.Name)
	}
	if len(groups["service-health"]) < 4 {
		t.Errorf("service-health group: %v", groups)
	}
	if _, ok := set.MetricSources()["prometheus_rules"]; !ok {
		t.Error("prometheus_rules metric source missing")
	}

	if _, err := Build(Config{}, capability.Options{}); err == nil {
		t.Error("empty config accepted")
	}
	bad := fullConfig("http://127.0.0.1:1")
	bad.Kubernetes = nil
	bad.AIInfra.Training = nil
	if _, err := Build(bad, capability.Options{}); err == nil || !strings.Contains(err.Error(), "traffic requires") {
		t.Errorf("traffic without kubernetes: %v", err)
	}
	r3 := fullConfig("http://127.0.0.1:1")
	r3.RiskFloor = map[string]domain.RiskClass{"traffic.shift": domain.RiskR3}
	if _, err := Build(r3, capability.Options{}); err == nil {
		t.Error("R3 floor accepted")
	}
}

// ---------------- Kubernetes ----------------

type fakeKube struct {
	mu      sync.Mutex
	image   string
	anno    map[string]string
	gen     int64
	patches []string
	queries []string
}

func (k *fakeKube) handler(w http.ResponseWriter, r *http.Request) {
	k.mu.Lock()
	defer k.mu.Unlock()
	list := func(items ...any) { writeJSON(w, map[string]any{"items": items, "metadata": map[string]string{}}) }
	dep := func() map[string]any {
		return map[string]any{"metadata": map[string]any{"name": "checkout", "namespace": "prod", "generation": k.gen,
			"annotations": k.anno},
			"spec": map[string]any{"replicas": 3, "template": map[string]any{"metadata": map[string]any{"labels": map[string]string{"app": "checkout"}},
				"spec": map[string]any{"containers": []map[string]string{{"name": "app", "image": k.image}}}}},
			"status": map[string]any{"readyReplicas": 3, "observedGeneration": k.gen}}
	}
	switch {
	case r.URL.Path == "/api/v1/nodes":
		list(map[string]any{"metadata": map[string]any{"name": "gpu-node-1"}, "status": map[string]any{
			"conditions": []map[string]any{{"type": "Ready", "status": "True", "lastTransitionTime": "2026-11-01T00:00:00Z"}}}})
	case r.URL.Path == "/apis/apps/v1/namespaces/prod/deployments":
		list(dep())
	case r.URL.Path == "/apis/apps/v1/namespaces/prod/replicasets":
		rs := func(name, rev, img string) any {
			return map[string]any{"metadata": map[string]any{"name": name, "annotations": map[string]string{revisionAnno: rev},
				"ownerReferences": []map[string]any{{"kind": "Deployment", "name": "checkout", "controller": true}}},
				"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []map[string]string{{"name": "app", "image": img}}}}}}
		}
		list(rs("checkout-v1", "1", "checkout:v1"), rs("checkout-v2", "2", "checkout:v2"))
	case r.URL.Path == "/api/v1/namespaces/prod/pods":
		list(map[string]any{"metadata": map[string]any{"name": "checkout-v2-abc", "namespace": "prod",
			"ownerReferences": []map[string]any{{"kind": "ReplicaSet", "name": "checkout-v2", "controller": true}}},
			"spec":   map[string]any{"nodeName": "gpu-node-1", "containers": []map[string]string{{"name": "app", "image": "checkout:v2"}}},
			"status": map[string]any{"phase": "Running", "containerStatuses": []map[string]any{{"name": "app", "ready": true, "restartCount": 2}}}})
	case r.URL.Path == "/apis/apps/v1/namespaces/prod/deployments/checkout" && r.Method == http.MethodGet:
		writeJSON(w, dep())
	case r.URL.Path == "/apis/apps/v1/namespaces/prod/deployments/checkout" && r.Method == http.MethodPatch:
		b, _ := io.ReadAll(r.Body)
		k.patches = append(k.patches, string(b))
		k.queries = append(k.queries, r.URL.RawQuery)
		var p struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Spec struct {
						Containers []container `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		_ = json.Unmarshal(b, &p)
		for key, v := range p.Metadata.Annotations {
			k.anno[key] = v
		}
		if len(p.Spec.Template.Spec.Containers) > 0 {
			k.image = p.Spec.Template.Spec.Containers[0].Image
		}
		k.gen++
		writeJSON(w, dep())
	default:
		notFound(w)
	}
}

func TestKubernetesTopologyAndRollback(t *testing.T) {
	fk := &fakeKube{image: "checkout:v2", gen: 7, anno: map[string]string{revisionAnno: "2"}}
	srv := serve(t, fk.handler)
	f := false
	set, err := Build(Config{Tenant: "acme", Kubernetes: &KubernetesConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL},
		Namespaces: []string{"prod"}, DryRun: &f}}, capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	if err := set.CollectOnce(ctx, rec.env(), "kubernetes", "topology"); err != nil {
		t.Fatal(err)
	}
	if f, ok := rec.fact("pod://prod/checkout-v2-abc", "restarts"); !ok || f.Value != "2" || f.TenantID != "acme" || f.Source != "kubernetes" {
		t.Fatalf("pod fact: %+v %v", f, ok)
	}
	if !rec.hasRelation("deployment://prod/checkout", "owns", "pod://prod/checkout-v2-abc") ||
		!rec.hasRelation("pod://prod/checkout-v2-abc", "runs_on", "node://gpu-node-1") {
		t.Fatalf("relations: %+v", rec.relations)
	}

	var rb action.Executor
	for _, a := range set.Actions() {
		if a.Descriptor().Name == "deployment.rollback" {
			rb = a
		}
	}
	p := plan("deployment.rollback", "deployment://prod/checkout", map[string]any{})
	pf, err := rb.Preflight(ctx, p)
	if err != nil || !pf.Allowed {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	if res, err := rb.Preflight(ctx, plan("deployment.rollback", "deployment://staging/checkout", nil)); err != nil || res.Allowed {
		t.Fatalf("namespace outside the allowlist allowed: %+v", res)
	}
	out, err := rb.Execute(ctx, p, "effect-1")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), `"to_image":"checkout:v1"`) || fk.image != "checkout:v1" {
		t.Fatalf("rollback: %s image=%s", out, fk.image)
	}
	if strings.Contains(fk.queries[0], "dryRun") {
		t.Fatalf("dry-run sent with dry_run=false: %s", fk.queries[0])
	}
	ins, err := rb.Inspect(ctx, p)
	if err != nil || ins.Applied == nil || !*ins.Applied {
		t.Fatalf("inspect: %+v %v", ins, err)
	}
	// A retry after the effect: refused as already applied, nothing patched.
	if _, err := rb.Execute(ctx, p, "effect-1"); !errors.Is(err, action.ErrNotApplied) || len(fk.patches) != 1 {
		t.Fatalf("retry: %v patches=%d", err, len(fk.patches))
	}
}

// ---------------- GitOps ----------------

type fakeGitHub struct {
	mu       sync.Mutex
	files    map[string]map[string]string // ref -> path -> content
	branches map[string]bool
	pulls    []map[string]any
	puts     int
}

func newFakeGitHub() *fakeGitHub {
	return &fakeGitHub{branches: map[string]bool{"main": true}, files: map[string]map[string]string{
		"main":   {"apps/checkout/values.yaml": "image: v2\n"},
		"parent": {"apps/checkout/values.yaml": "image: v1\n"},
	}}
}

func (g *fakeGitHub) handler(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	defer g.mu.Unlock()
	const base = "/repos/acme/deploy"
	p := strings.TrimPrefix(r.URL.Path, base)
	switch {
	case p == "/commits/abcdef1234567" && r.Method == http.MethodGet:
		writeJSON(w, map[string]any{"sha": "abcdef1234567", "commit": map[string]any{"message": "bump checkout\n\nbody"},
			"parents": []map[string]string{{"sha": "parent"}},
			"files":   []map[string]string{{"filename": "apps/checkout/values.yaml", "status": "modified"}}})
	case p == "/git/ref/heads/main":
		writeJSON(w, map[string]any{"object": map[string]string{"sha": "main"}})
	case p == "/git/refs" && r.Method == http.MethodPost:
		var b struct{ Ref, SHA string }
		_ = json.NewDecoder(r.Body).Decode(&b)
		br := strings.TrimPrefix(b.Ref, "refs/heads/")
		if g.branches[br] {
			http.Error(w, `{"message":"Reference already exists"}`, http.StatusUnprocessableEntity)
			return
		}
		g.branches[br] = true
		g.files[br] = map[string]string{}
		for k, v := range g.files[b.SHA] {
			g.files[br][k] = v
		}
		writeJSON(w, map[string]any{})
	case strings.HasPrefix(p, "/contents/"):
		path := strings.TrimPrefix(p, "/contents/")
		ref := r.URL.Query().Get("ref")
		switch r.Method {
		case http.MethodGet:
			c, ok := g.files[ref][path]
			if !ok {
				notFound(w)
				return
			}
			writeJSON(w, map[string]string{"sha": "blob-" + path, "encoding": "base64", "content": base64.StdEncoding.EncodeToString([]byte(c))})
		case http.MethodPut:
			var b struct{ Content, Branch string }
			_ = json.NewDecoder(r.Body).Decode(&b)
			d, _ := base64.StdEncoding.DecodeString(b.Content)
			g.files[b.Branch][path] = string(d)
			g.puts++
			writeJSON(w, map[string]any{})
		}
	case p == "/pulls" && r.Method == http.MethodGet:
		head := r.URL.Query().Get("head")
		out := []map[string]any{}
		for _, pr := range g.pulls {
			if "acme:"+pr["head"].(map[string]any)["ref"].(string) == head {
				out = append(out, pr)
			}
		}
		writeJSON(w, out)
	case p == "/pulls" && r.Method == http.MethodPost:
		var b struct{ Head, Title string }
		_ = json.NewDecoder(r.Body).Decode(&b)
		pr := map[string]any{"number": len(g.pulls) + 1, "html_url": "https://github.example/pr", "state": "open",
			"head": map[string]any{"ref": b.Head}, "title": b.Title}
		g.pulls = append(g.pulls, pr)
		writeJSON(w, pr)
	default:
		notFound(w)
	}
}

func TestGitOpsProposeChangeIsIdempotent(t *testing.T) {
	gh := newFakeGitHub()
	srv := serve(t, gh.handler)
	g, err := newGitOps(GitOpsConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}, Owner: "acme", Repo: "deploy",
		Paths: []string{"monitoring/"}})
	if err != nil {
		t.Fatal(err)
	}
	a := &proposeChange{g}
	rule := "groups:\n- name: checkout\n  rules:\n  - alert: CheckoutErrors\n    expr: rate(x[5m]) > 0.05\n"
	p := plan("gitops.propose_change", "git://acme/deploy/monitoring/checkout.yaml", map[string]string{"content": rule, "title": "Add checkout alerts"})
	if pf, err := a.Preflight(ctx, p); err != nil || !pf.Allowed {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	for _, bad := range []action.Plan{
		plan("gitops.propose_change", "git://acme/deploy/apps/checkout/values.yaml", map[string]string{"content": "x: 1", "title": "t"}),
		plan("gitops.propose_change", "git://acme/deploy/monitoring/../apps/x.yaml", map[string]string{"content": "x: 1", "title": "t"}),
		plan("gitops.propose_change", "git://acme/deploy/monitoring/bad.yaml", map[string]string{"content": "a: [", "title": "t"}),
	} {
		if pf, _ := a.Preflight(ctx, bad); pf.Allowed {
			t.Errorf("preflight allowed %s %s", bad.TargetRef, bad.Args)
		}
	}
	for i := 0; i < 2; i++ { // the retry finds its branch, file and PR
		if _, err := a.Execute(ctx, p, "effect-1"); err != nil {
			t.Fatal(err)
		}
	}
	if len(gh.pulls) != 1 || gh.puts != 1 {
		t.Fatalf("pulls=%d puts=%d", len(gh.pulls), gh.puts)
	}
	if got := gh.files[branchFor("effect-1")]["monitoring/checkout.yaml"]; got != rule {
		t.Fatalf("file on branch: %q", got)
	}
	ins, err := a.Inspect(ctx, p)
	if err != nil || ins.Applied == nil || !*ins.Applied {
		t.Fatalf("inspect: %+v %v", ins, err)
	}
	other := p
	other.EffectKey = "never-ran"
	if ins, _ := a.Inspect(ctx, other); ins.Applied == nil || *ins.Applied {
		t.Fatalf("inspect of an unknown key: %+v", ins)
	}
	if tp := MonitoringConfigTemplate(); tp.Normalize() != nil {
		t.Fatal("monitoring-config template invalid")
	}
}

func TestGitOpsRevertAndArgoEvents(t *testing.T) {
	gh := newFakeGitHub()
	srv := serve(t, gh.handler)
	g, err := newGitOps(GitOpsConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}, Owner: "acme", Repo: "deploy",
		Paths: []string{"apps/"}, ArgoCD: &ArgoCDConfig{}})
	if err != nil {
		t.Fatal(err)
	}
	a := &gitRevert{g}
	p := plan("gitops.revert", "git://acme/deploy", map[string]string{"commit": "abcdef1234567", "reason": "TTFT regression"})
	if pf, err := a.Preflight(ctx, p); err != nil || !pf.Allowed {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	if _, err := a.Execute(ctx, p, "effect-r"); err != nil {
		t.Fatal(err)
	}
	if got := gh.files[branchFor("effect-r")]["apps/checkout/values.yaml"]; got != "image: v1\n" {
		t.Fatalf("revert content: %q", got)
	}
	if len(gh.pulls) != 1 || !strings.HasPrefix(gh.pulls[0]["title"].(string), "Revert \"bump checkout\"") {
		t.Fatalf("pulls: %+v", gh.pulls)
	}

	now := time.Date(2026, 11, 9, 10, 0, 0, 0, time.UTC)
	body := `{"app":"checkout","destination_namespace":"prod","revision":"abcdef1","phase":"Succeeded","health_status":"Healthy","finished_at":"2026-11-09T09:58:00Z"}`
	evs, err := g.normalizeArgo(ctx, "acme", nil, []byte(body), now)
	if err != nil || len(evs) != 1 {
		t.Fatalf("argo: %v %v", evs, err)
	}
	if e := evs[0]; e.Type != "change.deployed" || e.SubjectRef != "service://prod/checkout" || e.OccurredAt.Minute() != 58 {
		t.Fatalf("argo event: %+v", e)
	}
	if _, err := g.normalizeArgo(ctx, "acme", nil, []byte(`{"app":"../x"}`), now); err == nil {
		t.Fatal("invalid app accepted")
	}
}

// ---------------- Traffic ----------------

func TestTrafficShift(t *testing.T) {
	var mu sync.Mutex
	vs := map[string]any{"metadata": map[string]any{"name": "llama", "namespace": "prod", "generation": 3, "resourceVersion": "100"},
		"spec": map[string]any{"hosts": []string{"llama"}, "http": []any{map[string]any{"name": "primary", "route": []any{
			map[string]any{"destination": map[string]any{"host": "llama", "subset": "stable"}, "weight": 90},
			map[string]any{"destination": map[string]any{"host": "llama", "subset": "canary"}, "weight": 10}}}}}}
	var puts int
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path != vsPath("prod", "llama") {
			notFound(w)
			return
		}
		if r.Method == http.MethodPut {
			puts++
			_ = json.NewDecoder(r.Body).Decode(&vs)
		}
		writeJSON(w, vs)
	})
	f := false
	k, err := newKube(KubernetesConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}, Namespaces: []string{"prod"}, DryRun: &f})
	if err != nil {
		t.Fatal(err)
	}
	a := &trafficShift{newTraffic(TrafficConfig{MaxStep: 50}, k)}
	target := "virtualservice://prod/llama"
	for _, bad := range []map[string]any{
		{"weights": map[string]int{"stable": 50, "canary": 40}}, // sum 90
		{"weights": map[string]int{"stable": 100}},              // missing destination
		{"weights": map[string]int{"stable": 30, "canary": 70}}, // +60 > max_step
		{"weights": map[string]int{"stable": 50, "blue": 50}},   // unknown destination
		{"route": "nope", "weights": map[string]int{"stable": 100, "canary": 0}},
	} {
		if pf, _ := a.Preflight(ctx, plan("traffic.shift", target, bad)); pf.Allowed {
			t.Errorf("allowed %v", bad)
		}
	}
	p := plan("traffic.shift", target, map[string]any{"weights": map[string]int{"stable": 100, "canary": 0}})
	pf, err := a.Preflight(ctx, p)
	if err != nil || !pf.Allowed || pf.TargetVersion != "3" {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	p.EffectKey = "effect-t"
	if _, err := a.Execute(ctx, p, "effect-t"); err != nil {
		t.Fatal(err)
	}
	routes, dests := virtualService{raw: vs}.httpRoutes()
	if len(routes) != 1 || dests[0][0].Weight != 100 || dests[0][1].Weight != 0 || puts != 1 {
		t.Fatalf("weights after shift: %+v puts=%d", dests, puts)
	}
	if ins, _ := a.Inspect(ctx, p); ins.Applied == nil || !*ins.Applied {
		t.Fatalf("inspect: %+v", ins)
	}
	if _, err := a.Execute(ctx, p, "effect-t"); err != nil || puts != 1 {
		t.Fatalf("retry wrote again: %v puts=%d", err, puts)
	}
}

// ---------------- CMDB ----------------

func TestCMDBFactsAndChangeRequests(t *testing.T) {
	var mu sync.Mutex
	var crs []map[string]any
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/api/v1/services":
			writeJSON(w, map[string]any{"items": []map[string]any{{"namespace": "prod", "name": "checkout", "owner": "alice",
				"team": "payments", "tier": "1", "oncall": "payments-primary", "depends_on": []string{"prod/payments-db"}}}})
		case r.URL.Path == "/api/v1/services/prod/checkout":
			writeJSON(w, map[string]any{"namespace": "prod", "name": "checkout", "team": "payments"})
		case r.URL.Path == "/api/v1/change-requests" && r.Method == http.MethodPost:
			var b map[string]any
			_ = json.NewDecoder(r.Body).Decode(&b)
			b["id"] = "CR-1"
			crs = append(crs, b)
			writeJSON(w, b)
		case r.URL.Path == "/api/v1/change-requests":
			out := []map[string]any{}
			for _, c := range crs {
				if c["external_id"] == r.URL.Query().Get("external_id") {
					out = append(out, map[string]any{"id": c["id"], "status": "pending"})
				}
			}
			writeJSON(w, out)
		default:
			notFound(w)
		}
	})
	for _, auth := range []bool{true, false} {
		cfg := Config{Tenant: "acme", CMDB: &CMDBConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}}}
		if auth {
			cfg.Authoritative = []string{"cmdb"}
		}
		set, err := Build(cfg, capability.Options{})
		if err != nil {
			t.Fatal(err)
		}
		rec := &recorder{}
		if err := set.CollectOnce(ctx, rec.env(), "cmdb", "services"); err != nil {
			t.Fatal(err)
		}
		f, ok := rec.fact("service://prod/checkout", "oncall")
		want := world.TrustFirstParty
		if auth {
			want = world.TrustAuthoritative
		}
		if !ok || f.Value != "payments-primary" || f.TrustTier != want {
			t.Fatalf("authoritative=%v: fact %+v", auth, f)
		}
		if !rec.hasRelation("service://prod/checkout", "depends_on", "service://prod/payments-db") {
			t.Fatalf("relations: %+v", rec.relations)
		}
	}
	c, _ := newCMDB(CMDBConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}})
	a := &cmdbUpdate{c}
	if pf, _ := a.Preflight(ctx, plan("cmdb.propose_update", "service://prod/checkout", map[string]any{"changes": map[string]string{"secret": "x"}, "reason": "r"})); pf.Allowed {
		t.Fatal("unknown field allowed")
	}
	p := plan("cmdb.propose_update", "service://prod/checkout", map[string]any{"changes": map[string]string{"oncall": "payments-secondary"}, "reason": "rotation"})
	if pf, err := a.Preflight(ctx, p); err != nil || !pf.Allowed {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	if _, err := a.Execute(ctx, p, "effect-c"); err != nil {
		t.Fatal(err)
	}
	p.EffectKey = "effect-c"
	if ins, err := a.Inspect(ctx, p); err != nil || !*ins.Applied {
		t.Fatalf("inspect: %+v %v", ins, err)
	}
}

// ---------------- Tickets ----------------

func TestTicketIdempotentCreateCommentAndWebhook(t *testing.T) {
	var mu sync.Mutex
	type issue struct {
		key      string
		labels   []string
		comments []string
	}
	var issues []*issue
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.URL.Path == "/rest/api/2/search":
			jql := r.URL.Query().Get("jql")
			out := []map[string]string{}
			for _, is := range issues {
				for _, l := range is.labels {
					if strings.Contains(jql, `"`+l+`"`) {
						out = append(out, map[string]string{"key": is.key})
					}
				}
			}
			writeJSON(w, map[string]any{"issues": out})
		case r.URL.Path == "/rest/api/2/issue" && r.Method == http.MethodPost:
			var b struct {
				Fields struct{ Labels []string } `json:"fields"`
			}
			_ = json.NewDecoder(r.Body).Decode(&b)
			is := &issue{key: "OPS-" + string(rune('1'+len(issues))), labels: b.Fields.Labels}
			issues = append(issues, is)
			writeJSON(w, map[string]string{"key": is.key})
		case strings.HasSuffix(r.URL.Path, "/comment"):
			key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/rest/api/2/issue/"), "/comment")
			var is *issue
			for _, x := range issues {
				if x.key == key {
					is = x
				}
			}
			if is == nil {
				notFound(w)
				return
			}
			if r.Method == http.MethodPost {
				var b struct{ Body string }
				_ = json.NewDecoder(r.Body).Decode(&b)
				is.comments = append(is.comments, b.Body)
				writeJSON(w, map[string]string{"id": "c1"})
				return
			}
			out := []map[string]string{}
			for _, c := range is.comments {
				out = append(out, map[string]string{"id": "c1", "body": c})
			}
			writeJSON(w, map[string]any{"comments": out})
		case strings.HasPrefix(r.URL.Path, "/rest/api/2/issue/OPS-"):
			writeJSON(w, map[string]any{"key": strings.TrimPrefix(r.URL.Path, "/rest/api/2/issue/"), "fields": map[string]any{"summary": "s"}})
		default:
			notFound(w)
		}
	})
	tk, err := newTicket(TicketConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}, Projects: []string{"OPS"}})
	if err != nil {
		t.Fatal(err)
	}
	create := &ticketCreate{tk}
	if pf, _ := create.Preflight(ctx, plan("ticket.create", "ticket://SEC", map[string]string{"summary": "s", "description": "d"})); pf.Allowed {
		t.Fatal("project outside the allowlist allowed")
	}
	p := plan("ticket.create", "ticket://OPS", map[string]any{"summary": "Raise llama replica floor", "description": "TTFT follow-up",
		"services": []string{"prod/llama"}})
	for i := 0; i < 2; i++ {
		if _, err := create.Execute(ctx, p, "effect-k"); err != nil {
			t.Fatal(err)
		}
	}
	if len(issues) != 1 || !strings.Contains(strings.Join(issues[0].labels, ","), "service:prod.llama") {
		t.Fatalf("issues: %+v", issues)
	}
	p.EffectKey = "effect-k"
	if ins, _ := create.Inspect(ctx, p); !*ins.Applied {
		t.Fatal("inspect create")
	}
	cm := &ticketComment{tk}
	cp := plan("ticket.comment", "ticket://OPS/OPS-1", map[string]string{"body": "Mitigated by scaling to 6 replicas."})
	if pf, err := cm.Preflight(ctx, cp); err != nil || !pf.Allowed {
		t.Fatalf("comment preflight: %+v %v", pf, err)
	}
	for i := 0; i < 2; i++ {
		if _, err := cm.Execute(ctx, cp, "effect-m"); err != nil {
			t.Fatal(err)
		}
	}
	if len(issues[0].comments) != 1 {
		t.Fatalf("comments: %v", issues[0].comments)
	}

	hook := `{"timestamp":1762682400000,"webhookEvent":"jira:issue_updated","issue":{"key":"OPS-1","fields":{"summary":"x","labels":["service:prod.llama","other"],"status":{"name":"In Progress"}}}}`
	evs, err := tk.normalize(ctx, "acme", nil, []byte(hook), time.Now())
	if err != nil || len(evs) != 1 || evs[0].SubjectRef != "service://prod/llama" || evs[0].Type != "ticket.updated" {
		t.Fatalf("jira webhook: %+v %v", evs, err)
	}
}

// ---------------- Prometheus / AI infra ----------------

// fakeProm answers instant and range queries by matching a substring of
// the expression.
func fakeProm(t *testing.T, answers map[string][]map[string]any, rules []map[string]any) *httptest.Server {
	return serve(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		switch r.URL.Path {
		case "/api/v1/query", "/api/v1/query_range":
			result := []map[string]any{}
			for sub, series := range answers {
				if strings.Contains(q, sub) {
					result = series
					break
				}
			}
			typ := "vector"
			if r.URL.Path == "/api/v1/query_range" {
				typ = "matrix"
				for _, s := range result {
					if v, ok := s["value"]; ok {
						s["values"] = []any{v, v, v}
					}
				}
			}
			writeJSON(w, map[string]any{"status": "success", "data": map[string]any{"resultType": typ, "result": result}})
		case "/api/v1/rules":
			writeJSON(w, map[string]any{"status": "success", "data": map[string]any{"groups": rules}})
		default:
			notFound(w)
		}
	})
}

func vec(v string, labels map[string]string) map[string]any {
	return map[string]any{"metric": labels, "value": []any{1762682400.0, v}}
}

func TestInferenceTTFTBreachAndMetricSource(t *testing.T) {
	srv := fakeProm(t, map[string][]map[string]any{
		"time_to_first_token":  {vec("2.4", nil)},
		"num_requests_waiting": {vec("40", nil)},
		"num_requests_running": {vec("64", nil)},
		"gpu_cache_usage_perc": {vec("0.97", nil)},
		"generation_tokens":    {vec("1800", nil)},
	}, nil)
	set, err := Build(Config{Tenant: "acme", AIInfra: &AIInfraConfig{Prometheus: capability.HTTPConfig{URL: srv.URL},
		Inference: []InferenceService{{Ref: "deployment://prod/llama", Engine: "vllm", Selector: `namespace="prod"`, TTFTSLO: 1.5}}}},
		capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	if err := set.CollectOnce(ctx, rec.env(), "inference", "serving"); err != nil {
		t.Fatal(err)
	}
	e, ok := rec.event("inference.ttft_breach")
	if !ok || e.SubjectRef != "deployment://prod/llama" || e.Labels["cause"] != "kv_cache_pressure" || e.TenantID != "acme" {
		t.Fatalf("breach event: %+v", e)
	}
	if f, _ := rec.fact("deployment://prod/llama", "ttft_slo"); f.Value != "breach" || f.TrustTier != world.TrustTelemetry {
		t.Fatalf("slo fact: %+v", f)
	}
	src := set.MetricSources()["inference"]
	now := time.Now()
	res, err := src.QueryRange(ctx, outcome.MetricQuery{Query: "ttft_p95", TargetRef: "deployment://prod/llama", Start: now.Add(-10 * time.Minute), End: now})
	if err != nil || len(res.Samples) != 3 || res.Samples[0].V != 2.4 {
		t.Fatalf("query range: %+v %v", res, err)
	}
	if _, err := src.QueryRange(ctx, outcome.MetricQuery{Query: "rm -rf", TargetRef: "deployment://prod/llama", Start: now.Add(-time.Minute), End: now}); err == nil {
		t.Fatal("unknown query accepted")
	}
	if _, err := src.QueryRange(ctx, outcome.MetricQuery{Query: "ttft_p95", TargetRef: "deployment://prod/other", Start: now.Add(-time.Minute), End: now}); err == nil {
		t.Fatal("unconfigured service accepted")
	}
	if spec := TTFTSuccess("deployment://prod/llama", 1.5); spec.Normalize() != nil {
		t.Fatalf("ttft success spec invalid: %v", spec.Normalize())
	}
	if tp := InferenceLatencyTemplate(); tp.Normalize() != nil {
		t.Fatal("inference-latency template invalid")
	}
	// Diagnosis classes.
	q, kv, low := 40.0, 0.97, 0.2
	svc := InferenceService{QueueLimit: 16}
	if d := diagnose(svc, snapshot{Queue: &q, KVCache: &low}, nil); d.Cause != "capacity" {
		t.Errorf("capacity: %+v", d)
	}
	if d := diagnose(svc, snapshot{Queue: &low, KVCache: &low}, nil); d.Cause != "prefill_bound" {
		t.Errorf("prefill: %+v", d)
	}
	if d := diagnose(svc, snapshot{Queue: &q, KVCache: &kv}, []*gpuRow{{Host: "n1", Index: "0", XID: 79}}); d.Cause != "gpu_fault" {
		t.Errorf("gpu fault: %+v", d)
	}
}

func TestGPUFaultsFromDCGM(t *testing.T) {
	lbl := func(gpu string) map[string]string {
		return map[string]string{"Hostname": "gpu-node-1", "gpu": gpu, "UUID": "GPU-" + gpu, "modelName": "H100", "namespace": "prod", "pod": "llama-0"}
	}
	srv := fakeProm(t, map[string][]map[string]any{
		"DCGM_FI_DEV_GPU_UTIL":   {vec("97", lbl("0")), vec("5", lbl("1"))},
		"DCGM_FI_DEV_FB_USED":    {vec("70000", lbl("0"))},
		"DCGM_FI_DEV_GPU_TEMP":   {vec("65", lbl("0")), vec("91", lbl("1"))},
		"DCGM_FI_DEV_XID_ERRORS": {vec("79", lbl("0")), vec("0", lbl("1"))},
	}, nil)
	set, err := Build(Config{Tenant: "acme", AIInfra: &AIInfraConfig{Prometheus: capability.HTTPConfig{URL: srv.URL}, GPU: &GPUConfig{}}},
		capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	if err := set.CollectOnce(ctx, rec.env(), "gpu", "dcgm"); err != nil {
		t.Fatal(err)
	}
	if f, _ := rec.fact("gpu://gpu-node-1/0", "health"); f.Value != "faulted" {
		t.Fatalf("gpu0 health: %+v", f)
	}
	if f, _ := rec.fact("gpu://gpu-node-1/1", "health"); f.Value != "overheating" {
		t.Fatalf("gpu1 health: %+v", f)
	}
	if e, ok := rec.event("gpu.xid_error"); !ok || e.SubjectRef != "node://gpu-node-1" || e.Labels["xid"] != "79" {
		t.Fatalf("xid event: %+v", e)
	}
	if _, ok := rec.event("gpu.overheat"); !ok {
		t.Fatal("no overheat event")
	}
	if !rec.hasRelation("node://gpu-node-1", "has_gpu", "gpu://gpu-node-1/0") || !rec.hasRelation("pod://prod/llama-0", "uses_gpu", "gpu://gpu-node-1/0") {
		t.Fatalf("relations: %+v", rec.relations)
	}
}

func TestPrometheusRuleLoadedSource(t *testing.T) {
	srv := fakeProm(t, nil, []map[string]any{{"name": "checkout", "file": "checkout.yaml", "rules": []map[string]any{
		{"name": "CheckoutErrors", "type": "alerting", "health": "ok"},
		{"name": "CheckoutBroken", "type": "alerting", "health": "err", "lastError": "parse error"}}}})
	p, err := newPrometheus(PrometheusConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	src := ruleSource{p}
	now := time.Now()
	for q, want := range map[string]float64{"checkout/CheckoutErrors": 1, "checkout/CheckoutBroken": 0, "checkout/Missing": 0} {
		res, err := src.QueryRange(ctx, outcome.MetricQuery{Query: q, Start: now.Add(-time.Minute), End: now})
		if err != nil || len(res.Samples) != 1 || res.Samples[0].V != want {
			t.Errorf("%s: %+v %v", q, res, err)
		}
	}
	if _, err := src.QueryRange(ctx, outcome.MetricQuery{Query: "nogroup", End: now}); err == nil {
		t.Error("malformed query accepted")
	}
	if spec := RuleLoadedSuccess("checkout", "CheckoutErrors"); spec.Normalize() != nil {
		t.Fatalf("rule-loaded spec invalid: %v", spec.Normalize())
	}
}

// ---------------- training ----------------

func TestTrainingFailureAndSuspend(t *testing.T) {
	var mu sync.Mutex
	suspended := false
	anno := map[string]string{}
	job := func(name, phase string) map[string]any {
		return map[string]any{"metadata": map[string]any{"name": name, "namespace": "ml", "uid": "uid-" + name, "generation": 2,
			"resourceVersion": "9", "annotations": anno},
			"spec": map[string]any{"runPolicy": map[string]any{"suspend": suspended}},
			"status": map[string]any{"conditions": []map[string]any{{"type": "Created", "status": "True", "lastTransitionTime": "2026-11-09T08:00:00Z"},
				{"type": phase, "status": "True", "lastTransitionTime": "2026-11-09T09:00:00Z", "message": "replica master-0 exited 137"}},
				"replicaStatuses": map[string]any{"Worker": map[string]int{"failed": 1}}}}
	}
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/apis/kubeflow.org/v1/namespaces/ml/pytorchjobs":
			writeJSON(w, map[string]any{"items": []any{job("pretrain", "Running"), job("finetune", "Failed")}, "metadata": map[string]any{}})
		case "/apis/kubeflow.org/v1/namespaces/ml/pytorchjobs/pretrain":
			if r.Method == http.MethodPatch {
				var b struct {
					Metadata struct{ Annotations map[string]string } `json:"metadata"`
					Spec     struct {
						RunPolicy struct{ Suspend bool } `json:"runPolicy"`
					} `json:"spec"`
				}
				_ = json.NewDecoder(r.Body).Decode(&b)
				suspended, anno = b.Spec.RunPolicy.Suspend, b.Metadata.Annotations
			}
			writeJSON(w, job("pretrain", "Running"))
		default:
			notFound(w)
		}
	})
	f := false
	set, err := Build(Config{Tenant: "acme", Kubernetes: &KubernetesConfig{HTTPConfig: capability.HTTPConfig{URL: srv.URL},
		Namespaces: []string{"ml"}, DryRun: &f, CollectEvery: -1},
		AIInfra: &AIInfraConfig{Prometheus: capability.HTTPConfig{URL: srv.URL}, Training: &TrainingConfig{}}}, capability.Options{})
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	if err := set.CollectOnce(ctx, rec.env(), "training", "jobs"); err != nil {
		t.Fatal(err)
	}
	if e, ok := rec.event("training.failed"); !ok || e.SubjectRef != "trainingjob://ml/finetune" {
		t.Fatalf("failed event: %+v", rec.events)
	}
	if f, _ := rec.fact("trainingjob://ml/pretrain", "phase"); f.Value != "Running" {
		t.Fatalf("phase: %+v", f)
	}
	var a action.Executor
	for _, x := range set.Actions() {
		if x.Descriptor().Name == "training.suspend" {
			a = x
		}
	}
	if a.Descriptor().Risk != domain.RiskR2 {
		t.Fatalf("risk %s", a.Descriptor().Risk)
	}
	p := plan("training.suspend", "trainingjob://ml/pretrain", map[string]bool{"suspend": true})
	if pf, err := a.Preflight(ctx, p); err != nil || !pf.Allowed {
		t.Fatalf("preflight: %+v %v", pf, err)
	}
	if _, err := a.Execute(ctx, p, "effect-s"); err != nil {
		t.Fatal(err)
	}
	if !suspended {
		t.Fatal("not suspended")
	}
	if ins, _ := a.Inspect(ctx, plan("training.suspend", "trainingjob://ml/pretrain", map[string]bool{"suspend": true})); ins.Applied == nil {
		t.Fatal("inspect")
	}
	p.EffectKey = "effect-s"
	if ins, _ := a.Inspect(ctx, p); !*ins.Applied {
		t.Fatal("inspect after suspend")
	}
}
