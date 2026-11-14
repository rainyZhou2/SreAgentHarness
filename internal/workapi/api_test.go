package workapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
	"go-sre-harness/internal/world"
)

type env struct {
	t     *testing.T
	mux   http.Handler
	store *cpsql.Store
}

func newEnv(t *testing.T) *env {
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	cat, _ := orchestration.NewCatalog(nil)
	router, _ := eventing.NewRouter([]eventing.Rule{{Name: "alerts", Match: eventing.Predicate{EventTypes: []string{"alert.*"}},
		Key: "dedup_key"}})
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := &API{Store: s, Service: &orchestration.Service{Store: s, Catalog: cat, Log: quiet},
		Ingestor: &orchestration.Ingestor{Store: s, Router: router, Catalog: cat, Log: quiet},
		World:    &world.Service{Store: s}, Eval: &workeval.Evaluator{Ledger: s}, Log: quiet, Postmortems: &postmortem.Builder{Ledger: s}}
	mux := http.NewServeMux()
	a.Register(mux)
	// Test auth: X-Tenant / X-Roles headers become the principal (the real
	// server's middleware also enforces RouteRoles).
	roles := a.RouteRoles()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := auth.Principal{Tenant: r.Header.Get("X-Tenant"), Subject: r.Header.Get("X-User")}
		for _, x := range strings.Split(r.Header.Get("X-Roles"), ",") {
			p.Roles = append(p.Roles, auth.Role(x))
		}
		_, pattern := mux.Handler(r)
		if need, ok := roles[pattern]; ok && !p.Has(need) {
			writeErr(w, http.StatusForbidden, "forbidden", "missing role")
			return
		}
		mux.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
	return &env{t: t, mux: h, store: s}
}

func (e *env) do(method, path, tenant, roles string, body any, hdr map[string]string) (int, map[string]any, http.Header) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	r := httptest.NewRequest(method, path, rd)
	r.Header.Set("X-Tenant", tenant)
	r.Header.Set("X-User", "u-"+tenant)
	r.Header.Set("X-Roles", roles)
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out, w.Header()
}

func TestWorkAPI(t *testing.T) {
	e := newEnv(t)
	body := map[string]any{"kind": "incident", "title": "checkout 5xx",
		"goal": map[string]any{"intent": "mitigate", "subject_refs": []string{"service://prod/checkout"}}}

	if code, _, _ := e.do("POST", "/v1/works", "t1", "operator", body, nil); code != http.StatusBadRequest {
		t.Fatalf("missing idempotency key: %d", code)
	}
	if code, _, _ := e.do("POST", "/v1/works", "t1", "viewer", body, map[string]string{"Idempotency-Key": "k1"}); code != http.StatusForbidden {
		t.Fatalf("viewer create: %d", code)
	}
	code, w1, hdr := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "k1"})
	if code != http.StatusCreated || hdr.Get("ETag") != `"1"` {
		t.Fatalf("create: %d %v %s", code, w1, hdr.Get("ETag"))
	}
	id := w1["id"].(string)
	code, w2, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "k1"})
	if code != http.StatusOK || w2["id"] != id {
		t.Fatalf("replay: %d", code)
	}
	body["title"] = "other"
	if code, _, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "k1"}); code != http.StatusUnprocessableEntity {
		t.Fatalf("idempotency conflict: %d", code)
	}
	// T18: other tenants cannot see the Work.
	if code, _, _ := e.do("GET", "/v1/works/"+id, "t2", "viewer", nil, nil); code != http.StatusNotFound {
		t.Fatalf("tenant isolation: %d", code)
	}
	if code, _, _ := e.do("GET", "/v1/works/"+id+"/events", "t2", "viewer", nil, nil); code != http.StatusNotFound {
		t.Fatalf("tenant isolation (events): %d", code)
	}
	code, list, _ := e.do("GET", "/v1/works?status=NEW", "t1", "viewer", nil, nil)
	if code != 200 || len(list["items"].([]any)) != 1 {
		t.Fatalf("list: %d %v", code, list)
	}

	// Commands: If-Match, role checks, transitions.
	cmd := func(typ, ifm, roles string, payload any) int {
		h := map[string]string{"Idempotency-Key": typ + ifm}
		if ifm != "" {
			h["If-Match"] = ifm
		}
		code, _, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", roles, map[string]any{"type": typ, "payload": payload}, h)
		return code
	}
	if c := cmd(work.CmdPause, `"9"`, "operator", nil); c != http.StatusPreconditionFailed {
		t.Fatalf("stale If-Match: %d", c)
	}
	if c := cmd(work.CmdConfirm, "", "operator", map[string]any{"verdict": "satisfied"}); c != http.StatusForbidden {
		t.Fatalf("confirm needs approver: %d", c)
	}
	if c := cmd(work.CmdResume, "", "operator", nil); c != http.StatusConflict {
		t.Fatalf("resume a NEW work: %d", c)
	}
	if c := cmd(work.CmdCancel, `"1"`, "operator", nil); c != http.StatusOK {
		t.Fatalf("cancel: %d", c)
	}
	code, got, _ := e.do("GET", "/v1/works/"+id, "t1", "viewer", nil, nil)
	if code != 200 || got["status"] != "CANCELLED" {
		t.Fatalf("after cancel: %v", got)
	}

	// Events: tenant is forced; redelivery is a duplicate.
	ev := map[string]any{"event_id": "a1", "source": "am", "type": "alert.firing", "subject_ref": "service://prod/checkout",
		"dedup_key": "fp"}
	code, r1, _ := e.do("POST", "/v1/events", "t1", "operator", ev, nil)
	if code != http.StatusAccepted || r1["decision"] != "created" {
		t.Fatalf("ingest: %d %v", code, r1)
	}
	code, r2, _ := e.do("POST", "/v1/events", "t1", "operator", ev, nil)
	if code != http.StatusOK || r2["duplicate"] != true {
		t.Fatalf("duplicate: %d %v", code, r2)
	}
	ev["tenant_id"] = "t2"
	if code, _, _ := e.do("POST", "/v1/events", "t1", "operator", ev, nil); code != http.StatusForbidden {
		t.Fatalf("cross-tenant ingest: %d", code)
	}

	// World: facts posted through the API are untrusted.
	fact := map[string]any{"subject_ref": "service://prod/checkout", "predicate": "replicas", "value": "3",
		"trust_tier": "T1"}
	code, f, _ := e.do("POST", "/v1/world/facts", "t1", "operator", fact, nil)
	if code != http.StatusCreated || f["trust_tier"] != "T4" {
		t.Fatalf("assert fact: %d %v", code, f)
	}
	code, fs, _ := e.do("GET", "/v1/world/facts?subject=service://prod/checkout", "t1", "viewer", nil, nil)
	if code != 200 || len(fs["items"].([]any)) != 1 {
		t.Fatalf("facts: %d %v", code, fs)
	}
	code, snap, _ := e.do("POST", "/v1/world/snapshots/query", "t1", "viewer",
		map[string]any{"subjects": []string{"service://prod/checkout"}, "required": []string{"replicas", "image"}}, nil)
	if code != 200 || len(snap["missing"].([]any)) != 1 {
		t.Fatalf("snapshot: %d %v", code, snap)
	}
	// Actions are disabled in this server.
	if code, _, _ := e.do("POST", "/v1/actions/x/approve", "t1", "approver", map[string]any{}, nil); code != http.StatusServiceUnavailable {
		t.Fatalf("disabled actions: %d", code)
	}
}

// §17.4 over the API: replan with an amended goal keeps the goal history;
// assignments and goals are readable per Work.
func TestGoalAmendmentAndAssignments(t *testing.T) {
	e := newEnv(t)
	body := map[string]any{"kind": "incident", "title": "checkout 5xx",
		"goal": map[string]any{"intent": "mitigate", "subject_refs": []string{"service://prod/checkout"}}}
	code, w1, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "g1"})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, w1)
	}
	id := w1["id"].(string)
	amend := map[string]any{"type": "replan", "reason": "widen scope", "payload": map[string]any{
		"goal": map[string]any{"intent": "mitigate", "description": "include payments",
			"subject_refs": []string{"service://prod/checkout", "service://prod/payments"}}}}
	code, w2, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", "operator", amend, map[string]string{"Idempotency-Key": "r1"})
	if code != http.StatusOK || w2["generation"].(float64) != 2 {
		t.Fatalf("replan with goal: %d %v", code, w2)
	}
	code, goals, _ := e.do("GET", "/v1/works/"+id+"/goals", "t1", "viewer", nil, nil)
	items, _ := goals["items"].([]any)
	if code != 200 || len(items) != 2 {
		t.Fatalf("goals: %d %v", code, goals)
	}
	g2 := items[1].(map[string]any)
	if g2["generation"].(float64) != 2 || g2["goal"].(map[string]any)["description"] != "include payments" {
		t.Fatalf("generation 2 goal: %v", g2)
	}
	bad := map[string]any{"type": "replan", "payload": map[string]any{"goal": map[string]any{"intent": ""}}}
	if code, _, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", "operator", bad, map[string]string{"Idempotency-Key": "r2"}); code != http.StatusBadRequest {
		t.Fatalf("invalid amended goal: %d", code)
	}
	code, as, _ := e.do("GET", "/v1/works/"+id+"/assignments", "t1", "viewer", nil, nil)
	if code != 200 || as["items"] == nil {
		t.Fatalf("assignments: %d %v", code, as)
	}
	if code, _, _ := e.do("GET", "/v1/works/"+id+"/goals", "t2", "viewer", nil, nil); code != http.StatusNotFound {
		t.Fatalf("goals across tenants: %d", code)
	}
}

func TestPostmortemRoutes(t *testing.T) {
	e := newEnv(t)
	body := map[string]any{"kind": "incident", "title": "checkout 5xx",
		"goal": map[string]any{"intent": "mitigate", "subject_refs": []string{"service://prod/checkout"},
			"labels": map[string]string{"environment": "prod"}}}
	_, w1, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "pm"})
	id := w1["id"].(string)

	code, rep, _ := e.do("GET", "/v1/works/"+id+"/postmortem", "t1", "viewer", nil, nil)
	if code != 200 || rep["work_id"] != id || rep["root_cause"] != nil {
		t.Fatalf("report: %d %v", code, rep)
	}
	if code, _, _ := e.do("GET", "/v1/works/"+id+"/postmortem", "t2", "viewer", nil, nil); code != http.StatusNotFound {
		t.Fatalf("tenant isolation: %d", code)
	}
	// Markdown rendering.
	r := httptest.NewRequest("GET", "/v1/works/"+id+"/postmortem?format=markdown", nil)
	r.Header.Set("X-Tenant", "t1")
	r.Header.Set("X-Roles", "viewer")
	rec := httptest.NewRecorder()
	e.mux.ServeHTTP(rec, r)
	if rec.Code != 200 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/markdown") ||
		!strings.Contains(rec.Body.String(), "# Postmortem: checkout 5xx") {
		t.Fatalf("markdown: %d %s", rec.Code, rec.Body.String())
	}

	// Only closed Works get a postmortem Work, and only operators open one.
	if code, _, _ := e.do("POST", "/v1/works/"+id+"/postmortem", "t1", "operator", nil, nil); code != http.StatusConflict {
		t.Fatalf("open Work: %d", code)
	}
	if code, _, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", "operator", map[string]any{"type": work.CmdCancel},
		map[string]string{"Idempotency-Key": "c", "If-Match": `"1"`}); code != 200 {
		t.Fatalf("cancel: %d", code)
	}
	if code, _, _ := e.do("POST", "/v1/works/"+id+"/postmortem", "t1", "viewer", nil, nil); code != http.StatusForbidden {
		t.Fatalf("viewer: %d", code)
	}
	code, pm, _ := e.do("POST", "/v1/works/"+id+"/postmortem", "t1", "operator", nil, nil)
	if code != http.StatusCreated {
		t.Fatalf("create postmortem: %d %v", code, pm)
	}
	goal := pm["goal"].(map[string]any)
	labels := goal["labels"].(map[string]any)
	tpl := pm["template"].(map[string]any)
	if pm["kind"] != "request" || tpl["id"] != work.TemplatePostmortem || labels["source_work"] != id || labels["environment"] != "prod" ||
		!strings.Contains(goal["description"].(string), "Root cause") {
		t.Fatalf("postmortem Work: %v", pm)
	}
	// Idempotent per source Work.
	code, again, _ := e.do("POST", "/v1/works/"+id+"/postmortem", "t1", "operator", nil, nil)
	if code != http.StatusOK || again["id"] != pm["id"] {
		t.Fatalf("replay: %d", code)
	}
	// A postmortem Work does not get its own postmortem.
	pmID := pm["id"].(string)
	e.do("POST", "/v1/works/"+pmID+"/commands", "t1", "operator", map[string]any{"type": work.CmdCancel},
		map[string]string{"Idempotency-Key": "c2", "If-Match": `"1"`})
	if code, _, _ := e.do("POST", "/v1/works/"+pmID+"/postmortem", "t1", "operator", nil, nil); code != http.StatusBadRequest {
		t.Fatalf("postmortem of postmortem: %d", code)
	}
}

// Queues over HTTP: an operator may create a Work in critical-mitigation,
// the list filters by queue, unknown queues are 400, and set_queue moves a
// Work (operators only).
func TestWorkQueueRoutes(t *testing.T) {
	e := newEnv(t)
	body := map[string]any{"kind": "incident", "title": "sev1", "queue": "critical-mitigation",
		"goal": map[string]any{"intent": "mitigate", "subject_refs": []string{"service://prod/checkout"}}}
	code, w1, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "q1"})
	if code != http.StatusCreated || w1["queue"] != "critical-mitigation" {
		t.Fatalf("create critical: %d %v", code, w1)
	}
	id := w1["id"].(string)
	body["queue"] = "asap"
	if code, _, _ := e.do("POST", "/v1/works", "t1", "operator", body, map[string]string{"Idempotency-Key": "q2"}); code != http.StatusBadRequest {
		t.Fatalf("unknown queue: %d", code)
	}
	code, list, _ := e.do("GET", "/v1/works?queue=critical-mitigation", "t1", "viewer", nil, nil)
	if items, _ := list["items"].([]any); code != 200 || len(items) != 1 {
		t.Fatalf("filter: %d %v", code, list)
	}
	if code, _, _ := e.do("GET", "/v1/works?queue=asap", "t1", "viewer", nil, nil); code != http.StatusBadRequest {
		t.Fatalf("filter unknown queue: %d", code)
	}
	cmd := map[string]any{"type": work.CmdSetQueue, "payload": map[string]any{"queue": "routine-work"}}
	if code, _, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", "viewer", cmd, nil); code != http.StatusForbidden {
		t.Fatalf("viewer set_queue: %d", code)
	}
	code, moved, _ := e.do("POST", "/v1/works/"+id+"/commands", "t1", "operator", cmd, map[string]string{"Idempotency-Key": "sq1"})
	if code != 200 || moved["queue"] != "routine-work" {
		t.Fatalf("set_queue: %d %v", code, moved)
	}
}

func TestDecisionsEndpointIsolationAndLegacyCompatibility(t *testing.T) {
	e := newEnv(t)
	code, w, _ := e.do("POST", "/v1/works", "t1", "operator", map[string]any{"kind": "incident", "goal": map[string]any{"intent": "diagnose"}}, map[string]string{"Idempotency-Key": "decision-work"})
	if code != http.StatusCreated {
		t.Fatalf("create %d %v", code, w)
	}
	path := "/v1/works/" + w["id"].(string) + "/decisions"
	if code, _, _ := e.do("GET", path, "t1", "viewer", nil, nil); code != http.StatusOK {
		t.Fatalf("decisions %d", code)
	}
	if code, _, _ := e.do("GET", path, "t2", "viewer", nil, nil); code != http.StatusNotFound {
		t.Fatalf("foreign tenant %d", code)
	}
	if code, _, _ := e.do("GET", path, "t1", "", nil, nil); code != http.StatusForbidden {
		t.Fatalf("missing role %d", code)
	}
}

func TestDecisionContractAcrossAPIViewsAndGenerations(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	code, created, _ := e.do("POST", "/v1/works", "t1", "operator", map[string]any{
		"kind": "incident", "goal": map[string]any{"intent": "diagnose controlled regression"},
	}, map[string]string{"Idempotency-Key": "decision-contract"})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, created)
	}
	id := created["id"].(string)
	w, err := e.store.GetWork(ctx, "t1", id)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	rec, _, err := e.store.AddEvidence(ctx, evidence.Record{TenantID: "t1", WorkID: id, SourceType: evidence.SourceTool,
		SourceRef: "experiment://checkout/version-reproduction", TrustTier: "T2", Quality: evidence.QualityValid,
		Relation: evidence.RelCausedBy, Kind: evidence.KindObservation, CapturedAt: at,
		Summary: "Controlled v1/v2/v1 reproduction isolates a checkout panic to v2."})
	if err != nil {
		t.Fatal(err)
	}
	syn, hyps, err := evidence.Adjudicate(evidence.DecisionScope{TenantID: "t1", WorkID: id, Generation: w.Generation},
		[]evidence.ResultEnvelope{{RunID: cpkit.NewID(), AgentRole: "investigator", Status: "COMPLETED",
			Hypotheses: []evidence.ResultHypothesis{{Statement: "checkout v2 introduced a panic", Category: evidence.CatRootCause,
				Status: "VERIFIED", Supports: []string{rec.SourceRef}}}}}, []evidence.Record{rec}, at)
	if err != nil || len(hyps) != 1 || hyps[0].Decision.Verdict != evidence.DecisionConfirmed {
		t.Fatalf("adjudication: %+v %v", hyps, err)
	}
	w, err = e.store.ApplyWork(ctx, work.Mutation{TenantID: "t1", WorkID: id, ExpectedVersion: w.Version, ActorType: "system", Now: at,
		Findings: syn.Findings, Hypotheses: hyps})
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/works/" + id
	code, decisions, _ := e.do("GET", base+"/decisions", "t1", "viewer", nil, nil)
	items, _ := decisions["items"].([]any)
	if code != http.StatusOK || len(items) != 1 {
		t.Fatalf("decisions: %d %v", code, decisions)
	}
	decision := items[0].(map[string]any)
	want := hyps[0].Decision
	if decision["input_digest"] != want.InputDigest || decision["verdict"] != want.Verdict || decision["decision_key"] != want.DecisionKey || decision["generation"] != float64(w.Generation) {
		t.Fatalf("decision changed at API boundary: %v", decision)
	}
	code, hypotheses, _ := e.do("GET", base+"/hypotheses", "t1", "viewer", nil, nil)
	hitems, _ := hypotheses["items"].([]any)
	if code != http.StatusOK || len(hitems) != 1 {
		t.Fatalf("hypotheses: %d %v", code, hypotheses)
	}
	hypothesis := hitems[0].(map[string]any)
	hdecision, _ := hypothesis["decision"].(map[string]any)
	if hypothesis["status"] != evidence.HypSupported || hdecision["input_digest"] != want.InputDigest || hdecision["verdict"] != want.Verdict {
		t.Fatalf("hypothesis disagrees with decision: %v", hypothesis)
	}
	if !reflect.DeepEqual(decision, hdecision) {
		t.Fatalf("hypothesis and decisions endpoints expose different records: %v vs %v", hdecision, decision)
	}
	code, report, _ := e.do("GET", base+"/postmortem", "t1", "viewer", nil, nil)
	rdecisions, _ := report["decisions"].([]any)
	root, _ := report["root_cause"].(map[string]any)
	if code != http.StatusOK || len(rdecisions) != 1 || root["statement"] != hyps[0].Statement {
		t.Fatalf("postmortem: %d %v", code, report)
	}
	rdecision := rdecisions[0].(map[string]any)
	if rdecision["input_digest"] != want.InputDigest || rdecision["verdict"] != want.Verdict {
		t.Fatalf("report disagrees with decision: %v", rdecision)
	}
	if !reflect.DeepEqual(decision, rdecision) {
		t.Fatalf("postmortem and decisions endpoints expose different records: %v vs %v", rdecision, decision)
	}

	_, _, err = e.store.AddEvidence(ctx, evidence.Record{TenantID: "t1", WorkID: id, SourceType: evidence.SourceTool,
		SourceRef: "experiment://checkout/retraction", TrustTier: "T2", Quality: evidence.QualityValid,
		Relation: evidence.RelRefutes, Kind: evidence.KindObservation, CapturedAt: time.Now().UTC(),
		Supersedes: rec.ID, Summary: "The original measurement was invalidated by a controlled retest."})
	if err != nil {
		t.Fatal(err)
	}
	code, decisions, _ = e.do("GET", base+"/decisions", "t1", "viewer", nil, nil)
	items, _ = decisions["items"].([]any)
	if code != http.StatusOK || len(items) != 0 {
		t.Fatalf("superseded evidence still establishes current decision: %d %v", code, decisions)
	}
	code, report, _ = e.do("GET", base+"/postmortem", "t1", "viewer", nil, nil)
	if code != http.StatusOK || report["root_cause"] != nil {
		t.Fatalf("superseded evidence still establishes root cause: %d %v", code, report)
	}
	code, score, _ := e.do("GET", base+"/evaluation", "t1", "viewer", nil, nil)
	if code != http.StatusOK || score["items"].(map[string]any)["grounded_conclusions"] != float64(0) {
		t.Fatalf("superseded evidence still grounded: %d %v", code, score)
	}
	if !hyps[0].CurrentDecisionFor(evidence.DecisionScope{TenantID: "t1", WorkID: id, Generation: w.Generation}) {
		t.Fatal("historical decision no longer replayable")
	}

	next := w.Generation + 1
	if _, err := e.store.ApplyWork(ctx, work.Mutation{TenantID: "t1", WorkID: id, ExpectedVersion: w.Version, ActorType: "system", Now: at.Add(time.Second), Generation: &next}); err != nil {
		t.Fatal(err)
	}
	code, decisions, _ = e.do("GET", base+"/decisions", "t1", "viewer", nil, nil)
	items, _ = decisions["items"].([]any)
	if code != http.StatusOK || len(items) != 0 {
		t.Fatalf("old generation leaked as current: %d %v", code, decisions)
	}
	code, report, _ = e.do("GET", base+"/postmortem", "t1", "viewer", nil, nil)
	rdecisions, _ = report["decisions"].([]any)
	if code != http.StatusOK || report["root_cause"] != nil || len(rdecisions) != 0 {
		t.Fatalf("old generation still establishes a cause: %d %v", code, report)
	}
	// The historical hypothesis stays readable, while current-decision
	// consumers exclude it until the new generation has been adjudicated.
	code, hypotheses, _ = e.do("GET", base+"/hypotheses", "t1", "viewer", nil, nil)
	hitems, _ = hypotheses["items"].([]any)
	if code != http.StatusOK || len(hitems) != 1 {
		t.Fatalf("historical hypothesis lost: %d %v", code, hypotheses)
	}
	hdecision = hitems[0].(map[string]any)["decision"].(map[string]any)
	if hdecision["input_digest"] != want.InputDigest || hdecision["generation"] != float64(next-1) {
		t.Fatalf("historical decision rewritten: %v", hdecision)
	}
}
