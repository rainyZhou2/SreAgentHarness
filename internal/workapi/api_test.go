package workapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

type env struct {
	t   *testing.T
	mux http.Handler
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
		World:    &world.Service{Store: s}, Log: quiet, Postmortems: &postmortem.Builder{Ledger: s}}
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
	return &env{t: t, mux: h}
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
