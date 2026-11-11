package workapi

// Security tests for §19: service-principal scopes, approver scope and risk
// ceilings, break-glass, operator evidence, and the audit trail.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

type auditLog struct {
	mu      sync.Mutex
	entries []pg.AuditEntry
}

func (l *auditLog) Audit(_ context.Context, e pg.AuditEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, e)
	return nil
}

func (l *auditLog) find(act, outcome string) *pg.AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := range l.entries {
		if l.entries[i].Action == act && l.entries[i].Outcome == outcome {
			return &l.entries[i]
		}
	}
	return nil
}

type stubExec struct{ risk domain.RiskClass }

func (x stubExec) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "deployment.rollback", Version: "1", ResourceKinds: []string{"deployment"}, Risk: x.risk,
		IdempotencyMode: action.IdemNative}
}
func (stubExec) Preflight(context.Context, action.Plan) (action.PreflightResult, error) {
	return action.PreflightResult{Allowed: true}, nil
}
func (stubExec) Execute(context.Context, action.Plan, string) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}
func (stubExec) Inspect(context.Context, action.Plan) (action.InspectResult, error) {
	return action.InspectResult{}, action.ErrUnsupported
}

type secEnv struct {
	t     *testing.T
	ctx   context.Context
	mux   http.Handler
	store *cpsql.Store
	rt    *action.Runtime
	audit *auditLog
	now   time.Time
}

// newSecEnv authenticates from headers: X-Tenant, X-User, X-Roles,
// X-Method (token|oidc|dev) and X-Scope (JSON auth.Scope).
func newSecEnv(t *testing.T) *secEnv {
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	now := time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)
	clock := cpkit.Clock(func() time.Time { return now })
	s.Clock = clock
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cat, _ := orchestration.NewCatalog(nil)
	router, _ := eventing.NewRouter([]eventing.Rule{{Name: "alerts", Match: eventing.Predicate{EventTypes: []string{"alert.*"}},
		Key: "dedup_key"}})
	reg := action.NewRegistry()
	if err := reg.Register(stubExec{risk: domain.RiskR2}); err != nil {
		t.Fatal(err)
	}
	rt := &action.Runtime{Store: s, Registry: reg, Clock: clock, Log: quiet, Policy: policy.Default()}
	al := &auditLog{}
	a := &API{Store: s, Service: &orchestration.Service{Store: s, Catalog: cat, Clock: clock, Log: quiet},
		Ingestor: &orchestration.Ingestor{Store: s, Router: router, Catalog: cat, Clock: clock, Log: quiet},
		World:    &world.Service{Store: s, Clock: clock}, Actions: rt, Audit: al, Clock: clock, Log: quiet}
	mux := http.NewServeMux()
	a.Register(mux)
	roles := a.RouteRoles()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := auth.Principal{Tenant: r.Header.Get("X-Tenant"), Subject: r.Header.Get("X-User"), Method: r.Header.Get("X-Method")}
		for _, x := range strings.Split(r.Header.Get("X-Roles"), ",") {
			p.Roles = append(p.Roles, auth.Role(x))
		}
		if sc := r.Header.Get("X-Scope"); sc != "" {
			p.Scope = &auth.Scope{}
			if err := json.Unmarshal([]byte(sc), p.Scope); err != nil {
				t.Fatal(err)
			}
		}
		_, pattern := mux.Handler(r)
		if need, ok := roles[pattern]; ok && !p.Has(need) {
			writeErr(w, http.StatusForbidden, "forbidden", "missing role")
			return
		}
		mux.ServeHTTP(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	})
	return &secEnv{t: t, ctx: context.Background(), mux: h, store: s, rt: rt, audit: al, now: now}
}

type caller struct {
	tenant, user, roles, method, scope string
}

func (e *secEnv) do(c caller, method, path string, body any, hdr ...string) (int, map[string]any) {
	e.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	r := httptest.NewRequest(method, path, rd)
	if c.tenant == "" {
		c.tenant = "t1"
	}
	if c.method == "" {
		c.method = "token"
	}
	r.Header.Set("X-Tenant", c.tenant)
	r.Header.Set("X-User", c.user)
	r.Header.Set("X-Roles", c.roles)
	r.Header.Set("X-Method", c.method)
	r.Header.Set("X-Scope", c.scope)
	for i := 0; i+1 < len(hdr); i += 2 {
		r.Header.Set(hdr[i], hdr[i+1])
	}
	w := httptest.NewRecorder()
	e.mux.ServeHTTP(w, r)
	var out map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return w.Code, out
}

// heldAction creates a Work and a WAITING_APPROVAL R2 rollback on target.
func (e *secEnv) heldAction(target string) action.Record {
	e.t.Helper()
	var tpl work.Template
	for _, x := range work.Builtin() {
		if x.ID == "incident" {
			tpl = x
		}
	}
	w, _, err := e.store.CreateWork(e.ctx, work.NewWork{TenantID: "t1", Kind: work.KindIncident, Title: "checkout 5xx",
		Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{target}}, Template: tpl, IdempotencyKey: cpkit.NewID(),
		Now: e.now, ActorType: "system", ActorID: "work", Subjects: []work.SubjectLink{{Kind: "subject", ID: target}}})
	if err != nil {
		e.t.Fatal(err)
	}
	r, err := e.rt.Propose(e.ctx, action.ProposeRequest{TenantID: "t1", WorkID: w.ID, Generation: 1,
		Name: "deployment.rollback", Target: target, Args: json.RawMessage(`{"to":"v1"}`), ProposedBy: "system:work"})
	if err != nil || r.State != action.StateWaitingApproval {
		e.t.Fatalf("propose: %v %s %q", err, r.State, r.Reason)
	}
	return r
}

func TestServicePrincipalScope(t *testing.T) {
	e := newSecEnv(t)
	svc := caller{user: "alertmanager-prod", roles: "service",
		scope: `{"event_sources":["alertmanager"],"event_types":["alert.*"],"resources":["service://prod/*"]}`}
	ev := func(source, typ, subject string) map[string]any {
		return map[string]any{"event_id": cpkit.NewID(), "source": source, "type": typ, "subject_ref": subject,
			"dedup_key": "k-" + subject, "occurred_at": e.now}
	}
	if c, out := e.do(svc, "POST", "/v1/events", ev("alertmanager", "alert.firing", "service://prod/checkout")); c != http.StatusAccepted {
		t.Fatalf("in-scope event: %d %v", c, out)
	}
	for name, body := range map[string]map[string]any{
		"other source":  ev("grafana", "alert.firing", "service://prod/checkout"),
		"other type":    ev("alertmanager", "deploy.started", "service://prod/checkout"),
		"other subject": ev("alertmanager", "alert.firing", "service://staging/checkout"),
	} {
		if c, _ := e.do(svc, "POST", "/v1/events", body); c != http.StatusForbidden {
			t.Fatalf("%s: %d", name, c)
		}
	}
	if e.audit.find("event.ingest", "denied") == nil {
		t.Fatal("scope denials must be audited")
	}
	// A service principal can only submit events.
	if c, _ := e.do(svc, "POST", "/v1/works", map[string]any{"kind": "incident", "template": "incident"},
		"Idempotency-Key", "k1"); c != http.StatusForbidden {
		t.Fatalf("service creating a Work: %d", c)
	}
	if c, _ := e.do(svc, "GET", "/v1/works", nil); c != http.StatusForbidden {
		t.Fatalf("service reading Works: %d", c)
	}
	// Operators keep event ingestion (operator implies service).
	op := caller{user: "oncall", roles: "operator"}
	if c, _ := e.do(op, "POST", "/v1/events", ev("manual", "note.added", "service://prod/checkout")); c != http.StatusAccepted {
		t.Fatalf("operator event: %d", c)
	}
	// A scoped operator cannot open Works or assert facts outside its scope.
	scoped := caller{user: "team-a", roles: "operator", scope: `{"resources":["service://prod/search*"]}`}
	if c, _ := e.do(scoped, "POST", "/v1/works", map[string]any{"kind": "incident", "template": "incident", "title": "x",
		"goal": map[string]any{"intent": "mitigate", "subject_refs": []string{"service://prod/checkout"}}},
		"Idempotency-Key", "k2"); c != http.StatusForbidden {
		t.Fatalf("scoped operator, foreign subject: %d", c)
	}
	if c, _ := e.do(scoped, "POST", "/v1/world/facts", map[string]any{"subject_ref": "service://prod/checkout",
		"predicate": "owner", "value": "x"}); c != http.StatusForbidden {
		t.Fatalf("scoped fact: %d", c)
	}
}

func TestApproverScopeAndRisk(t *testing.T) {
	e := newSecEnv(t)
	a := e.heldAction("deployment://prod/checkout")
	body := map[string]any{"decision": "approve", "intent_hash": a.IntentHash}
	path := "/v1/actions/" + a.ID + "/approve"

	if c, _ := e.do(caller{user: "junior", roles: "approver", scope: `{"max_risk":"R1"}`}, "POST", path, body); c != http.StatusForbidden {
		t.Fatalf("R1 approver on an R2 action: %d", c)
	}
	if c, _ := e.do(caller{user: "staging-lead", roles: "approver", scope: `{"resources":["deployment://staging/*"]}`},
		"POST", path, body); c != http.StatusForbidden {
		t.Fatalf("approver outside resource scope: %d", c)
	}
	if got, _ := e.store.GetAction(e.ctx, "t1", a.ID); got.State != action.StateWaitingApproval {
		t.Fatalf("refused approvals must not change the action: %s", got.State)
	}
	// Rejecting is always allowed to an approver (it only reduces risk).
	// An approver in scope with an R2 ceiling approves.
	c, out := e.do(caller{user: "prod-lead", roles: "approver", scope: `{"resources":["deployment://prod/*"],"max_risk":"R2"}`},
		"POST", path, body)
	if c != http.StatusOK || out["state"] != string(action.StateReady) {
		t.Fatalf("in-scope approval: %d %v", c, out)
	}
	if en := e.audit.find("action.approve", "allowed"); en == nil || en.ActorID != "prod-lead" || en.Details["intent_hash"] != a.IntentHash {
		t.Fatalf("approval audit: %+v", en)
	}
	if e.audit.find("action.approve", "denied") == nil {
		t.Fatal("out-of-scope approvals must be audited")
	}
}

func TestBreakGlass(t *testing.T) {
	e := newSecEnv(t)
	a := e.heldAction("deployment://prod/checkout")
	path := "/v1/actions/" + a.ID + "/break-glass"
	good := map[string]any{"intent_hash": a.IntentHash, "incident_ref": "INC-2026-1110",
		"justification": "SEV1: checkout down for all users, approvers unreachable, rollback is the documented fix"}

	// Not implied by admin or approver.
	for _, roles := range []string{"admin", "approver"} {
		if c, _ := e.do(caller{user: "boss", roles: roles}, "POST", path, good); c != http.StatusForbidden {
			t.Fatalf("%s must not break glass: %d", roles, c)
		}
	}
	bg := caller{user: "ic-alice", roles: "break_glass"}
	// Development identities cannot break glass.
	if c, _ := e.do(caller{user: "ic-alice", roles: "break_glass", method: "dev"}, "POST", path, good); c != http.StatusForbidden {
		t.Fatalf("dev identity: %d", c)
	}
	if c, _ := e.do(bg, "POST", path, map[string]any{"intent_hash": a.IntentHash, "justification": "urgent"}); c != http.StatusUnprocessableEntity {
		t.Fatalf("short justification: %d", c)
	}
	if c, _ := e.do(bg, "POST", path, map[string]any{"intent_hash": "sha256:00", "justification": good["justification"]}); c != http.StatusUnprocessableEntity {
		t.Fatalf("wrong intent: %d", c)
	}
	// An R2 release must name its incident.
	if c, _ := e.do(bg, "POST", path, map[string]any{"intent_hash": a.IntentHash, "justification": good["justification"]}); c != http.StatusUnprocessableEntity {
		t.Fatalf("missing incident: %d", c)
	}
	c, out := e.do(bg, "POST", path, good)
	if c != http.StatusOK || out["state"] != string(action.StateReady) {
		t.Fatalf("break glass: %d %v", c, out)
	}
	got, _ := e.store.GetAction(e.ctx, "t1", a.ID)
	if got.BreakGlass == nil || got.BreakGlass.Actor != "ic-alice" || got.BreakGlass.IntentHash != a.IntentHash ||
		got.BreakGlass.IncidentRef != "INC-2026-1110" || got.BreakGlass.Scope != a.TargetRef || got.BreakGlass.ExpiresAt.IsZero() {
		t.Fatalf("grant: %+v", got.BreakGlass)
	}
	if en := e.audit.find("action.break_glass", "applied"); en == nil || en.ActorID != "ic-alice" {
		t.Fatalf("break-glass audit: %+v", en)
	}
	evs, _ := e.store.ListWorkEvents(e.ctx, "t1", a.WorkID, 0, 50)
	found := false
	for _, ev := range evs {
		if ev.Type == "work.wake" && strings.Contains(string(ev.Payload), "action_break_glass") {
			found = true
		}
	}
	if !found {
		t.Fatal("break-glass must be on the Work timeline")
	}
	// Once released it cannot be broken again.
	if c, _ := e.do(bg, "POST", path, good); c != http.StatusConflict {
		t.Fatalf("second break-glass: %d", c)
	}
}

func TestOperatorEvidence(t *testing.T) {
	e := newSecEnv(t)
	a := e.heldAction("deployment://prod/checkout")
	path := "/v1/works/" + a.WorkID + "/evidence"
	ev := map[string]any{"source_type": "human", "summary": "pod logs show OOMKilled; token=abcd1234efgh5678 leaked in env",
		"trust_tier": "T1", "claims": []string{"oom"}}
	if c, _ := e.do(caller{user: "viewer", roles: "viewer"}, "POST", path, ev); c != http.StatusForbidden {
		t.Fatalf("viewer: %d", c)
	}
	if c, _ := e.do(caller{tenant: "t2", user: "other", roles: "operator"}, "POST", path, ev); c != http.StatusNotFound {
		t.Fatalf("other tenant: %d", c)
	}
	c, out := e.do(caller{user: "oncall", roles: "operator"}, "POST", path, ev)
	if c != http.StatusCreated {
		t.Fatalf("add evidence: %d %v", c, out)
	}
	if out["trust_tier"] != world.TrustUntrusted || strings.Contains(out["summary"].(string), "abcd1234efgh5678") ||
		out["actor_id"] != "oncall" {
		t.Fatalf("evidence: %v", out)
	}
	if c, _ := e.do(caller{user: "oncall", roles: "operator"}, "POST", path, ev); c != http.StatusOK {
		t.Fatalf("duplicate evidence must be idempotent: %d", c)
	}
	c, list := e.do(caller{user: "v", roles: "viewer"}, "GET", path, nil)
	if c != http.StatusOK || len(list["items"].([]any)) != 1 {
		t.Fatalf("list: %d %v", c, list)
	}
	// The Work's evidence budget (RFC §20.3) answers 409 once spent.
	for i := 1; i < work.DefaultMaxEvidence; i++ {
		if _, _, err := e.store.AddEvidence(e.ctx, evidence.Record{TenantID: "t1", WorkID: a.WorkID, SourceType: evidence.SourceHuman,
			SourceRef: "api:filler", Summary: fmt.Sprintf("note %d", i), TrustTier: world.TrustUntrusted}); err != nil {
			t.Fatal(err)
		}
	}
	ev["summary"] = "one more"
	if c, out := e.do(caller{user: "oncall", roles: "operator"}, "POST", path, ev); c != http.StatusConflict || out["error"] != "limit_reached" {
		t.Fatalf("over budget: %d %v", c, out)
	}
}
