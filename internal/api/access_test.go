package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/store/pg"
)

// approvalStore reports a pending approval on one run/session, to exercise
// the approver exception without building the whole approval flow.
type approvalStore struct {
	api.Store
	runID, sessionID, initiator string
}

func (a approvalStore) ListApprovals(_ context.Context, _, runID string) ([]domain.ApprovalRequest, error) {
	if runID == a.runID {
		return []domain.ApprovalRequest{{ID: "ap1"}}, nil
	}
	return nil, nil
}

func (a approvalStore) ListPendingApprovals(context.Context, string, int) ([]pg.PendingApproval, error) {
	sid, ini := a.sessionID, a.initiator
	return []pg.PendingApproval{{ApprovalRequest: domain.ApprovalRequest{ID: "ap1"}, RunID: a.runID, SessionID: &sid, InitiatorID: &ini}}, nil
}

type fixture struct {
	h                      http.Handler
	aliceRun, bobRun, cron string
	unknown                string
	aliceSess, bobSess     string
	st                     *localstore.Store
}

func newFixture(t *testing.T, withApproval bool) *fixture {
	t.Helper()
	ctx := context.Background()
	st, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	arts := artifact.NewMemory()
	if _, err := st.RegisterProfile(ctx, "t1", "p", 1, json.RawMessage(`{"strategy":"llm","tools":["*"],"max_steps":4}`)); err != nil {
		t.Fatal(err)
	}
	mkRun := func(key, typ, user string) string {
		g, _ := json.Marshal(map[string]string{"goal": "secret goal of " + key})
		ref, hash, _ := arts.Put(ctx, "t1", "goal", g)
		run, _, err := st.CreateRun(ctx, domain.CreateRunRequest{TenantID: "t1", ProfileID: "p", ProfileVersion: 1, GoalRef: ref,
			GoalHash: hash, IdempotencyKey: key, InitiatorType: typ, InitiatorID: user,
			InitialSteps: []domain.PlannedStep{{Kind: domain.StepReason, Executor: "reasoner", InputRef: ref, InputHash: hash, Status: domain.StepPending}}})
		if err != nil {
			t.Fatal(err)
		}
		return run.ID
	}
	f := &fixture{st: st}
	f.aliceRun, f.bobRun, f.cron = mkRun("a", "user", "alice"), mkRun("b", "user", "bob"), mkRun("c", "trigger", "")
	f.unknown = mkRun("u", "unknown", "")
	mkSess := func(key, user string) string {
		s, _, err := st.CreateSession(ctx, domain.CreateSessionRequest{TenantID: "t1", ActorID: user, ProfileID: "p", ProfileVersion: 1, Title: key + " chat", IdempotencyKey: key})
		if err != nil {
			t.Fatal(err)
		}
		return s.ID
	}
	f.aliceSess, f.bobSess = mkSess("alice", "alice"), mkSess("bob", "bob")
	var store api.Store = st
	if withApproval {
		store = approvalStore{Store: st, runID: f.aliceRun, sessionID: f.aliceSess, initiator: "alice"}
	}
	f.h = (&api.Server{Store: store, Artifacts: arts}).Handler()
	return f
}

func (f *fixture) do(user, roles, method, path, body string) (int, map[string]any) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", "t1")
	req.Header.Set("X-Actor-ID", user)
	req.Header.Set("X-Roles", roles)
	req.Header.Set("Idempotency-Key", "k-"+user+path)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	f.h.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func ids(out map[string]any, key string) map[string]bool {
	got := map[string]bool{}
	list, _ := out[key].([]any)
	for _, x := range list {
		m, _ := x.(map[string]any)
		if id, _ := m["id"].(string); id != "" {
			got[id] = true
		}
	}
	return got
}

// Another operator in the same tenant must not reach a user's chats or runs,
// by id or by list, through any route that names them.
func TestUsersCannotSeeEachOthersChatsAndRuns(t *testing.T) {
	f := newFixture(t, false)
	const bob = "operator"
	for _, p := range []string{
		"/v1/runs/" + f.aliceRun, "/v1/runs/" + f.aliceRun + "/steps", "/v1/runs/" + f.aliceRun + "/events",
		"/v1/runs/" + f.aliceRun + "/events/stream", "/v1/runs/" + f.aliceRun + "/approvals", "/v1/runs/" + f.aliceRun + "/cost",
		"/v1/runs/" + f.aliceRun + "/checkpoints", "/v1/runs/" + f.aliceRun + "/model-requests", "/v1/runs/" + f.aliceRun + "/flow",
		"/v1/runs/" + f.aliceRun + "/attachments/0", "/v1/reasoning/runs/" + f.aliceRun,
		"/v1/sessions/" + f.aliceSess, "/v1/sessions/" + f.aliceSess + "/turns",
	} {
		if code, _ := f.do("bob", bob, "GET", p, ""); code != 404 {
			t.Errorf("bob GET %s: %d, want 404 (the same answer as a missing id)", p, code)
		}
	}
	for _, c := range [][3]string{
		{"POST", "/v1/runs/" + f.aliceRun + "/commands", `{"type":"cancel","payload":{}}`},
		{"POST", "/v1/runs/" + f.aliceRun + "/fork", `{}`},
		{"POST", "/v1/sessions/" + f.aliceSess + "/turns", `{"text":"hi","client_request_id":"x"}`},
		{"PATCH", "/v1/sessions/" + f.aliceSess, `{"title":"mine now"}`},
		{"POST", "/v1/sessions/" + f.aliceSess + "/branches", `{}`},
	} {
		if code, _ := f.do("bob", bob, c[0], c[1], c[2]); code != 404 {
			t.Errorf("bob %s %s: %d, want 404", c[0], c[1], code)
		}
	}
	// a missing id answers the same
	if code, _ := f.do("bob", bob, "GET", "/v1/runs/does-not-exist", ""); code != 404 {
		t.Errorf("%d", code)
	}

	// lists
	code, out := f.do("bob", bob, "GET", "/v1/sessions", "")
	if got := ids(out, "sessions"); code != 200 || got[f.aliceSess] || !got[f.bobSess] {
		t.Errorf("bob lists only his own sessions: %v", got)
	}
	code, out = f.do("bob", bob, "GET", "/v1/runs", "")
	got := ids(out, "runs")
	if code != 200 || got[f.aliceRun] || !got[f.bobRun] || !got[f.cron] {
		t.Errorf("bob sees his runs and shared operational runs, not alice's: %v", got)
	}
	if strings.Contains(mustJSON(out), "secret goal of a") {
		t.Errorf("alice's goal text leaked into bob's run list")
	}

	// a run of unrecognized origin fails closed: admins only
	if code, _ := f.do("bob", bob, "GET", "/v1/runs/"+f.unknown, ""); code != 404 {
		t.Errorf("a run of unknown origin must not be shared: %d", code)
	}
	if code, _ := f.do("root", "admin", "GET", "/v1/runs/"+f.unknown, ""); code != 200 {
		t.Errorf("%d", code)
	}

	// the owner, an admin and shared runs are reachable
	for _, c := range []struct{ user, roles, path string }{
		{"alice", "operator", "/v1/runs/" + f.aliceRun}, {"alice", "operator", "/v1/sessions/" + f.aliceSess},
		{"root", "admin", "/v1/runs/" + f.aliceRun}, {"root", "admin", "/v1/sessions/" + f.aliceSess},
		{"bob", "operator", "/v1/runs/" + f.cron}, {"bob", "viewer", "/v1/runs/" + f.bobRun},
	} {
		if code, _ := f.do(c.user, c.roles, "GET", c.path, ""); code != 200 {
			t.Errorf("%s (%s) GET %s: %d, want 200", c.user, c.roles, c.path, code)
		}
	}
	code, out = f.do("root", "admin", "GET", "/v1/sessions", "")
	if got := ids(out, "sessions"); code != 200 || !got[f.aliceSess] || !got[f.bobSess] {
		t.Errorf("an admin lists every session: %v", got)
	}
	code, out = f.do("root", "admin", "GET", "/v1/runs", "")
	if got := ids(out, "runs"); code != 200 || !got[f.aliceRun] || !got[f.bobRun] {
		t.Errorf("an admin lists every run: %v", got)
	}
}

// The create-idempotency key is private to the caller: another user sending
// the same key must get their own session and run, not the first user's.
func TestIdempotencyKeysAreNotSharedAcrossUsers(t *testing.T) {
	f := newFixture(t, false)
	post := func(user, path, body string) (int, map[string]any) {
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", "t1")
		req.Header.Set("X-Actor-ID", user)
		req.Header.Set("X-Roles", "operator")
		req.Header.Set("Idempotency-Key", "same-key")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		f.h.ServeHTTP(rec, req)
		out := map[string]any{}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	sessionID := func(out map[string]any) string {
		m, _ := out["session"].(map[string]any)
		s, _ := m["id"].(string)
		return s
	}
	_, a := post("alice", "/v1/sessions", `{"title":"alice idem"}`)
	_, b := post("bob", "/v1/sessions", `{"title":"bob idem"}`)
	if a["created"] != true || b["created"] != true || sessionID(a) == sessionID(b) {
		t.Fatalf("the same key from two users must create two sessions: %v %v", a, b)
	}
	_, a2 := post("alice", "/v1/sessions", `{"title":"alice idem"}`)
	if a2["created"] != false || sessionID(a2) != sessionID(a) {
		t.Fatalf("the same user replaying a key gets the same session: %v", a2)
	}
}

func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

// An approver can open what they are asked to approve, and nothing else of
// the same user's.
func TestApproverSeesOnlyWhatNeedsApproval(t *testing.T) {
	f := newFixture(t, true)
	if code, _ := f.do("carol", "approver", "GET", "/v1/runs/"+f.aliceRun, ""); code != 200 {
		t.Errorf("an approver opens a run with an approval request: %d", code)
	}
	if code, _ := f.do("carol", "approver", "GET", "/v1/sessions/"+f.aliceSess, ""); code != 200 {
		t.Errorf("an approver reads the session of a pending approval: %d", code)
	}
	if code, _ := f.do("carol", "approver", "POST", "/v1/sessions/"+f.aliceSess+"/turns", `{"text":"hi","client_request_id":"x"}`); code != 404 && code != 403 {
		t.Errorf("the session exception is read-only: %d", code)
	}
	for _, p := range []string{"/v1/runs/" + f.bobRun, "/v1/sessions/" + f.bobSess} {
		if code, _ := f.do("carol", "approver", "GET", p, ""); code != 404 {
			t.Errorf("an approver has no blanket access: GET %s %d", p, code)
		}
	}
	// the inbox: approvers see every pending approval, others only their own
	if code, out := f.do("carol", "approver", "GET", "/v1/approvals", ""); code != 200 || len(ids(out, "approvals")) != 1 {
		t.Errorf("approver inbox: %d %v", code, out)
	}
	if _, out := f.do("alice", "operator", "GET", "/v1/approvals", ""); len(ids(out, "approvals")) != 1 {
		t.Errorf("alice sees the approval on her own run: %v", out)
	}
	if _, out := f.do("bob", "operator", "GET", "/v1/approvals", ""); len(ids(out, "approvals")) != 0 {
		t.Errorf("bob must not see alice's pending approval: %v", out)
	}
}
