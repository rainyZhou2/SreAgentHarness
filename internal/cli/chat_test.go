package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a tiny harness: one session, one run that asks for approval
// and completes once the approve command arrives.
type fakeServer struct {
	mu       sync.Mutex
	approved bool
	commands []map[string]any
	auth     []string
}

func (f *fakeServer) handler() http.Handler {
	mux := http.NewServeMux()
	js := func(w http.ResponseWriter, v any) { _ = json.NewEncoder(w).Encode(v) }
	mux.HandleFunc("GET /v1/whoami", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		f.mu.Unlock()
		js(w, map[string]any{"subject": "me"})
	})
	mux.HandleFunc("POST /v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Idempotency-Key") == "" {
			http.Error(w, `{"error":"idempotency_key_required"}`, 400)
			return
		}
		js(w, map[string]any{"session": map[string]any{"id": "s1"}})
	})
	mux.HandleFunc("POST /v1/sessions/s1/turns", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]any{"turn": map[string]any{"run_id": "r1"}})
	})
	mux.HandleFunc("GET /v1/runs/r1/events/stream", func(w http.ResponseWriter, r *http.Request) {
		fl := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		send := func(ev string, data any) {
			b, _ := json.Marshal(data)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev, b)
			fl.Flush()
		}
		send("model.delta", map[string]any{"event": map[string]any{"type": "text_delta", "text": "checking…"}})
		send("run.created", map[string]any{"event_seq": 1, "event_type": "approval.requested"})
		// wait for the decision
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			f.mu.Lock()
			ok := f.approved
			f.mu.Unlock()
			if ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		send("run.completed", map[string]any{"event_seq": 2, "event_type": "run.completed"})
		fmt.Fprint(w, "event: stream.end\ndata: {\"status\":\"COMPLETED\"}\n\n")
		fl.Flush()
	})
	mux.HandleFunc("GET /v1/runs/r1/approvals", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]any{"approvals": []map[string]any{{"id": "a1", "action_hash": "sha256:abc", "risk_class": "R2",
			"tool_name": "demo.restart", "target": "checkout", "status": "PENDING"}}})
	})
	mux.HandleFunc("GET /v1/runs/r1", func(w http.ResponseWriter, r *http.Request) {
		js(w, map[string]any{"display_status": "COMPLETED", "run": map[string]any{"version": 7},
			"result": map[string]any{"answer": "restarted checkout"}})
	})
	mux.HandleFunc("POST /v1/runs/r1/commands", func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		f.mu.Lock()
		f.commands = append(f.commands, b)
		f.approved = true
		f.mu.Unlock()
		js(w, map[string]any{"ok": true})
	})
	return mux
}

func run(t *testing.T, in string, yes bool) (string, *fakeServer) {
	t.Helper()
	f := &fakeServer{}
	srv := httptest.NewServer(f.handler())
	defer srv.Close()
	c, err := NewClient(srv.URL, " tok \n")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	ch := &Chat{C: c, In: strings.NewReader(in), Out: &out, AutoYes: yes}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ch.Run(ctx); err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	return out.String(), f
}

func TestChatApprovesInline(t *testing.T) {
	out, f := run(t, "restart checkout\ny\n", false)
	for _, want := range []string{"approval needed: R2 demo.restart", "approved", "restarted checkout", "COMPLETED", "checking…"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	if len(f.commands) != 1 || f.commands[0]["type"] != "approve_tool" || f.commands[0]["expected_version"] != float64(7) {
		t.Fatalf("commands: %+v", f.commands)
	}
	p := f.commands[0]["payload"].(map[string]any)
	if p["approval_request_id"] != "a1" || p["action_hash"] != "sha256:abc" {
		t.Fatalf("payload must bind the approval and its hash: %+v", p)
	}
	if f.auth[0] != "Bearer tok" {
		t.Fatalf("token must be trimmed and sent as bearer: %q", f.auth[0])
	}
}

func TestChatRejectWithReason(t *testing.T) {
	_, f := run(t, "restart checkout\nnot during peak\n", false)
	if len(f.commands) != 1 || f.commands[0]["type"] != "reject_tool" || f.commands[0]["reason"] != "not during peak" {
		t.Fatalf("commands: %+v", f.commands)
	}
}

func TestChatDefaultIsReject(t *testing.T) {
	_, f := run(t, "restart checkout\n\n", false)
	if len(f.commands) != 1 || f.commands[0]["type"] != "reject_tool" {
		t.Fatalf("an empty answer must not approve: %+v", f.commands)
	}
}

func TestChatYesFlag(t *testing.T) {
	_, f := run(t, "restart checkout\n", true)
	if len(f.commands) != 1 || f.commands[0]["type"] != "approve_tool" {
		t.Fatalf("commands: %+v", f.commands)
	}
}

func TestChatInputClosedKeepsRunWaiting(t *testing.T) {
	out, f := run(t, "restart checkout\n", false)
	if len(f.commands) != 0 || !strings.Contains(out, "still waiting for approval") {
		t.Fatalf("EOF at the prompt must not decide anything:\n%s\n%+v", out, f.commands)
	}
}

func TestNewClientValidation(t *testing.T) {
	for _, c := range [][2]string{{"ftp://x", "t"}, {"http://", "t"}, {"http://x", " "}} {
		if _, err := NewClient(c[0], c[1]); err == nil {
			t.Errorf("%v must be rejected", c)
		}
	}
}

func TestParseSSE(t *testing.T) {
	var got []string
	_ = parseSSE(strings.NewReader(": keepalive\nevent: a\ndata: 1\ndata: 2\n\ndata: x\n\n"), func(e, d string) {
		got = append(got, e+"|"+d)
	})
	if strings.Join(got, ",") != "a|1\n2,|x" {
		t.Fatalf("%q", got)
	}
	_ = io.Discard
}

func TestMemoryCommands(t *testing.T) {
	var gotPath, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.EscapedPath(), r.Method
		switch {
		case r.Method == "GET":
			_, _ = w.Write([]byte(`{"memories":[{"subject":"pref:answer_language","title":"answer language","content":"中文","status":"ACTIVE"},{"title":"old","content":"x","status":"ARCHIVED"}]}`))
		case r.Method == "PUT":
			_, _ = w.Write([]byte(`{"created":true,"redactions":2}`))
		default:
			http.Error(w, `{"error":"not_found","message":"resource not found"}`, 404)
		}
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, "t")
	var out bytes.Buffer
	if err := MemoryList(context.Background(), c, &out); err != nil || !strings.Contains(out.String(), "answer_language") || strings.Contains(out.String(), "old") {
		t.Fatalf("list: %v %q", err, out.String())
	}
	out.Reset()
	if err := MemorySet(context.Background(), c, &out, "answer language", "中文"); err != nil ||
		gotPath != "/v1/me/memories/answer%20language" || !strings.Contains(out.String(), "2 sensitive") {
		t.Fatalf("set: %v %s %q", err, gotPath, out.String())
	}
	if err := MemoryRemove(context.Background(), c, &out, "nope"); err == nil || !strings.Contains(err.Error(), `no personal memory "nope"`) || gotMethod != "DELETE" {
		t.Fatalf("rm: %v", err)
	}
	if MemorySet(context.Background(), c, &out, " ", "v") == nil || MemoryRemove(context.Background(), c, &out, "") == nil {
		t.Fatal("empty key/value must be rejected")
	}
}

func reviewServer(t *testing.T, transitions *[]map[string]any) *httptest.Server {
	t.Helper()
	mems := map[string]string{
		"CANDIDATE": `[{"id":"aaaa1111-0","scope":"service","scope_id":"checkout","memory_type":"semantic","status":"CANDIDATE","version":1,"subject":"note:pool","title":"pool limit","content":"checkout pool max is 150","trust":"T3"},
			{"id":"aaaa2222-0","scope":"service","scope_id":"checkout","memory_type":"semantic","status":"CANDIDATE","version":1,"subject":"note:x","title":"x","content":"x","trust":"T3"},
			{"id":"pers0000-0","scope":"user","scope_id":"me","status":"CANDIDATE","title":"personal"}]`,
		"VALIDATING": `[]`,
		"ACTIVE":     `[{"id":"old00000-0","scope":"service","scope_id":"checkout","status":"ACTIVE","subject":"note:pool","title":"pool limit","content":"checkout pool max is 100"}]`,
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/v1/memories":
			body, ok := mems[r.URL.Query().Get("status")]
			if !ok {
				body = "[]"
			}
			_, _ = w.Write([]byte(`{"memories":` + body + `}`))
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/transition"):
			var b map[string]any
			_ = json.NewDecoder(r.Body).Decode(&b)
			b["path"] = r.URL.Path
			*transitions = append(*transitions, b)
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestMemoryReviewShowsWhatChanges(t *testing.T) {
	var tr []map[string]any
	srv := reviewServer(t, &tr)
	defer srv.Close()
	c, _ := NewClient(srv.URL, "t")
	var out bytes.Buffer
	if err := MemoryReview(context.Background(), c, &out); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if !strings.Contains(s, "pool limit") || !strings.Contains(s, "replaces old00000") || !strings.Contains(s, "max is 100") {
		t.Fatalf("a reviewer must see the ACTIVE memory a proposal replaces:\n%s", s)
	}
	if strings.Contains(s, "personal") {
		t.Fatalf("personal memories are never in the shared review queue:\n%s", s)
	}
	if !strings.Contains(s, "2 to review") {
		t.Fatalf("%s", s)
	}
}

func TestMemoryDecide(t *testing.T) {
	var tr []map[string]any
	srv := reviewServer(t, &tr)
	defer srv.Close()
	c, _ := NewClient(srv.URL, "t")
	var out bytes.Buffer
	if err := MemoryDecide(context.Background(), c, &out, "aaaa1", "approve", ""); err != nil {
		t.Fatal(err)
	}
	if len(tr) != 1 || tr[0]["to"] != "ACTIVE" || tr[0]["expected_status"] != "CANDIDATE" || tr[0]["path"] != "/v1/memories/aaaa1111-0/transition" {
		t.Fatalf("approve must move CANDIDATE to ACTIVE: %+v", tr)
	}
	if err := MemoryDecide(context.Background(), c, &out, "aaaa", "approve", ""); err == nil || !strings.Contains(err.Error(), "matches 2") {
		t.Fatalf("an ambiguous prefix must not pick one: %v", err)
	}
	if err := MemoryDecide(context.Background(), c, &out, "aa", "approve", ""); err == nil {
		t.Fatal("a too-short prefix must be rejected")
	}
	if err := MemoryDecide(context.Background(), c, &out, "aaaa2222", "reject", "wrong"); err != nil || tr[1]["to"] != "REJECTED" || tr[1]["reason"] != "wrong" {
		t.Fatalf("%v %+v", err, tr)
	}
	if err := MemoryDecide(context.Background(), c, &out, "old0", "archive", ""); err != nil || tr[2]["to"] != "ARCHIVED" || tr[2]["expected_status"] != "ACTIVE" {
		t.Fatalf("%v %+v", err, tr)
	}
	if err := MemoryDecide(context.Background(), c, &out, "old0", "approve", ""); err == nil {
		t.Fatal("an ACTIVE memory is not in the review queue")
	}
	if err := MemoryDecide(context.Background(), c, &out, "pers0000", "approve", ""); err == nil {
		t.Fatal("personal memories cannot be approved through the shared queue")
	}
}

func TestCurationCommands(t *testing.T) {
	var decided map[string]any
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Path == "/v1/memory/curation/run":
			_, _ = w.Write([]byte(`{"examined":12,"created":{"duplicate":2},"new_proposals":2,"already_proposed":1,"skipped":["history: no run history available"]}`))
		case r.Method == "GET" && r.URL.Path == "/v1/memory/curation":
			_, _ = w.Write([]byte(`{"proposals":[{"id":"abcd1234-x","signal":"refuted_by_history","status":"PENDING","reason":"Later runs refuted this lesson 2 times",
				"targets":[{"memory_id":"mem00001-x","title":"root cause","content":"traffic spike"}],"evidence":["run:r1","run:r2","memory:mem00001-x@1"]},
				{"id":"abcd9999-x","signal":"duplicate","status":"PENDING","reason":"dup","targets":[{"memory_id":"m2"}]}]}`))
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/decide"):
			path = r.URL.Path
			_ = json.NewDecoder(r.Body).Decode(&decided)
			if strings.Contains(r.URL.Path, "outdated") {
				w.WriteHeader(409)
				_, _ = w.Write([]byte(`{"error":"proposal_outdated","message":"changed"}`))
				return
			}
			_, _ = w.Write([]byte(`{}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	c, _ := NewClient(srv.URL, "t")
	var out bytes.Buffer
	if err := MemoryCurate(context.Background(), c, &out); err != nil || !strings.Contains(out.String(), "2 new suggestion") || !strings.Contains(out.String(), "no run history") {
		t.Fatalf("%v %s", err, out.String())
	}
	out.Reset()
	if err := MemorySuggestions(context.Background(), c, &out); err != nil || !strings.Contains(out.String(), "evidence  run:r1") || !strings.Contains(out.String(), "set aside mem00001") {
		t.Fatalf("a reviewer must see the evidence runs: %v\n%s", err, out.String())
	}
	if err := DecideSuggestion(context.Background(), c, &out, "abcd1", "apply", "ok"); err != nil || decided["decision"] != "apply" || path != "/v1/memory/curation/abcd1234-x/decide" {
		t.Fatalf("%v %v %s", err, decided, path)
	}
	if err := DecideSuggestion(context.Background(), c, &out, "abcd", "apply", ""); err == nil || !strings.Contains(err.Error(), "matches 2") {
		t.Fatalf("an ambiguous prefix must not pick one: %v", err)
	}
	if err := DecideSuggestion(context.Background(), c, &out, "zzzz", "dismiss", ""); err == nil {
		t.Fatal("an unknown id must fail")
	}
}
