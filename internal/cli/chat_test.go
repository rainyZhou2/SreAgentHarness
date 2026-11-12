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
