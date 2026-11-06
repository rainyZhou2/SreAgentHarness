package harness

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseSSE(t *testing.T) {
	in := "id: 1\nevent: run.created\ndata: {\"a\":1}\n\n: keepalive\n\nevent: model.delta\ndata: x\n\nid: 2\nevent: e2\ndata: l1\ndata: l2\n\n"
	var got []string
	if err := ParseSSE(strings.NewReader(in), func(id, ev, data string) { got = append(got, id+"|"+ev+"|"+data) }); err != nil {
		t.Fatal(err)
	}
	want := []string{"1|run.created|{\"a\":1}", "1|model.delta|x", "2|e2|l1\nl2"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %q", got)
	}
}

func TestNewValidates(t *testing.T) {
	for _, tc := range []struct {
		url string
		c   Credentials
	}{
		{"ftp://x", Credentials{Mode: "dev", Tenant: "t", Actor: "a"}},
		{"http://x", Credentials{Mode: "dev", Tenant: "t"}},
		{"http://x", Credentials{Mode: "bearer"}},
		{"http://x", Credentials{Mode: "other"}},
	} {
		if _, err := New(tc.url, tc.c); err == nil {
			t.Fatalf("expected error for %+v", tc)
		}
	}
}

func TestDoHeadersAndErrors(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		if r.URL.Path == "/v1/runs" && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"error":"forbidden","message":"role operator required"}`)
			return
		}
		fmt.Fprint(w, `{"tenant":"t1"}`)
	}))
	defer srv.Close()

	c, err := New(srv.URL+"/", Credentials{Mode: "dev", Tenant: "t1", Actor: "alice", Roles: "viewer"})
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := c.Get(context.Background(), "/v1/whoami", &out); err != nil || out["tenant"] != "t1" {
		t.Fatalf("%v %v", out, err)
	}
	if seen.Get("X-Tenant-ID") != "t1" || seen.Get("X-Actor-ID") != "alice" || seen.Get("Idempotency-Key") != "" {
		t.Fatalf("headers %v", seen)
	}
	err = c.Do(context.Background(), http.MethodPost, "/v1/runs", map[string]string{"goal": "x"}, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 403 || apiErr.Kind != "forbidden" {
		t.Fatalf("err %v", err)
	}
	if !strings.HasPrefix(seen.Get("Idempotency-Key"), "desktop-") {
		t.Fatalf("missing idempotency key: %v", seen)
	}

	b, _ := New(srv.URL, Credentials{Mode: "bearer", Token: "s3cret"})
	_ = b.Get(context.Background(), "/v1/whoami", nil)
	if seen.Get("Authorization") != "Bearer s3cret" || seen.Get("X-Tenant-ID") != "" {
		t.Fatalf("bearer headers %v", seen)
	}
}

// The stream resumes from the last delivered event after a disconnect.
func TestStreamEventsResumes(t *testing.T) {
	var mu sync.Mutex
	var lastIDs []string
	conn := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		conn++
		n := conn
		lastIDs = append(lastIDs, r.Header.Get("Last-Event-ID"))
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		start := 1
		if n > 1 {
			start = 3
		}
		for seq := start; seq < start+2; seq++ {
			fmt.Fprintf(w, "id: %d\nevent: e\ndata: {\"event_seq\":%d,\"event_type\":\"e\"}\n\n", seq, seq)
		}
		// first connection also replays an old event that must be skipped on reconnect
		if n > 1 {
			fmt.Fprint(w, "id: 2\nevent: e\ndata: {\"event_seq\":2,\"event_type\":\"e\"}\n\n")
		}
	}))
	defer srv.Close()
	c, _ := New(srv.URL, Credentials{Mode: "dev", Tenant: "t", Actor: "a"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []int64
	_ = c.StreamEvents(ctx, "r1", 0, func(e Event) {
		got = append(got, e.Seq)
		if len(got) == 4 {
			cancel()
		}
	})
	if fmt.Sprint(got) != "[1 2 3 4]" {
		t.Fatalf("got %v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if lastIDs[0] != "0" || lastIDs[1] != "2" {
		t.Fatalf("Last-Event-ID %v", lastIDs)
	}
}
