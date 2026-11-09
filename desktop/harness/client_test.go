package harness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDoWithHeaders(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c, err := New(srv.URL, Credentials{Mode: "bearer", Token: "tok"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// Writes get a fresh key by default.
	if err := c.Do(ctx, http.MethodPost, "/x", map[string]any{}, nil); err != nil {
		t.Fatal(err)
	}
	first := got.Get("Idempotency-Key")
	if first == "" || got.Get("Authorization") != "Bearer tok" {
		t.Fatalf("defaults: %v", got)
	}
	// A caller-chosen key and If-Match replace the defaults.
	h := http.Header{"Idempotency-Key": {"postmortem:w1"}, "If-Match": {`"7"`}}
	var out map[string]any
	if err := c.DoWith(ctx, http.MethodPost, "/x", h, nil, &out); err != nil {
		t.Fatal(err)
	}
	if got.Get("Idempotency-Key") != "postmortem:w1" || len(got.Values("Idempotency-Key")) != 1 || got.Get("If-Match") != `"7"` || out["ok"] != true {
		t.Fatalf("override: %v %v", got, out)
	}
}
