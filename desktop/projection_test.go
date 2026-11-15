package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go-sre-harness/desktop/harness"
)

func TestGetRunProjectionPreservesBlockFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/runs/run-1/projection" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema_version":1,"projection_version":1,"run_id":"run-1","status":"RUNNING","blocks":[{"id":"b1","kind":"evidence","ordinal":0,"signal":"metrics","source":"prom-prod","status":"ok"},{"id":"b2","kind":"conclusion","ordinal":1,"verdict":"UNVERIFIED","answer":"pending"}]}`))
	}))
	defer server.Close()

	client, err := harness.New(server.URL, harness.Credentials{Mode: "bearer", Token: "test-token"})
	if err != nil {
		t.Fatal(err)
	}
	app := &App{ctx: context.Background(), client: client}
	view, err := app.GetRunProjection("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Blocks) != 2 || view.Blocks[0].Kind != "evidence" || view.Blocks[1].Kind != "conclusion" {
		t.Fatalf("blocks = %+v", view.Blocks)
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Blocks []map[string]any `json:"blocks"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Blocks[0]["signal"] != "metrics" || decoded.Blocks[0]["status"] != "ok" {
		t.Fatalf("block fields lost: %+v", decoded.Blocks[0])
	}
	if decoded.Blocks[1]["verdict"] != "UNVERIFIED" || decoded.Blocks[1]["answer"] != "pending" {
		t.Fatalf("conclusion fields lost: %+v", decoded.Blocks[1])
	}
}
