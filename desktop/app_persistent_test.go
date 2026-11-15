package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go-sre-harness/desktop/harness"
	"go-sre-harness/pkg/eventcursor"
)

func TestWatchRunCommitsPersistentCursor(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/whoami", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant": "tenant", "subject": "subject"})
	})
	mux.HandleFunc("GET /api/runs/run/events/stream", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"event_seq\":1,\"event_type\":\"run.updated\",\"payload\":{}}\n\n")
		fmt.Fprint(w, "event: stream.end\ndata: {}\n\n")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	client, err := harness.New(srv.URL, harness.Credentials{Mode: "bearer", Token: "token"})
	if err != nil {
		t.Fatal(err)
	}
	store, err := eventcursor.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	emitted := make(chan struct{}, 1)
	app := &App{
		ctx:         context.Background(),
		client:      client,
		cursorStore: store,
		emit: func(name string, data any) {
			if name == "run:event" {
				emitted <- struct{}{}
			}
		},
	}
	if err := app.WatchRun("run", 0); err != nil {
		t.Fatal(err)
	}
	defer app.UnwatchRun()
	select {
	case <-emitted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for run event")
	}
	key := eventcursor.Key{ServerURL: srv.URL, TenantID: "tenant", SubjectID: "subject", Stream: "run", ResourceID: "run", Purpose: "events-v1"}
	deadline := time.Now().Add(time.Second)
	for {
		cursor, ok, loadErr := store.Load(context.Background(), key)
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if ok && cursor.Sequence == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cursor=%+v ok=%v", cursor, ok)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestWatchActiveDoesNotOverridePersistentCursor(t *testing.T) {
	data, err := os.ReadFile("frontend/dist/app.js")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "api().WatchRun(t.run_id, 0)") {
		t.Fatal("watchActive must leave resume position to the persistent cursor")
	}
}
