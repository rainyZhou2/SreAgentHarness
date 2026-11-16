package localruntime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProbeCore(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	if !probeCore(server.URL) {
		t.Fatal("healthy core not detected")
	}
	if probeCore(server.URL + "/missing") {
		t.Fatal("unhealthy core reported as healthy")
	}
}

func TestTerminateStaleDropsUnhealthyState(t *testing.T) {
	dir := t.TempDir()
	manager := &Manager{cfg: Config{DataDir: dir}}
	state, err := json.Marshal(runtimeState{PID: 1, URL: "http://127.0.0.1:1", StartedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, stateFile)
	if err := os.WriteFile(path, state, 0o600); err != nil {
		t.Fatal(err)
	}
	manager.terminateStale()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("stale state was not removed: %v", err)
	}
}
