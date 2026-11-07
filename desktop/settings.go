package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"time"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// Version is the client version shown in Settings → About (build.sh copies
// it into the app bundle).
var Version = "0.2.0"

// ---------- preferences ----------

// SetPreferences saves the appearance and notification settings.
func (a *App) SetPreferences(p Preferences) error {
	switch p.Theme {
	case "", "light", "dark":
	default:
		return fmt.Errorf("unsupported theme %q", p.Theme)
	}
	if p.PollSeconds != 0 && (p.PollSeconds < 3 || p.PollSeconds > 300) {
		return fmt.Errorf("refresh interval must be 3–300 seconds")
	}
	a.mu.Lock()
	a.settings.Preferences = p
	a.mu.Unlock()
	a.RefreshApprovals() // apply a new interval now
	return a.saveSettings()
}

// Disconnect signs out: the client is dropped and a dev-mode connection
// is not restored at the next start. Saved connection fields stay so
// signing in again only needs Connect.
func (a *App) Disconnect() error {
	a.UnwatchRun()
	a.stopLocal()
	a.mu.Lock()
	a.client = nil
	a.settings.Creds.Token = ""
	a.settings.SignedOut = true
	a.mu.Unlock()
	a.logEvent("info", "signed out")
	a.emit("approvals:update", []any{})
	return a.saveSettings()
}

// ---------- models ----------

// ListModels asks the provider which models a spec's key can use (admin):
// the stored model when spec is nil, otherwise spec with blank secrets
// taken from it.
func (a *App) ListModels(name string, spec map[string]any) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var body any
	if spec != nil {
		body = map[string]any{"spec": spec}
	}
	var out map[string]any
	if err := c.Do(a.ctx, http.MethodPost, "/v1/extensions/models/"+url.PathEscape(name)+"/list", body, &out); err != nil {
		a.logEvent("warn", "listing models for %s failed: %v", name, err)
		return nil, err
	}
	return out, nil
}

// ---------- about and diagnostics ----------

type logEntry struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
}

// eventLog keeps the last client events in memory for diagnostics. It never
// holds request bodies or credentials.
var eventLog struct {
	sync.Mutex
	entries []logEntry
}

const maxLogEntries = 300

func (a *App) logEvent(level, format string, args ...any) {
	eventLog.Lock()
	defer eventLog.Unlock()
	eventLog.entries = append(eventLog.entries, logEntry{Time: time.Now(), Level: level, Msg: fmt.Sprintf(format, args...)})
	if n := len(eventLog.entries); n > maxLogEntries {
		eventLog.entries = append([]logEntry(nil), eventLog.entries[n-maxLogEntries:]...)
	}
}

// RecentLog returns the newest client events first.
func (a *App) RecentLog(limit int) []logEntry {
	eventLog.Lock()
	defer eventLog.Unlock()
	if limit <= 0 || limit > len(eventLog.entries) {
		limit = len(eventLog.entries)
	}
	out := make([]logEntry, 0, limit)
	for i := len(eventLog.entries) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, eventLog.entries[i])
	}
	return out
}

// About describes the client, the configured server and its health.
func (a *App) About() map[string]any {
	a.mu.Lock()
	s := a.settings
	connected := a.client != nil
	a.mu.Unlock()
	out := map[string]any{
		"version": Version, "os": goruntime.GOOS, "arch": goruntime.GOARCH, "go": goruntime.Version(),
		"settings_path": settingsPath(), "server_url": s.ServerURL, "auth_mode": s.Creds.Mode, "connected": connected,
	}
	server := map[string]any{}
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(s.ServerURL, "/")+"/healthz", nil)
	if err == nil {
		var resp *http.Response
		if resp, err = http.DefaultClient.Do(req); err == nil {
			resp.Body.Close()
			server["status"] = resp.StatusCode
			server["ok"] = resp.StatusCode == http.StatusOK
			server["latency_ms"] = time.Since(start).Milliseconds()
		}
	}
	if err != nil {
		server["ok"], server["error"] = false, err.Error()
	}
	out["server"] = server
	if connected {
		if who, err := a.Whoami(); err == nil {
			out["whoami"] = who
		}
	}
	return out
}

// ExportDiagnostics saves the about information and recent client events
// as JSON (no token, no chat content) and returns the file path.
func (a *App) ExportDiagnostics() (string, error) {
	a.mu.Lock()
	s := a.settings
	a.mu.Unlock()
	s.Creds.Token = ""
	data, err := json.MarshalIndent(map[string]any{"exported_at": time.Now().Format(time.RFC3339), "about": a.About(),
		"settings": s, "events": a.RecentLog(0)}, "", "  ")
	if err != nil {
		return "", err
	}
	name := "sre-harness-diagnostics-" + time.Now().Format("20060102-150405") + ".json"
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, "Downloads", name)
	if a.native {
		path, err = runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{DefaultFilename: name,
			Title: a.tr("Export diagnostics", "导出诊断信息"), DefaultDirectory: filepath.Join(home, "Downloads"),
			Filters: []runtime.FileFilter{{DisplayName: "JSON", Pattern: "*.json"}}})
		if err != nil || path == "" {
			return "", err
		}
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return "", err
	}
	a.logEvent("info", "diagnostics exported to %s", path)
	return path, nil
}
