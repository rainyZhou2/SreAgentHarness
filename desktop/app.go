package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"

	"github.com/wailsapp/wails/v2/pkg/runtime"

	"go-sre-harness/desktop/harness"
)

// Settings is the saved connection. The bearer token is kept in memory only
// and never written to disk.
type Settings struct {
	ServerURL string              `json:"server_url"`
	Creds     harness.Credentials `json:"creds"`
}

// App is bound to the UI: every exported method is callable from JavaScript
// as window.go.main.App.<Method>. All HTTP traffic happens here, in Go, so
// the web view never talks to the network itself.
type App struct {
	ctx context.Context

	mu        sync.Mutex
	settings  Settings
	client    *harness.Client
	watchRun  string
	stopWatch context.CancelFunc

	// emit delivers an event to the UI (Wails runtime events in the window,
	// server-sent events in browser preview mode).
	emit func(name string, data any)
	// native is false in browser preview mode (no Wails runtime).
	native    bool
	inboxKick chan struct{}
}

func NewApp() *App { return &App{} }

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.native = a.emit == nil
	a.inboxKick = make(chan struct{}, 1)
	if a.emit == nil {
		a.emit = func(name string, data any) { runtime.EventsEmit(ctx, name, data) }
	}
	a.settings = Settings{ServerURL: "http://127.0.0.1:8080",
		Creds: harness.Credentials{Mode: "dev", Tenant: "tenant-a", Actor: "alice", Roles: "operator,approver"}}
	if b, err := os.ReadFile(settingsPath()); err == nil {
		_ = json.Unmarshal(b, &a.settings)
	}
	a.settings.Creds.Token = ""
	if a.settings.Creds.Mode == "dev" {
		a.client, _ = harness.New(a.settings.ServerURL, a.settings.Creds)
	}
	go a.watchApprovals()
}

func (a *App) shutdown(context.Context) { a.UnwatchRun() }

func settingsPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		dir = os.TempDir()
	}
	return filepath.Join(dir, "sre-harness-desktop", "settings.json")
}

// GetSettings returns the current connection settings (without the token).
func (a *App) GetSettings() Settings {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.settings
	s.Creds.Token = ""
	return s
}

// Connected reports whether a usable client is configured.
func (a *App) Connected() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.client != nil
}

// Connect validates the settings against the server (GET /v1/whoami) and,
// on success, saves them and returns the principal.
func (a *App) Connect(s Settings) (map[string]any, error) {
	c, err := harness.New(s.ServerURL, s.Creds)
	if err != nil {
		return nil, err
	}
	var who map[string]any
	if err := c.Get(a.ctx, "/v1/whoami", &who); err != nil {
		return nil, fmt.Errorf("cannot sign in: %w", err)
	}
	a.UnwatchRun()
	a.mu.Lock()
	a.client, a.settings = c, s
	a.mu.Unlock()
	saved := s
	saved.Creds.Token = ""
	if b, err := json.MarshalIndent(saved, "", "  "); err == nil {
		_ = os.MkdirAll(filepath.Dir(settingsPath()), 0o700)
		_ = os.WriteFile(settingsPath(), b, 0o600)
	}
	return who, nil
}

func (a *App) c() (*harness.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client == nil {
		return nil, errors.New("not connected: open Connection and sign in")
	}
	return a.client, nil
}

func (a *App) get(path string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, c.Get(a.ctx, path, &out)
}

// Whoami returns the authenticated principal.
func (a *App) Whoami() (map[string]any, error) { return a.get("/v1/whoami") }

// ListSessions lists the tenant's conversations, most recently used first.
func (a *App) ListSessions(limit int) (map[string]any, error) {
	if limit <= 0 {
		limit = 100
	}
	return a.get(fmt.Sprintf("/v1/sessions?limit=%d", limit))
}

// CreateSession starts a conversation; an empty profile uses the server default.
func (a *App) CreateSession(title, profileID string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"title": title}
	if profileID != "" {
		body["profile_id"] = profileID
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/sessions", body, &out)
}

// TurnView is one exchange in a conversation: the user's text and the run
// that answers it.
type TurnView struct {
	ID        string `json:"id"`
	Seq       int64  `json:"seq"`
	Status    string `json:"status"`
	RunID     string `json:"run_id"`
	Text      string `json:"text"`
	CreatedAt string `json:"created_at"`
}

// Transcript returns the session and its turns (default branch) in order.
func (a *App) Transcript(sessionID string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	base := "/v1/sessions/" + url.PathEscape(sessionID)
	var view struct {
		Session struct {
			DefaultBranchID string `json:"default_branch_id"`
		} `json:"session"`
	}
	var raw map[string]any
	if err := c.Get(a.ctx, base, &raw); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(raw)
	_ = json.Unmarshal(b, &view)
	branch := view.Session.DefaultBranchID

	var turns struct {
		Turns []struct {
			ID        string  `json:"id"`
			Seq       int64   `json:"seq"`
			Status    string  `json:"status"`
			RunID     *string `json:"run_id"`
			CreatedAt string  `json:"created_at"`
		} `json:"turns"`
	}
	if err := c.Get(a.ctx, base+"/turns?branch_id="+url.QueryEscape(branch), &turns); err != nil {
		return nil, err
	}
	var msgs struct {
		Messages []struct {
			Message struct {
				TurnID string `json:"turn_id"`
				Role   string `json:"role"`
			} `json:"message"`
			Content struct {
				Goal string `json:"goal"`
			} `json:"content"`
		} `json:"messages"`
	}
	if branch != "" {
		if err := c.Get(a.ctx, base+"/branches/"+url.PathEscape(branch)+"/messages", &msgs); err != nil {
			return nil, err
		}
	}
	text := map[string]string{}
	for _, m := range msgs.Messages {
		if m.Message.Role == "user" {
			text[m.Message.TurnID] = m.Content.Goal
		}
	}
	out := make([]TurnView, 0, len(turns.Turns))
	for _, t := range turns.Turns {
		tv := TurnView{ID: t.ID, Seq: t.Seq, Status: t.Status, Text: text[t.ID], CreatedAt: t.CreatedAt}
		if t.RunID != nil {
			tv.RunID = *t.RunID
		}
		if tv.Text == "" && tv.RunID != "" { // queued turns have no message yet: read the run goal
			var run map[string]any
			if c.Get(a.ctx, "/v1/runs/"+url.PathEscape(tv.RunID), &run) == nil {
				tv.Text, _ = run["goal"].(string)
			}
		}
		out = append(out, tv)
	}
	return map[string]any{"view": raw, "branch_id": branch, "turns": out}, nil
}

// SendMessage submits a turn; the server queues it if a turn is running.
func (a *App) SendMessage(sessionID, text string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"text": text, "client_request_id": harness.NewKey()}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/sessions/"+url.PathEscape(sessionID)+"/turns", body, &out)
}

// RunView gathers what the chat shows for one run: the run and result, its
// steps with inputs and outputs, approvals and cost.
func (a *App) RunView(runID string) (map[string]any, error) {
	base := "/v1/runs/" + url.PathEscape(runID)
	parts := map[string]string{"run": base, "steps": base + "/steps?expand=io", "approvals": base + "/approvals", "cost": base + "/cost"}
	out := map[string]any{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error
	for key, path := range parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := a.get(path)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if key == "run" {
					firstErr = err
				}
				return
			}
			out[key] = v
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// CommandRequest is a run command from the UI. ExpectedVersion 0 means
// "not sent".
type CommandRequest struct {
	Type            string         `json:"type"`
	ExpectedVersion int64          `json:"expected_version"`
	Reason          string         `json:"reason"`
	Payload         map[string]any `json:"payload"`
}

// Command sends POST /v1/runs/{id}/commands.
func (a *App) Command(runID string, req CommandRequest) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"type": req.Type, "payload": req.Payload}
	if req.Payload == nil {
		body["payload"] = map[string]any{}
	}
	if req.ExpectedVersion > 0 {
		body["expected_version"] = req.ExpectedVersion
	}
	if req.Reason != "" {
		body["reason"] = req.Reason
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/commands", body, &out)
}

// Plugins returns the server's plugin inventory (admin only).
func (a *App) Plugins() (map[string]any, error) { return a.get("/v1/admin/plugins") }

// WatchRun follows the run's event stream and emits every event to the UI
// as "run:event" ({run_id, event}) and model deltas as "run:delta"
// ({run_id, delta}); it replaces any previous watch.
func (a *App) WatchRun(runID string, afterSeq int64) error {
	c, err := a.c()
	if err != nil {
		return err
	}
	a.UnwatchRun()
	ctx, cancel := context.WithCancel(a.ctx)
	a.mu.Lock()
	a.watchRun, a.stopWatch = runID, cancel
	a.mu.Unlock()
	go func() {
		err := c.StreamEvents(ctx, runID, afterSeq, func(e harness.Event) {
			a.emit("run:event", map[string]any{"run_id": runID, "event": e})
		}, func(d harness.Delta) {
			a.emit("run:delta", map[string]any{"run_id": runID, "delta": d})
		})
		if err != nil && ctx.Err() == nil {
			a.emit("run:stream-error", map[string]any{"run_id": runID, "error": err.Error()})
		}
	}()
	return nil
}

// UnwatchRun stops the current event stream.
func (a *App) UnwatchRun() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stopWatch != nil {
		a.stopWatch()
		a.stopWatch, a.watchRun = nil, ""
	}
}

// Extensions lists the tenant's MCP servers, HTTP tools and skills with
// their load status. Header and env values come back blank (write-only).
func (a *App) Extensions() (map[string]any, error) { return a.get("/v1/extensions") }

// Tools lists the tools runs of this tenant can see, with whether the
// default profile allows each.
func (a *App) Tools() (map[string]any, error) { return a.get("/v1/tools") }

var extKinds = map[string]bool{"mcp-servers": true, "http-tools": true, "skills": true, "models": true}

// PutExtension creates or replaces an extension (admin). kind is
// mcp-servers, http-tools or skills; expectedVersion 0 means create-only,
// a positive value is an optimistic-concurrency check.
func (a *App) PutExtension(kind, name string, spec map[string]any, enabled bool, expectedVersion int64) (map[string]any, error) {
	if !extKinds[kind] {
		return nil, fmt.Errorf("unknown extension kind %q", kind)
	}
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"spec": spec, "enabled": enabled, "expected_version": expectedVersion}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPut, "/v1/extensions/"+kind+"/"+url.PathEscape(name), body, &out)
}

// DeleteExtension removes an extension (admin).
func (a *App) DeleteExtension(kind, name string) error {
	if !extKinds[kind] {
		return fmt.Errorf("unknown extension kind %q", kind)
	}
	c, err := a.c()
	if err != nil {
		return err
	}
	return c.Do(a.ctx, http.MethodDelete, "/v1/extensions/"+kind+"/"+url.PathEscape(name), nil, nil)
}

// ProbeMCP connects to an MCP server and lists its tools without saving
// anything (admin). A nil spec probes the stored server of that name.
func (a *App) ProbeMCP(name string, spec map[string]any) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var body any
	if spec != nil {
		body = map[string]any{"spec": spec}
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/extensions/mcp-servers/"+url.PathEscape(name)+"/probe", body, &out)
}

// TestModel sends a one-line request through a model (admin): the stored
// one when spec is nil, otherwise spec with blank secrets taken from it.
func (a *App) TestModel(name string, spec map[string]any) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var body any
	if spec != nil {
		body = map[string]any{"spec": spec}
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/extensions/models/"+url.PathEscape(name)+"/test", body, &out)
}
