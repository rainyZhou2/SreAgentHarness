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
	"strings"
	"sync"

	"github.com/wailsapp/wails/v2/pkg/runtime"

	"go-sre-harness/desktop/harness"
	"go-sre-harness/desktop/localruntime"
)

// Settings is the saved connection. The bearer token is kept in memory only
// and never written to disk.
type Settings struct {
	ServerURL string              `json:"server_url"`
	Creds     harness.Credentials `json:"creds"`
	// Mode is "local" (supervised local runtime) or "remote" (team server).
	Mode string `json:"mode,omitempty"`
	// LocalDataDir is the per-user directory for PostgreSQL, artifacts and logs.
	LocalDataDir string `json:"local_data_dir,omitempty"`
	// Optional local model selection. The key is kept in the OS keychain.
	ModelProvider string `json:"model_provider,omitempty"`
	ModelBaseURL  string `json:"model_base_url,omitempty"`
	ModelName     string `json:"model_name,omitempty"`
	// Language of the interface, notifications and exports: "en" or "zh"
	// (empty: the UI follows the system language).
	Language string `json:"language,omitempty"`
	Preferences
	// SignedOut keeps a dev-mode connection from reconnecting at startup.
	SignedOut bool `json:"signed_out,omitempty"`
}

// Preferences are the app's own settings (Settings → Appearance and
// Notifications). Zero values are the defaults.
type Preferences struct {
	Theme       string `json:"theme,omitempty"`        // "" (system) | light | dark
	NoGlass     bool   `json:"no_glass,omitempty"`     // opaque surfaces instead of glass
	NotifyOff   bool   `json:"notify_off,omitempty"`   // no system notifications for approvals
	PollSeconds int    `json:"poll_seconds,omitempty"` // approvals inbox refresh; 0 = 8s
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
	// local supervises the bundled PostgreSQL + harness core in local mode.
	local   LocalRuntime
	secrets localruntime.SecretStore
}

func NewApp() *App {
	return &App{secrets: localruntime.DefaultSecretStore()}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.native = a.emit == nil
	a.inboxKick = make(chan struct{}, 1)
	if a.emit == nil {
		a.emit = func(name string, data any) { runtime.EventsEmit(ctx, name, data) }
	}
	a.settings = defaultSettings()
	if b, err := os.ReadFile(settingsPath()); err == nil {
		_ = json.Unmarshal(b, &a.settings)
	}
	if a.settings.Mode == "" {
		a.settings.Mode = "local"
	}
	if a.settings.LocalDataDir == "" {
		a.settings.LocalDataDir = defaultLocalDataDir()
	}
	a.settings.Creds.Token = ""
	if a.settings.Mode != "local" && a.settings.Creds.Mode == "dev" && !a.settings.SignedOut {
		a.client, _ = harness.New(a.settings.ServerURL, a.settings.Creds)
	}
	go a.watchApprovals()
}

func (a *App) shutdown(context.Context) {
	a.UnwatchRun()
	a.stopLocal()
}

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
		a.logEvent("error", "sign in to %s failed: %v", s.ServerURL, err)
		return nil, fmt.Errorf("cannot sign in: %w", err)
	}
	a.UnwatchRun()
	a.stopLocal()
	a.mu.Lock()
	// The connection dialog carries only the connection; keep the rest.
	if s.Language == "" {
		s.Language = a.settings.Language
	}
	if s.LocalDataDir == "" {
		s.LocalDataDir = a.settings.LocalDataDir
	}
	if s.ModelProvider == "" {
		s.ModelProvider, s.ModelBaseURL, s.ModelName = a.settings.ModelProvider, a.settings.ModelBaseURL, a.settings.ModelName
	}
	s.Mode = "remote"
	s.Preferences, s.SignedOut = a.settings.Preferences, false
	a.client, a.settings = c, s
	a.mu.Unlock()
	a.saveSettings()
	a.logEvent("info", "signed in to %s as %v (%s)", s.ServerURL, who["subject"], s.Creds.Mode)
	a.RefreshApprovals()
	return who, nil
}

// SetLanguage saves the interface language ("en" or "zh").
func (a *App) SetLanguage(lang string) error {
	if lang != "en" && lang != "zh" {
		return fmt.Errorf("unsupported language %q", lang)
	}
	a.mu.Lock()
	a.settings.Language = lang
	a.mu.Unlock()
	return a.saveSettings()
}

// saveSettings writes the settings without the token.
func (a *App) saveSettings() error {
	a.mu.Lock()
	saved := a.settings
	a.mu.Unlock()
	saved.Creds.Token = ""
	b, err := json.MarshalIndent(saved, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(settingsPath()), 0o700); err != nil {
		return err
	}
	return os.WriteFile(settingsPath(), b, 0o600)
}

// zh reports whether the user chose Chinese.
func (a *App) zh() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.settings.Language == "zh"
}

// tr picks the English or Chinese text.
func (a *App) tr(en, zh string) string {
	if a.zh() {
		return zh
	}
	return en
}

// trLocked is tr for callers that hold a.mu.
func (a *App) trLocked(en, zh string) string {
	if a.settings.Language == "zh" {
		return zh
	}
	return en
}

func (a *App) c() (*harness.Client, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.client == nil {
		return nil, errors.New(a.trLocked("not connected: open Connection and sign in", "未连接：请打开“连接”并登录"))
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

// CreateSession starts a conversation; an empty profile lets the server
// choose its automatic routing mode.
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
	parts := map[string]string{"run": base, "steps": base + "/steps?expand=io", "approvals": base + "/approvals", "cost": base + "/cost",
		"flow": base + "/flow"}
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
	if err := c.Do(a.ctx, http.MethodPost, "/v1/runs/"+url.PathEscape(runID)+"/commands", body, &out); err != nil {
		a.logEvent("warn", "command %s on run %s failed: %v", req.Type, runID, err)
		return nil, err
	}
	return out, nil
}

// ProvideInput answers a flow input node.
func (a *App) ProvideInput(runID string, expectedVersion int64, value any) (map[string]any, error) {
	return a.Command(runID, CommandRequest{Type: "provide_input", ExpectedVersion: expectedVersion,
		Payload: map[string]any{"value": value}})
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
			a.logEvent("warn", "event stream of run %s stopped: %v", runID, err)
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

// GraphSearch finds capability, memory, service, run and step nodes.
func (a *App) GraphSearch(q string, kinds []string, limit int) (map[string]any, error) {
	v := url.Values{}
	if q != "" {
		v.Set("q", q)
	}
	if len(kinds) > 0 {
		v.Set("kinds", strings.Join(kinds, ","))
	}
	if limit > 0 {
		v.Set("limit", fmt.Sprint(limit))
	}
	return a.get("/v1/graph/search?" + v.Encode())
}

// GraphNeighborhood returns the nodes and edges within depth of ref.
func (a *App) GraphNeighborhood(ref string, depth int) (map[string]any, error) {
	v := url.Values{"ref": {ref}}
	if depth > 0 {
		v.Set("depth", fmt.Sprint(depth))
	}
	return a.get("/v1/graph/neighborhood?" + v.Encode())
}

// Capabilities lists the tenant's projected capabilities with recent usage.
func (a *App) Capabilities(days int) (map[string]any, error) {
	v := url.Values{}
	if days > 0 {
		v.Set("days", fmt.Sprint(days))
	}
	return a.get("/v1/capabilities?" + v.Encode())
}

// GraphRelate declares a relation between two graph nodes (admin).
func (a *App) GraphRelate(src, dst, relation string, weight float64) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	body := map[string]any{"src_ref": src, "dst_ref": dst, "relation": relation}
	if weight > 0 {
		body["weight"] = weight
	}
	return out, c.Do(a.ctx, http.MethodPost, "/v1/graph/relations", body, &out)
}

// GraphUnrelate removes a declared relation (admin).
func (a *App) GraphUnrelate(src, dst, relation string) error {
	c, err := a.c()
	if err != nil {
		return err
	}
	v := url.Values{"src": {src}, "dst": {dst}, "relation": {relation}}
	return c.Do(a.ctx, http.MethodDelete, "/v1/graph/relations?"+v.Encode(), nil, nil)
}

// SetToolEnabled turns one tool on or off for the tenant (admin).
func (a *App) SetToolEnabled(name string, enabled bool) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPut, "/v1/tools/"+url.PathEscape(name), map[string]any{"enabled": enabled}, &out)
}

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
