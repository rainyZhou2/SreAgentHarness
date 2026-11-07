# Local-First Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship a downloadable desktop app that starts a complete harness locally with no Docker, system PostgreSQL, NATS or CLI, while keeping the same client able to connect to a remote team server.

**Architecture:** The desktop app supervises two child processes: a bundled PostgreSQL and the existing `harness all` core. The core listens on loopback with a random bearer token; the desktop UI talks to it through the existing `/v1` HTTP/SSE client. Remote mode swaps the endpoint and credential source without changing the UI.

**Tech Stack:** Go 1.26, Wails v2, existing `internal/app` core, PostgreSQL 16 binaries bundled with the app, `go-keyring` for model credentials, existing static HTML/CSS/JS UI.

**Spec:** `docs/superpowers/specs/2026-11-07-local-first-harness-design.md`

> **Phase 2 update:** local mode now uses `internal/localstore` (SQLite) and `cmd/harness-local`; the embedded-PostgreSQL tasks in this plan were the Phase 1 bridge and have been superseded. The desktop local/remote contract remains the same.

## Global Constraints

- Local listeners bind `127.0.0.1` only.
- Local auth is token auth; never `dev` header auth.
- The local token is random per launch and only its SHA-256 is persisted.
- No external PostgreSQL, Docker, NATS or CLI is required by the end user.
- The existing `/v1` API and desktop UI remain the client contract.
- Local mode must reuse the existing store, migrations, kernel, flow and plan review.
- Remote mode must continue to work with OIDC or bearer tokens.
- Every task ends with a focused commit.

---

### Task 1: Local runtime manager skeleton

**Files:**
- Create: `desktop/localruntime/manager.go`
- Create: `desktop/localruntime/manager_test.go`
- Modify: `desktop/go.mod`

**Interfaces:**
- Produces:
  ```go
  type Config struct {
      HarnessBin     string
      PostgresBin    string
      InitDBBin      string
      DataDir        string
      ModelEnv       map[string]string
      StartupTimeout time.Duration
  }

  type Endpoint struct {
      URL   string
      Token string
      PID   int
  }

  type Manager struct { /* ... */ }
  func New(Config) (*Manager, error)
  func (m *Manager) Start(ctx context.Context) (Endpoint, error)
  func (m *Manager) Stop(ctx context.Context) error
  func (m *Manager) Status() string
  ```

- [ ] **Step 1: Write the failing validation test**

```go
func TestNewRequiresBinsAndDataDir(t *testing.T) {
    if _, err := New(Config{}); err == nil {
        t.Fatal("empty config must fail")
    }
    if _, err := New(Config{HarnessBin: "h", PostgresBin: "p", InitDBBin: "i"}); err == nil {
        t.Fatal("missing data dir must fail")
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd desktop && go test ./localruntime -run TestNewRequiresBinsAndDataDir -v`  
Expected: FAIL because package/type does not exist.

- [ ] **Step 3: Implement the skeleton**

```go
package localruntime

import (
    "context"
    "errors"
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "time"
)

type Config struct {
    HarnessBin     string
    PostgresBin    string
    InitDBBin      string
    DataDir        string
    ModelEnv       map[string]string
    StartupTimeout time.Duration
}

type Endpoint struct {
    URL   string
    Token string
    PID   int
}

type Manager struct {
    cfg     Config
    mu      sync.Mutex
    status  string
    core    *exec.Cmd
    pg      *exec.Cmd
    token   string
    baseURL string
    pgPort  int
}

func New(cfg Config) (*Manager, error) {
    if cfg.HarnessBin == "" || cfg.PostgresBin == "" || cfg.InitDBBin == "" || cfg.DataDir == "" {
        return nil, errors.New("local runtime needs harness/postgres/initdb binaries and a data dir")
    }
    if cfg.StartupTimeout <= 0 {
        cfg.StartupTimeout = 90 * time.Second
    }
    if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
        return nil, fmt.Errorf("create data dir: %w", err)
    }
    return &Manager{cfg: cfg, status: "stopped"}, nil
}

func (m *Manager) Status() string {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.status
}

func (m *Manager) Start(ctx context.Context) (Endpoint, error) {
    return Endpoint{}, errors.New("not implemented")
}

func (m *Manager) Stop(ctx context.Context) error { return nil }

func (m *Manager) runtimeFile(name string) string {
    return filepath.Join(m.cfg.DataDir, name)
}

func (m *Manager) setStatus(s string) {
    m.mu.Lock()
    m.status = s
    m.mu.Unlock()
}

func (m *Manager) logWriter(name string) io.Writer {
    if err := os.MkdirAll(filepath.Join(m.cfg.DataDir, "logs"), 0o700); err != nil {
        return io.Discard
    }
    f, err := os.OpenFile(filepath.Join(m.cfg.DataDir, "logs", name),
        os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
    if err != nil {
        return io.Discard
    }
    return f
}
```

- [ ] **Step 4: Run the test and build**

Run: `cd desktop && go test ./localruntime -run TestNewRequiresBinsAndDataDir -v && go build ./...`  
Expected: PASS and build succeeds.

- [ ] **Step 5: Commit**

```bash
git add desktop/go.mod desktop/go.sum desktop/localruntime
git commit -m "feat(desktop): add local runtime skeleton"
```

---

### Task 2: Per-launch local token and token file

**Files:**
- Create: `desktop/localruntime/token.go`
- Create: `desktop/localruntime/token_test.go`

**Interfaces:**
- Produces:
  ```go
  func newToken() (plain string, tokenFile []byte, err error)
  func writeTokenFile(path string, data []byte) error
  ```

- [ ] **Step 1: Write the failing test**

```go
func TestTokenFileContainsSHA256Only(t *testing.T) {
    plain, data, err := newToken()
    if err != nil || len(plain) != 64 {
        t.Fatalf("token = %q err=%v", plain, err)
    }
    if strings.Contains(string(data), plain) {
        t.Fatal("token file must not contain the plaintext token")
    }
    var entries []struct {
        SHA256 string   `json:"sha256"`
        Tenant string   `json:"tenant"`
        Subject string  `json:"subject"`
        Roles []string  `json:"roles"`
    }
    if err := json.Unmarshal(data, &entries); err != nil {
        t.Fatal(err)
    }
    if len(entries) != 1 || entries[0].Tenant != "local" || entries[0].SHA256 != auth.HashToken(plain) {
        t.Fatalf("entries = %+v", entries)
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd desktop && go test ./localruntime -run TestTokenFileContainsSHA256Only -v`  
Expected: FAIL because `newToken` is undefined.

- [ ] **Step 3: Implement token generation**

```go
func newToken() (string, []byte, error) {
    b := make([]byte, 32)
    if _, err := rand.Read(b); err != nil {
        return "", nil, err
    }
    plain := hex.EncodeToString(b)
    entry := struct {
        SHA256  string   `json:"sha256"`
        Tenant  string   `json:"tenant"`
        Subject string   `json:"subject"`
        Roles   []string `json:"roles"`
    }{tokenSHA256(plain), "local", "desktop", []string{"operator", "approver", "admin"}}
    data, err := json.Marshal([]any{entry})
    return plain, data, err
}

func tokenSHA256(token string) string {
    sum := sha256.Sum256([]byte(token))
    return hex.EncodeToString(sum[:])
}

func writeTokenFile(path string, data []byte) error {
    if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
        return err
    }
    return os.WriteFile(path, data, 0o600)
}
```

The desktop module stays independent of the root module; it does not import `internal/auth`. `tokenSHA256` is the small local copy of the hash helper.

- [ ] **Step 4: Run tests**

Run: `cd desktop && go test ./localruntime -run 'TestToken|TestNew' -v`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add desktop/localruntime/token.go desktop/localruntime/token_test.go
git commit -m "feat(desktop): generate a per-launch local token"
```

---

### Task 3: Embedded PostgreSQL lifecycle

**Files:**
- Create: `desktop/localruntime/postgres.go`
- Create: `desktop/localruntime/postgres_test.go`

**Interfaces:**
- Produces:
  ```go
  func (m *Manager) startPostgres(ctx context.Context) (string, error)
  func freePort() (int, error)
  func waitTCP(ctx context.Context, addr string, timeout time.Duration) error
  ```

- [ ] **Step 1: Write a guarded integration test**

```go
func TestStartPostgres(t *testing.T) {
    bin := os.Getenv("HARNESS_TEST_PG_BIN")
    initdb := os.Getenv("HARNESS_TEST_INITDB_BIN")
    if bin == "" || initdb == "" {
        t.Skip("set HARNESS_TEST_PG_BIN and HARNESS_TEST_INITDB_BIN")
    }
    m, err := New(Config{HarnessBin: "true", PostgresBin: bin, InitDBBin: initdb,
        DataDir: t.TempDir(), StartupTimeout: 60 * time.Second})
    if err != nil {
        t.Fatal(err)
    }
    url, err := m.startPostgres(context.Background())
    if err != nil {
        t.Fatal(err)
    }
    if !strings.HasPrefix(url, "postgres://") {
        t.Fatalf("url = %s", url)
    }
    _ = m.Stop(context.Background())
}
```

- [ ] **Step 2: Run it without env**

Run: `cd desktop && go test ./localruntime -run TestStartPostgres -v`  
Expected: SKIP.

- [ ] **Step 3: Implement initdb and postgres start**

```go
func (m *Manager) startPostgres(ctx context.Context) (string, error) {
    data := filepath.Join(m.cfg.DataDir, "pgdata")
    if _, err := os.Stat(filepath.Join(data, "PG_VERSION")); errors.Is(err, os.ErrNotExist) {
        if err := os.MkdirAll(data, 0o700); err != nil {
            return "", err
        }
        cmd := exec.CommandContext(ctx, m.cfg.InitDBBin, "-D", data, "-U", "harness",
            "--auth=trust", "--encoding=UTF8", "--no-locale")
        cmd.Stdout = m.logWriter("initdb.log")
        cmd.Stderr = cmd.Stdout
        if err := cmd.Run(); err != nil {
            return "", fmt.Errorf("initdb: %w", err)
        }
    }
    port, err := freePort()
    if err != nil {
        return "", err
    }
    pg := exec.CommandContext(ctx, m.cfg.PostgresBin, "-D", data, "-h", "127.0.0.1",
        "-p", strconv.Itoa(port), "-k", filepath.Join(m.cfg.DataDir, "pgsock"))
    pg.Stdout = m.logWriter("postgres.log")
    pg.Stderr = pg.Stdout
    if err := pg.Start(); err != nil {
        return "", fmt.Errorf("start postgres: %w", err)
    }
    m.pg = pg
    m.pgPort = port
    if err := waitTCP(ctx, fmt.Sprintf("127.0.0.1:%d", port), m.cfg.StartupTimeout); err != nil {
        _ = m.stopPostgres()
        return "", err
    }
    return fmt.Sprintf("postgres://harness@127.0.0.1:%d/harness?sslmode=disable", port), nil
}

func (m *Manager) stopPostgres() error {
    if m.pg == nil || m.pg.Process == nil {
        return nil
    }
    _ = m.pg.Process.Signal(os.Interrupt)
    done := make(chan struct{})
    go func() { _ = m.pg.Wait(); close(done) }()
    select {
    case <-done:
    case <-time.After(10 * time.Second):
        _ = m.pg.Process.Kill()
        <-done
    }
    m.pg = nil
    return nil
}

func freePort() (int, error) {
    ln, err := net.Listen("tcp", "127.0.0.1:0")
    if err != nil {
        return 0, err
    }
    defer ln.Close()
    return ln.Addr().(*net.TCPAddr).Port, nil
}

func waitTCP(ctx context.Context, addr string, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
        if err == nil {
            conn.Close()
            return nil
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(200 * time.Millisecond):
        }
    }
    return fmt.Errorf("timeout waiting for %s", addr)
}
```

- [ ] **Step 4: Run tests**

Run: `cd desktop && go test ./localruntime -v`  
Expected: PASS; PostgreSQL test skips unless env vars are set.

- [ ] **Step 5: Commit**

```bash
git add desktop/localruntime/postgres.go desktop/localruntime/postgres_test.go
git commit -m "feat(desktop): start an embedded postgres"
```

---

### Task 4: Harness core lifecycle and health check

**Files:**
- Create: `desktop/localruntime/core.go`
- Create: `desktop/localruntime/core_test.go`

**Interfaces:**
- Produces:
  ```go
  func (m *Manager) startCore(ctx context.Context, dbURL, tokenFile string) (string, error)
  func waitHTTP(ctx context.Context, url string, timeout time.Duration) error
  ```

- [ ] **Step 1: Write a fake-core integration test**

Create a temporary shell script that starts an HTTP server on `$HARNESS_HTTP_ADDR` and writes a marker. If the test is too platform-specific, test `waitHTTP` against `httptest.NewServer`.

```go
func TestWaitHTTP(t *testing.T) {
    srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        w.WriteHeader(200)
    }))
    defer srv.Close()
    if err := waitHTTP(context.Background(), srv.URL+"/healthz", 2*time.Second); err != nil {
        t.Fatal(err)
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd desktop && go test ./localruntime -run TestWaitHTTP -v`  
Expected: FAIL because `waitHTTP` is undefined.

- [ ] **Step 3: Implement core start**

```go
func (m *Manager) startCore(ctx context.Context, dbURL, tokenFile string) (string, error) {
    port, err := freePort()
    if err != nil {
        return "", err
    }
    addr := fmt.Sprintf("127.0.0.1:%d", port)
    env := append(os.Environ(),
        "HARNESS_DATABASE_URL="+dbURL,
        "HARNESS_HTTP_ADDR="+addr,
        "HARNESS_AUTH_MODE=token",
        "HARNESS_AUTH_TOKENS_FILE="+tokenFile,
        "HARNESS_DEFAULT_TENANT=local",
        "HARNESS_DEFAULT_PROFILE=sre-diagnosis",
        "HARNESS_DEFAULT_PROFILE_VERSION=7",
        "HARNESS_ARTIFACT_DIR="+filepath.Join(m.cfg.DataDir, "artifacts"),
        "HARNESS_AUTO_MIGRATE=true",
    )
    for k, v := range m.cfg.ModelEnv {
        if v != "" {
            env = append(env, k+"="+v)
        }
    }
    cmd := exec.CommandContext(ctx, m.cfg.HarnessBin, "all")
    cmd.Env = env
    cmd.Stdout = m.logWriter("core.log")
    cmd.Stderr = cmd.Stdout
    if err := cmd.Start(); err != nil {
        return "", fmt.Errorf("start harness core: %w", err)
    }
    m.core = cmd
    base := "http://" + addr
    if err := waitHTTP(ctx, base+"/healthz", m.cfg.StartupTimeout); err != nil {
        _ = m.stopCore()
        return "", err
    }
    return base, nil
}

func (m *Manager) stopCore() error {
    if m.core == nil || m.core.Process == nil {
        return nil
    }
    _ = m.core.Process.Signal(os.Interrupt)
    done := make(chan struct{})
    go func() { _ = m.core.Wait(); close(done) }()
    select {
    case <-done:
    case <-time.After(10 * time.Second):
        _ = m.core.Process.Kill()
        <-done
    }
    m.core = nil
    return nil
}

func waitHTTP(ctx context.Context, url string, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
        resp, err := http.DefaultClient.Do(req)
        if err == nil {
            resp.Body.Close()
            if resp.StatusCode == http.StatusOK {
                return nil
            }
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(200 * time.Millisecond):
        }
    }
    return fmt.Errorf("timeout waiting for %s", url)
}
```

- [ ] **Step 4: Wire Start/Stop**

`Start` should call `startPostgres`, write the token file, call `startCore`, and return:
```go
func (m *Manager) Start(ctx context.Context) (Endpoint, error) {
    m.mu.Lock()
    m.status = "starting"
    m.mu.Unlock()
    dbURL, err := m.startPostgres(ctx)
    if err != nil {
        m.setStatus("failed")
        return Endpoint{}, err
    }
    plain, data, err := newToken()
    if err != nil {
        _ = m.stopPostgres()
        m.setStatus("failed")
        return Endpoint{}, err
    }
    tokenFile := m.runtimeFile("tokens.json")
    if err := writeTokenFile(tokenFile, data); err != nil {
        _ = m.stopPostgres()
        m.setStatus("failed")
        return Endpoint{}, err
    }
    m.token = plain
    base, err := m.startCore(ctx, dbURL, tokenFile)
    if err != nil {
        _ = m.stopPostgres()
        m.setStatus("failed")
        return Endpoint{}, err
    }
    m.baseURL = base
    m.setStatus("running")
    return Endpoint{URL: base, Token: plain, PID: m.core.Process.Pid}, nil
}
```

`Stop` must terminate the core, wait, then stop PostgreSQL, remove the token file, and set status `stopped`.

- [ ] **Step 5: Run tests and commit**

Run: `cd desktop && go test ./localruntime -v && go build ./...`

```bash
git add desktop/localruntime/core.go desktop/localruntime/core_test.go desktop/localruntime/manager.go
git commit -m "feat(desktop): start and health-check the local core"
```

---

### Task 5: Desktop settings for local/remote mode

**Files:**
- Modify: `desktop/app.go` (Settings struct)
- Create: `desktop/settings_defaults.go`
- Create: `desktop/localruntime/credentials.go`
- Create: `desktop/localruntime/credentials_test.go`

**Interfaces:**
- Settings gains:
  ```go
  Mode          string // "local" | "remote"
  LocalDataDir  string
  ModelProvider string // "anthropic" | "openai" | "gemini" | "scripted"
  ModelBaseURL  string
  ModelName     string
  ```
- Produces:
  ```go
  func (s Settings) LocalEnabled() bool
  func ModelEnv(provider, baseURL, model, key string) map[string]string
  ```

- [ ] **Step 1: Write settings tests**

```go
func TestSettingsLocalDefaults(t *testing.T) {
    s := defaultSettings()
    if s.Mode != "local" {
        t.Fatalf("mode = %q", s.Mode)
    }
    if s.LocalDataDir == "" {
        t.Fatal("local data dir must have a default")
    }
}

func TestModelEnv(t *testing.T) {
    env := ModelEnv("openai", "http://127.0.0.1:11434/v1", "qwen", "k")
    if env["HARNESS_OPENAI_BASE_URL"] != "http://127.0.0.1:11434/v1" ||
        env["HARNESS_OPENAI_MODEL"] != "qwen" || env["HARNESS_OPENAI_API_KEY"] != "k" {
        t.Fatalf("env = %v", env)
    }
}
```

- [ ] **Step 2: Run tests**

Run: `cd desktop && go test ./... -run 'TestSettingsLocalDefaults|TestModelEnv' -v`  
Expected: FAIL.

- [ ] **Step 3: Implement defaults and env mapping**

Default settings:
```go
func defaultSettings() Settings {
    return Settings{
        Mode:         "local",
        LocalDataDir: defaultLocalDataDir(),
        ServerURL:    "http://127.0.0.1:8080",
        Creds:        harness.Credentials{Mode: "dev", Tenant: "tenant-a", Actor: "alice", Roles: "operator,approver"},
    }
}
```

Default local data dir:
```go
func defaultLocalDataDir() string {
    dir, err := os.UserConfigDir()
    if err != nil {
        return filepath.Join(os.TempDir(), "sre-harness")
    }
    return filepath.Join(dir, "sre-harness-desktop", "local")
}
```

`ModelEnv` mapping:
```go
func ModelEnv(provider, baseURL, model, key string) map[string]string {
    switch provider {
    case "anthropic":
        return map[string]string{"ANTHROPIC_API_KEY": key, "HARNESS_ANTHROPIC_BASE_URL": baseURL}
    case "openai":
        return map[string]string{"HARNESS_OPENAI_BASE_URL": baseURL, "HARNESS_OPENAI_MODEL": model,
            "HARNESS_OPENAI_API_KEY": key}
    case "gemini":
        return map[string]string{"GEMINI_API_KEY": key, "HARNESS_GEMINI_BASE_URL": baseURL,
            "HARNESS_GEMINI_MODEL": model}
    default:
        return nil
    }
}
```

- [ ] **Step 4: Add keychain storage**

Use `github.com/zalando/go-keyring`:
```go
type SecretStore interface {
    Set(service, user, secret string) error
    Get(service, user string) (string, error)
    Delete(service, user string) error
}
```
The default implementation wraps `keyring`. Tests use an in-memory fake. Store only the model key in the OS keychain; settings store provider/base/model.

- [ ] **Step 5: Run tests and commit**

Run: `cd desktop && go test ./...`

```bash
git add desktop/app.go desktop/settings_defaults.go desktop/go.mod desktop/go.sum desktop/localruntime/credentials.go desktop/localruntime/credentials_test.go
git commit -m "feat(desktop): add local mode settings and model credentials"
```

---

### Task 6: Desktop local connect and shutdown lifecycle

**Files:**
- Modify: `desktop/app.go`
- Modify: `desktop/main.go`
- Create: `desktop/local.go`
- Create: `desktop/local_test.go`

**Interfaces:**
- Produces:
  ```go
  type LocalRuntime interface {
      Start(context.Context) (localruntime.Endpoint, error)
      Stop(context.Context) error
      Status() string
  }

  func (a *App) ConnectLocal() (map[string]any, error)
  func (a *App) LocalStatus() map[string]any
  ```
- App keeps one active runtime and stops it on shutdown.

- [ ] **Step 1: Write a fake-runtime test**

```go
type fakeLocal struct{ started, stopped bool }
func (f *fakeLocal) Start(context.Context) (localruntime.Endpoint, error) {
    f.started = true
    return localruntime.Endpoint{URL: "http://127.0.0.1:1234", Token: "t"}, nil
}
func (f *fakeLocal) Stop(context.Context) error { f.stopped = true; return nil }
func (f *fakeLocal) Status() string { return "running" }
```

Test that `ConnectLocal` sets the client credentials to bearer/tenant local and that `Shutdown` stops the runtime.

- [ ] **Step 2: Run it**

Run: `cd desktop && go test -run TestConnectLocal -v`  
Expected: FAIL.

- [ ] **Step 3: Implement**

`ConnectLocal`:
```go
func (a *App) ConnectLocal() (map[string]any, error) {
    if a.local == nil {
        return nil, errors.New("local runtime is not configured")
    }
    ep, err := a.local.Start(a.ctx)
    if err != nil {
        return nil, err
    }
    a.mu.Lock()
    a.settings.ServerURL = ep.URL
    a.settings.AuthMode = "bearer"
    a.settings.Token = ep.Token
    a.settings.Mode = "local"
    a.client = nil
    a.mu.Unlock()
    a.saveSettings()
    return map[string]any{"mode": "local", "url": ep.URL, "status": a.local.Status()}, nil
}
```

`Shutdown`:
```go
func (a *App) Shutdown() {
    a.UnwatchRun()
    if a.local != nil {
        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        _ = a.local.Stop(ctx)
        cancel()
    }
}
```

- [ ] **Step 4: Auto-connect on launch**

In `main.go` or app startup:
```go
if a.settings.Mode == "local" {
    go func() {
        if _, err := a.ConnectLocal(); err != nil {
            a.emit("local:error", err.Error())
        }
    }()
}
```
Do not block the Wails window; the UI shows "本地运行时启动中".

- [ ] **Step 5: Run tests and commit**

Run: `cd desktop && go test ./... && go build ./...`

```bash
git add desktop/app.go desktop/main.go desktop/local.go desktop/local_test.go
git commit -m "feat(desktop): connect to a supervised local runtime"
```

---

### Task 7: Connection UI for local and team modes

**Files:**
- Modify: `desktop/frontend/dist/index.html`
- Modify: `desktop/frontend/dist/app.js`
- Modify: `desktop/frontend/dist/i18n.js`
- Modify: `desktop/frontend/dist/app.css`

**Interfaces:**
- UI calls `App.ConnectLocal`, `App.LocalStatus`, `App.Connect` (remote).

- [ ] **Step 1: Write the UI state test**

If there is no JS test harness, add a Go test that reads the built JS and asserts the local labels exist:
```go
func TestConnectionUILocalMode(t *testing.T) {
    data, err := os.ReadFile("frontend/dist/app.js")
    if err != nil { t.Fatal(err) }
    for _, want := range []string{"ConnectLocal", "Local runtime", "Team server"} {
        if !strings.Contains(string(data), want) {
            t.Fatalf("missing %q", want)
        }
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd desktop && go test -run TestConnectionUILocalMode -v`  
Expected: FAIL.

- [ ] **Step 3: Implement the dialog**

Add two tabs above the server URL:
- `本地运行` — no URL field; shows data dir and model status; button `启动并连接`.
- `团队服务` — existing URL/auth fields; button `连接`.

On local start:
```js
async function connectLocal() {
  setConnecting(true);
  try {
    const out = await window.go.main.App.ConnectLocal();
    toast(t("Local runtime ready"));
    await bootstrap();
  } catch (e) {
    setConnError(e);
  } finally {
    setConnecting(false);
  }
}
```

- [ ] **Step 4: Add i18n strings**

Add:
- `Local runtime`
- `Team server`
- `Start and connect`
- `Local runtime is starting…`
- `Local data directory`
- `Local runtime ready`
- `Local runtime failed: {err}`

- [ ] **Step 5: Verify syntax and commit**

Run: `node --check desktop/frontend/dist/app.js && node --check desktop/frontend/dist/i18n.js && cd desktop && go test ./...`

```bash
git add desktop/frontend/dist
git commit -m "feat(desktop): add local and team connection modes"
```

---

### Task 8: Bundle the harness core and PostgreSQL

**Files:**
- Modify: `desktop/build.sh`
- Create: `desktop/localruntime/assets.go`
- Create: `desktop/localruntime/assets_test.go`

**Interfaces:**
- Produces:
  ```go
  func ResolveBins() (Config, error)
  ```
  It looks next to the executable in `Contents/Resources/local/bin` and falls back to `HARNESS_HARNESS_BIN`, `HARNESS_PG_BIN`, `HARNESS_INITDB_BIN` for development.

- [ ] **Step 1: Write the resolver test**

```go
func TestResolveBinsFromEnv(t *testing.T) {
    t.Setenv("HARNESS_HARNESS_BIN", "/tmp/harness")
    t.Setenv("HARNESS_PG_BIN", "/tmp/postgres")
    t.Setenv("HARNESS_INITDB_BIN", "/tmp/initdb")
    cfg, err := ResolveBins()
    if err != nil || cfg.HarnessBin != "/tmp/harness" || cfg.PostgresBin != "/tmp/postgres" {
        t.Fatalf("cfg=%+v err=%v", cfg, err)
    }
}
```

- [ ] **Step 2: Run it**

Run: `cd desktop && go test ./localruntime -run TestResolveBinsFromEnv -v`  
Expected: FAIL.

- [ ] **Step 3: Implement resolver**

```go
func ResolveBins() (Config, error) {
    cfg := Config{
        HarnessBin:  os.Getenv("HARNESS_HARNESS_BIN"),
        PostgresBin: os.Getenv("HARNESS_PG_BIN"),
        InitDBBin:   os.Getenv("HARNESS_INITDB_BIN"),
    }
    if cfg.HarnessBin != "" && cfg.PostgresBin != "" && cfg.InitDBBin != "" {
        return cfg, nil
    }
    exe, err := os.Executable()
    if err != nil {
        return cfg, err
    }
    base := filepath.Join(filepath.Dir(exe), "..", "Resources", "local", "bin")
    if cfg.HarnessBin == "" { cfg.HarnessBin = filepath.Join(base, "harness") }
    if cfg.PostgresBin == "" { cfg.PostgresBin = filepath.Join(base, "postgres") }
    if cfg.InitDBBin == "" { cfg.InitDBBin = filepath.Join(base, "initdb") }
    for _, p := range []string{cfg.HarnessBin, cfg.PostgresBin, cfg.InitDBBin} {
        if _, err := os.Stat(p); err != nil {
            return cfg, fmt.Errorf("missing local binary %s: %w", p, err)
        }
    }
    return cfg, nil
}
```

- [ ] **Step 4: Update build.sh**

Add:
```bash
# Build the local core.
(cd .. && go build -o "desktop/build/bin/SRE Harness.app/Contents/Resources/local/bin/harness" ./cmd/harness)

# Copy PostgreSQL (path supplied by the release environment).
pg_prefix="${HARNESS_PG_PREFIX:-}"
if [[ -z "$pg_prefix" || ! -d "$pg_prefix" ]]; then
  echo "HARNESS_PG_PREFIX is required to bundle PostgreSQL" >&2
  exit 1
fi
mkdir -p "$app/Contents/Resources/local/bin"
cp "$pg_prefix/bin/postgres" "$pg_prefix/bin/initdb" "$pg_prefix/bin/pg_ctl" "$app/Contents/Resources/local/bin/"
cp -R "$pg_prefix/lib" "$app/Contents/Resources/local/"
cp -R "$pg_prefix/share" "$app/Contents/Resources/local/"
# Sign every Mach-O binary after copying.
find "$app/Contents/Resources/local/bin" -type f -exec codesign --force --sign - {} \;
```

- [ ] **Step 5: Verify layout and commit**

Run: `cd desktop && ./build.sh && find "build/bin/SRE Harness.app/Contents/Resources/local" -maxdepth 3 -type f | head`

```bash
git add desktop/build.sh desktop/localruntime/assets.go desktop/localruntime/assets_test.go
git commit -m "build(desktop): bundle the harness core and postgres"
```

---

### Task 9: Clean-machine acceptance and documentation

**Files:**
- Modify: `desktop/README.md`
- Modify: `desktop/README.zh-CN.md`
- Modify: `README.md`
- Modify: `README.zh-CN.md`

- [ ] **Step 1: Add a clean-machine checklist**

Document:
- delete `~/Library/Application Support/SRE Harness`
- launch the app
- local mode starts
- send "diagnose checkout"
- approve a generated plan
- quit and relaunch; the session remains
- switch to team mode with a remote URL

- [ ] **Step 2: Run the checklist manually**

Run:
```bash
rm -rf "$HOME/Library/Application Support/SRE Harness"
open "desktop/build/bin/SRE Harness.app"
```

Expected: local runtime starts; no Docker/PostgreSQL/NATS/CLI needed.

- [ ] **Step 3: Verify no external listener**

Run:
```bash
lsof -nP -iTCP -sTCP:LISTEN | grep sre-harness
```
Expected: only `127.0.0.1:<random>` listeners.

- [ ] **Step 4: Run the existing test suites**

Run:
```bash
HARNESS_TEST_DATABASE_URL='postgres://harness:harness@localhost:5432/harness_test?sslmode=disable' go test ./...
cd desktop && go test ./...
node --check frontend/dist/app.js
```

- [ ] **Step 5: Commit**

```bash
git add README.md README.zh-CN.md desktop/README.md desktop/README.zh-CN.md
git commit -m "docs: document local-first desktop mode"
```

---

## Phase 2 (separate plan): SQLite/local store

After Phase 1 ships, replace embedded PostgreSQL with a local store:

1. Extract `app.Store`, `kernel.StateStore`, `api.Store`, `profile.Loader` adapters.
2. Add `internal/store/local` using `modernc.org/sqlite`.
3. Port the schema and claim/commit transactions.
4. Keep PostgreSQL as the remote/team store.
5. Remove the PostgreSQL child process from the package.

## Phase 3 (separate plan): team collaboration and OIDC

1. OIDC Authorization Code + PKCE in the desktop app.
2. Keychain refresh tokens.
3. Remote connection profiles and tenant switching.
4. Optional session export/import between local and team modes.
5. Notification webhooks and deep links.

## Self-review

- Local mode reuses the existing `/v1` API, kernel, flow engine, plan review and approvals.
- No task requires writing a second store implementation in Phase 1.
- The local token is random per launch and never stored in plaintext.
- Remote mode remains a separate connection profile.
- The only new runtime binaries are `harness`, `postgres` and `initdb`; NATS is not used locally.
- Phase 2 has a clear path to remove embedded PostgreSQL without changing the client contract.