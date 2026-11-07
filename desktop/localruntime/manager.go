// Package localruntime supervises the processes that make the desktop client
// self-contained: a SQLite-backed harness core. The UI still talks to the
// core over the normal /v1 HTTP/SSE API.
package localruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// Config locates the bundled executables and the per-user data directory.
type Config struct {
	HarnessBin     string
	DataDir        string
	ModelEnv       map[string]string
	StartupTimeout time.Duration
}

// Endpoint is what the desktop client needs to talk to the local core.
type Endpoint struct {
	URL   string
	Token string
	PID   int
}

// Manager owns one local runtime. It is not safe to start two Managers on the
// same data directory.
type Manager struct {
	cfg Config

	mu      sync.Mutex
	status  string
	core    *exec.Cmd
	token   string
	baseURL string
	logs    map[string]*os.File
}

// New validates the configuration and creates the private data directory.
func New(cfg Config) (*Manager, error) {
	if cfg.HarnessBin == "" || cfg.DataDir == "" {
		return nil, errors.New("local runtime needs a harness binary and a data dir")
	}
	if cfg.StartupTimeout <= 0 {
		cfg.StartupTimeout = 90 * time.Second
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create local data dir: %w", err)
	}
	return &Manager{cfg: cfg, status: "stopped", logs: map[string]*os.File{}}, nil
}

// Status is one of stopped, starting, running, stopping, failed.
func (m *Manager) Status() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

// Start writes a fresh local token and starts the SQLite-backed core.
func (m *Manager) Start(ctx context.Context) (Endpoint, error) {
	m.mu.Lock()
	if m.status == "running" || m.status == "starting" {
		m.mu.Unlock()
		return Endpoint{}, errors.New("local runtime is already running")
	}
	m.status = "starting"
	m.mu.Unlock()

	plain, data, err := newToken()
	if err != nil {
		m.setStatus("failed")
		return Endpoint{}, err
	}
	tokenFile := m.runtimeFile("tokens.json")
	if err := writeTokenFile(tokenFile, data); err != nil {
		m.setStatus("failed")
		return Endpoint{}, err
	}
	m.mu.Lock()
	m.token = plain
	m.mu.Unlock()

	base, err := m.startCore(ctx, tokenFile)
	if err != nil {
		_ = os.Remove(tokenFile)
		m.setStatus("failed")
		return Endpoint{}, err
	}
	m.mu.Lock()
	m.baseURL = base
	m.status = "running"
	pid := 0
	if m.core != nil && m.core.Process != nil {
		pid = m.core.Process.Pid
	}
	m.mu.Unlock()
	return Endpoint{URL: base, Token: plain, PID: pid}, nil
}

// Stop terminates the core. It is safe to call twice.
func (m *Manager) Stop(_ context.Context) error {
	m.mu.Lock()
	if m.status == "stopped" || m.status == "stopping" {
		m.mu.Unlock()
		return nil
	}
	m.status = "stopping"
	m.mu.Unlock()

	_ = m.stopCore()
	_ = os.Remove(m.runtimeFile("tokens.json"))
	m.closeLogs()
	m.mu.Lock()
	m.token, m.baseURL = "", ""
	m.status = "stopped"
	m.mu.Unlock()
	return nil
}

func (m *Manager) setStatus(s string) {
	m.mu.Lock()
	m.status = s
	m.mu.Unlock()
}

func (m *Manager) runtimeFile(name string) string {
	return filepath.Join(m.cfg.DataDir, name)
}

// logWriter appends one child-process log under <data>/logs. On failure it
// falls back to io.Discard so a logging problem never blocks startup.
func (m *Manager) logWriter(name string) io.Writer {
	m.mu.Lock()
	defer m.mu.Unlock()
	if f, ok := m.logs[name]; ok {
		return f
	}
	if err := os.MkdirAll(filepath.Join(m.cfg.DataDir, "logs"), 0o700); err != nil {
		return io.Discard
	}
	f, err := os.OpenFile(filepath.Join(m.cfg.DataDir, "logs", name),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return io.Discard
	}
	m.logs[name] = f
	return f
}

func (m *Manager) closeLogs() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, f := range m.logs {
		_ = f.Close()
		delete(m.logs, name)
	}
}
