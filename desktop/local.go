package main

import (
	"context"
	"errors"
	"time"

	"go-sre-harness/desktop/harness"
	"go-sre-harness/desktop/localruntime"
)

// LocalRuntime is the supervised local core used by tests and the app.
type LocalRuntime interface {
	Start(context.Context) (localruntime.Endpoint, error)
	Stop(context.Context) error
	Status() string
}

// localRuntime returns the one manager for this app, creating it on demand.
func (a *App) localRuntime() (LocalRuntime, error) {
	a.mu.Lock()
	if a.local != nil {
		rt := a.local
		a.mu.Unlock()
		return rt, nil
	}
	dataDir := a.settings.LocalDataDir
	a.mu.Unlock()
	cfg, err := localruntime.ResolveBins()
	if err != nil {
		return nil, err
	}
	cfg.DataDir = dataDir
	cfg.ModelEnv = a.localModelEnv()
	rt, err := localruntime.New(cfg)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.local = rt
	a.mu.Unlock()
	return rt, nil
}

func (a *App) localModelEnv() map[string]string {
	a.mu.Lock()
	provider, baseURL, model := a.settings.ModelProvider, a.settings.ModelBaseURL, a.settings.ModelName
	secrets := a.secrets
	a.mu.Unlock()
	if provider == "" || provider == "scripted" {
		return nil
	}
	key := ""
	if secrets != nil {
		key, _ = localruntime.ModelKey(secrets, provider)
	}
	return localruntime.ModelEnv(provider, baseURL, model, key)
}

// ConnectLocal starts the bundled runtime and points the normal /v1 client at
// its loopback endpoint. It is safe to call from the UI after startup.
func (a *App) ConnectLocal() (map[string]any, error) {
	if a.ctx == nil {
		return nil, errors.New("app is not started")
	}
	a.UnwatchRun()
	rt, err := a.localRuntime()
	if err != nil {
		return nil, err
	}
	if rt.Status() == "running" {
		return a.LocalStatus(), nil
	}
	ep, err := rt.Start(a.ctx)
	if err != nil {
		return nil, err
	}
	c, err := harness.New(ep.URL, harness.Credentials{Mode: "bearer", Tenant: "local", Actor: "desktop",
		Roles: "operator,approver,admin", Token: ep.Token})
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = rt.Stop(ctx)
		cancel()
		return nil, err
	}
	a.mu.Lock()
	a.client = c
	a.settings.Mode = "local"
	a.settings.ServerURL = ep.URL
	a.settings.Creds = harness.Credentials{Mode: "bearer", Tenant: "local", Actor: "desktop",
		Roles: "operator,approver,admin"}
	a.settings.SignedOut = false
	a.mu.Unlock()
	if err := a.saveSettings(); err != nil {
		return nil, err
	}
	a.logEvent("info", "local runtime ready at %s", ep.URL)
	a.emit("local:ready", a.LocalStatus())
	a.emit("connection:ready", a.LocalStatus())
	return a.LocalStatus(), nil
}

// LocalStatus reports the supervised runtime without exposing its token.
func (a *App) LocalStatus() map[string]any {
	a.mu.Lock()
	rt := a.local
	url := a.settings.ServerURL
	mode := a.settings.Mode
	a.mu.Unlock()
	status := "stopped"
	if rt != nil {
		status = rt.Status()
	}
	return map[string]any{"mode": mode, "status": status, "url": url, "running": status == "running"}
}

func (a *App) stopLocal() {
	a.mu.Lock()
	rt := a.local
	a.local = nil
	a.mu.Unlock()
	if rt == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = rt.Stop(ctx)
}
