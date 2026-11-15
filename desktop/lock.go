package main

import (
	"errors"
	"time"
)

// Lock clears the in-memory OIDC access token while keeping the refresh token
// in the OS keychain. Only SSO sessions can be unlocked without re-entering a token.
func (a *App) Lock() error {
	a.mu.Lock()
	if a.settings.OIDC == nil {
		a.mu.Unlock()
		return errors.New("locking requires an SSO session")
	}
	a.mu.Unlock()
	a.UnwatchRun()
	a.mu.Lock()
	a.locked = true
	a.client = nil
	a.oidcAccess = ""
	a.oidcExpiry = time.Time{}
	a.mu.Unlock()
	a.logEvent("info", "session locked")
	return nil
}

// Unlock refreshes the OIDC session and reconnects the client.
func (a *App) Unlock() (map[string]any, error) {
	a.mu.Lock()
	locked := a.locked
	a.mu.Unlock()
	if !locked {
		return nil, errors.New("session is not locked")
	}
	a.restoreOIDC()
	a.mu.Lock()
	client := a.client
	a.mu.Unlock()
	if client == nil {
		return nil, errors.New("session refresh failed; sign in again")
	}
	var who map[string]any
	if err := client.Get(a.ctx, "/api/whoami", &who); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.locked = false
	a.mu.Unlock()
	a.RefreshApprovals()
	return who, nil
}

// Locked reports whether the UI should show the lock screen.
func (a *App) Locked() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.locked
}
