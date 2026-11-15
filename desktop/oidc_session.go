package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/wailsapp/wails/v2/pkg/runtime"

	"go-sre-harness/desktop/harness"
	oidcflow "go-sre-harness/desktop/oidc"
)

const oidcKeyService = "SRE Harness"

type OIDCSettings struct {
	Issuer   string   `json:"issuer"`
	ClientID string   `json:"client_id"`
	Scopes   []string `json:"scopes,omitempty"`
	Subject  string   `json:"subject,omitempty"`
}

type publicAuthConfig struct {
	Mode     string   `json:"mode"`
	Issuer   string   `json:"issuer"`
	ClientID string   `json:"client_id"`
	Audience string   `json:"audience"`
	Scopes   []string `json:"scopes"`
}

func (a *App) ConnectOIDC(serverURL, issuer, clientID string) (map[string]any, error) {
	serverURL = strings.TrimRight(strings.TrimSpace(serverURL), "/")
	if serverURL == "" {
		return nil, errors.New(a.tr("server URL is required", "缺少服务端地址"))
	}
	if !a.native {
		return nil, errors.New(a.tr("SSO is available in the desktop app", "SSO 仅在桌面应用内可用"))
	}
	if issuer == "" || clientID == "" {
		cfg, err := a.fetchAuthConfig(serverURL)
		if err != nil {
			return nil, err
		}
		if cfg.Mode != "oidc" {
			return nil, errors.New(a.tr("the server does not offer OIDC login", "服务端未启用 OIDC 登录"))
		}
		issuer, clientID = cfg.Issuer, cfg.ClientID
	}
	a.mu.Lock()
	network := a.settings.Network
	a.mu.Unlock()
	httpClient, err := a.networkHTTP(network, 30*time.Second)
	if err != nil {
		return nil, err
	}
	client, err := oidcflow.New(oidcflow.Config{
		Issuer: issuer, ClientID: clientID, HTTP: httpClient,
		OpenURL: func(raw string) error {
			runtime.BrowserOpenURL(a.ctx, raw)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Minute)
	defer cancel()
	tokens, err := client.Authorize(ctx)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.oidcAccess = tokens.AccessToken
	a.oidcExpiry = tokens.Expiry
	a.mu.Unlock()
	c, err := harness.New(serverURL, harness.Credentials{Mode: "bearer"})
	if err != nil {
		return nil, err
	}
	if err := a.applyNetwork(c, network); err != nil {
		return nil, err
	}
	c.TokenSource = a.oidcToken
	var who map[string]any
	if err := c.Get(a.ctx, "/api/whoami", &who); err != nil {
		return nil, err
	}
	subject, _ := who["subject"].(string)
	if subject == "" {
		subject = tokens.Subject
	}
	a.mu.Lock()
	a.client = c
	a.locked = false
	a.settings.ServerURL = serverURL
	a.settings.Mode = "remote"
	a.settings.SignedOut = false
	a.settings.Creds = harness.Credentials{Mode: "bearer"}
	a.settings.OIDC = &OIDCSettings{
		Issuer: strings.TrimRight(issuer, "/"), ClientID: clientID, Subject: subject,
	}
	a.oidcClient = client
	a.oidcRefresh = tokens.RefreshToken
	a.oidcAccess = tokens.AccessToken
	a.oidcExpiry = tokens.Expiry
	a.mu.Unlock()
	a.ensureOIDCRefresh()
	if tokens.RefreshToken != "" {
		_ = a.secrets.Set(oidcKeyService, oidcSecretUser(issuer, subject), tokens.RefreshToken)
	}
	if err := a.saveSettings(); err != nil {
		return nil, err
	}
	a.logEvent("info", "signed in to %s with OIDC as %s", serverURL, subject)
	a.RefreshApprovals()
	return who, nil
}

func (a *App) fetchAuthConfig(serverURL string) (publicAuthConfig, error) {
	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL+"/api/auth/config", nil)
	if err != nil {
		return publicAuthConfig{}, err
	}
	a.mu.Lock()
	network := a.settings.Network
	a.mu.Unlock()
	client, err := a.networkHTTP(network, 10*time.Second)
	if err != nil {
		return publicAuthConfig{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return publicAuthConfig{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return publicAuthConfig{}, errors.New(a.tr("cannot read the server login configuration", "无法读取服务端登录配置"))
	}
	var cfg publicAuthConfig
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return publicAuthConfig{}, err
	}
	return cfg, nil
}

func (a *App) restoreOIDC() {
	a.mu.Lock()
	settings := a.settings
	a.mu.Unlock()
	if settings.OIDC == nil || settings.OIDC.Issuer == "" || settings.OIDC.ClientID == "" {
		return
	}
	refresh, err := a.secrets.Get(oidcKeyService, oidcSecretUser(settings.OIDC.Issuer, settings.OIDC.Subject))
	if err != nil || refresh == "" {
		return
	}
	network := settings.Network
	httpClient, err := a.networkHTTP(network, 30*time.Second)
	if err != nil {
		return
	}
	client, err := oidcflow.New(oidcflow.Config{Issuer: settings.OIDC.Issuer, ClientID: settings.OIDC.ClientID, HTTP: httpClient})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()
	tokens, err := client.Refresh(ctx, refresh)
	if err != nil {
		a.logEvent("warn", "OIDC session refresh failed: %v", err)
		return
	}
	a.mu.Lock()
	a.oidcAccess = tokens.AccessToken
	a.oidcExpiry = tokens.Expiry
	a.mu.Unlock()
	c, err := harness.New(settings.ServerURL, harness.Credentials{Mode: "bearer"})
	if err != nil {
		return
	}
	if err := a.applyNetwork(c, network); err != nil {
		return
	}
	c.TokenSource = a.oidcToken
	var who map[string]any
	if err := c.Get(a.ctx, "/api/whoami", &who); err != nil {
		a.logEvent("warn", "OIDC session restore failed: %v", err)
		return
	}
	a.mu.Lock()
	a.client = c
	a.oidcClient = client
	a.oidcRefresh = tokens.RefreshToken
	a.oidcAccess = tokens.AccessToken
	a.oidcExpiry = tokens.Expiry
	a.mu.Unlock()
	a.ensureOIDCRefresh()
	if tokens.RefreshToken != "" {
		_ = a.secrets.Set(oidcKeyService, oidcSecretUser(settings.OIDC.Issuer, settings.OIDC.Subject), tokens.RefreshToken)
	}
}

func (a *App) oidcToken() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.oidcAccess
}

func (a *App) ensureOIDCRefresh() {
	a.mu.Lock()
	if a.oidcLoop {
		a.mu.Unlock()
		return
	}
	a.oidcLoop = true
	a.mu.Unlock()
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-a.ctx.Done():
				return
			case <-ticker.C:
				a.refreshOIDC()
			}
		}
	}()
}

func (a *App) refreshOIDC() {
	a.mu.Lock()
	client, refresh, expiry := a.oidcClient, a.oidcRefresh, a.oidcExpiry
	a.mu.Unlock()
	if client == nil || refresh == "" {
		return
	}
	if !expiry.IsZero() && time.Until(expiry) > 2*time.Minute {
		return
	}
	ctx, cancel := context.WithTimeout(a.ctx, 20*time.Second)
	defer cancel()
	tokens, err := client.Refresh(ctx, refresh)
	if err != nil {
		a.logEvent("warn", "OIDC token refresh failed: %v", err)
		return
	}
	a.mu.Lock()
	a.oidcAccess = tokens.AccessToken
	a.oidcExpiry = tokens.Expiry
	if tokens.RefreshToken != "" {
		a.oidcRefresh = tokens.RefreshToken
	}
	settings := a.settings
	a.mu.Unlock()
	if settings.OIDC != nil && tokens.RefreshToken != "" {
		_ = a.secrets.Set(oidcKeyService, oidcSecretUser(settings.OIDC.Issuer, settings.OIDC.Subject), tokens.RefreshToken)
	}
}

func oidcSecretUser(issuer, subject string) string {
	return "oidc." + strings.TrimRight(issuer, "/") + "." + subject
}
