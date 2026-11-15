package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/url"
	"os"
	"time"

	"go-sre-harness/desktop/harness"
	oidcflow "go-sre-harness/desktop/oidc"
)

type NetworkSettings struct {
	ProxyURL   string `json:"proxy_url,omitempty"`
	CACertPath string `json:"ca_cert_path,omitempty"`
}

func (a *App) buildTransport(network NetworkSettings) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if network.ProxyURL != "" {
		proxyURL, err := url.Parse(network.ProxyURL)
		if err != nil || proxyURL.Host == "" {
			return nil, errors.New(a.tr("proxy URL is invalid", "代理地址无效"))
		}
		switch proxyURL.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return nil, errors.New(a.tr("proxy must use http, https or socks5", "代理仅支持 http、https 或 socks5"))
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	if network.CACertPath != "" {
		pem, err := os.ReadFile(network.CACertPath)
		if err != nil {
			return nil, err
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New(a.tr("CA file has no valid certificates", "CA 文件没有有效证书"))
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	return transport, nil
}

func (a *App) networkHTTP(network NetworkSettings, timeout time.Duration) (*http.Client, error) {
	transport, err := a.buildTransport(network)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport, Timeout: timeout}, nil
}

func (a *App) applyNetwork(client *harness.Client, network NetworkSettings) error {
	transport, err := a.buildTransport(network)
	if err != nil {
		return err
	}
	client.HTTP = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	client.Stream = &http.Client{Transport: transport}
	return nil
}

// SetNetwork validates and saves proxy/CA settings. Existing connections pick
// them up immediately; OIDC discovery uses them on the next refresh/login.
func (a *App) SetNetwork(network NetworkSettings) error {
	if _, err := a.buildTransport(network); err != nil {
		return err
	}
	a.mu.Lock()
	a.settings.Network = network
	client := a.client
	a.mu.Unlock()
	if client != nil {
		if err := a.applyNetwork(client, network); err != nil {
			return err
		}
	}
	a.mu.Lock()
	oidcSettings := a.settings.OIDC
	a.mu.Unlock()
	if oidcSettings != nil {
		httpClient, err := a.networkHTTP(network, 30*time.Second)
		if err != nil {
			return err
		}
		refreshed, err := oidcflow.New(oidcflow.Config{
			Issuer: oidcSettings.Issuer, ClientID: oidcSettings.ClientID, HTTP: httpClient,
		})
		if err != nil {
			return err
		}
		a.mu.Lock()
		a.oidcClient = refreshed
		a.mu.Unlock()
	}
	return a.saveSettings()
}

// NetworkDiagnostics checks DNS, TCP, TLS and the control-plane health with the
// configured proxy and CA. It never sends credentials.
func (a *App) NetworkDiagnostics() map[string]any {
	a.mu.Lock()
	network := a.settings.Network
	serverURL := a.settings.ServerURL
	a.mu.Unlock()
	out := map[string]any{"proxy": network.ProxyURL, "ca_cert": network.CACertPath}
	client, err := a.networkHTTP(network, 10*time.Second)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	started := time.Now()
	req, err := http.NewRequest(http.MethodGet, serverURL+"/healthz", nil)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	resp, err := client.Do(req)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer resp.Body.Close()
	out["status"] = resp.StatusCode
	out["ok"] = resp.StatusCode == http.StatusOK
	out["latency_ms"] = time.Since(started).Milliseconds()
	return out
}
