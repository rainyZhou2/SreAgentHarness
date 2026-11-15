// Package oidc runs the desktop Authorization Code + PKCE flow against an
// OpenID Connect provider and refreshes the resulting tokens.
package oidc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	Issuer     string
	ClientID   string
	Scopes     []string
	HTTP       *http.Client
	OpenURL    func(string) error
	ListenAddr string
}

type Discovery struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	EndSessionEndpoint    string `json:"end_session_endpoint"`
}

type Tokens struct {
	AccessToken  string
	RefreshToken string
	IDToken      string
	Expiry       time.Time
	Subject      string
}

type Client struct {
	cfg       Config
	discovery Discovery
}

func New(cfg Config) (*Client, error) {
	cfg.Issuer = strings.TrimRight(strings.TrimSpace(cfg.Issuer), "/")
	if cfg.Issuer == "" || cfg.ClientID == "" {
		return nil, errors.New("oidc: issuer and client_id are required")
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{"openid", "profile", "email", "offline_access"}
	}
	if cfg.HTTP == nil {
		cfg.HTTP = &http.Client{Timeout: 15 * time.Second}
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = "127.0.0.1:0"
	}
	return &Client{cfg: cfg}, nil
}

func (c *Client) Discover(ctx context.Context) error {
	var doc Discovery
	if err := c.getJSON(ctx, c.cfg.Issuer+"/.well-known/openid-configuration", &doc); err != nil {
		return err
	}
	if doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" {
		return errors.New("oidc: discovery has no authorization or token endpoint")
	}
	if doc.Issuer != "" && strings.TrimRight(doc.Issuer, "/") != c.cfg.Issuer {
		return fmt.Errorf("oidc: discovery issuer %q does not match %q", doc.Issuer, c.cfg.Issuer)
	}
	c.discovery = doc
	return nil
}

func (c *Client) Authorize(ctx context.Context) (*Tokens, error) {
	if c.discovery.AuthorizationEndpoint == "" {
		if err := c.Discover(ctx); err != nil {
			return nil, err
		}
	}
	if c.cfg.OpenURL == nil {
		return nil, errors.New("oidc: no browser opener")
	}
	listener, err := net.Listen("tcp", c.cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("oidc: callback listener: %w", err)
	}
	defer listener.Close()
	redirect := "http://" + listener.Addr().String() + "/callback"

	state, err := randomString(24)
	if err != nil {
		return nil, err
	}
	nonce, err := randomString(24)
	if err != nil {
		return nil, err
	}
	verifier, err := randomString(40)
	if err != nil {
		return nil, err
	}
	challenge := pkceChallenge(verifier)

	authURL, err := url.Parse(c.discovery.AuthorizationEndpoint)
	if err != nil {
		return nil, err
	}
	query := authURL.Query()
	query.Set("response_type", "code")
	query.Set("client_id", c.cfg.ClientID)
	query.Set("redirect_uri", redirect)
	query.Set("scope", strings.Join(c.cfg.Scopes, " "))
	query.Set("state", state)
	query.Set("nonce", nonce)
	query.Set("code_challenge", challenge)
	query.Set("code_challenge_method", "S256")
	authURL.RawQuery = query.Encode()

	type callback struct {
		code string
		err  error
	}
	result := make(chan callback, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/callback" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("state") != state {
			http.Error(w, "state mismatch", http.StatusBadRequest)
			result <- callback{err: errors.New("oidc: state mismatch")}
			return
		}
		if e := q.Get("error"); e != "" {
			http.Error(w, e, http.StatusBadRequest)
			result <- callback{err: fmt.Errorf("oidc: %s", e)}
			return
		}
		code := q.Get("code")
		if code == "" {
			http.Error(w, "missing code", http.StatusBadRequest)
			result <- callback{err: errors.New("oidc: missing code")}
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, "<html><body>Signed in. You can close this window.</body></html>")
		result <- callback{code: code}
	})}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	if err := c.cfg.OpenURL(authURL.String()); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case cb := <-result:
		if cb.err != nil {
			return nil, cb.err
		}
		return c.exchange(ctx, cb.code, redirect, verifier, nonce)
	}
}

func (c *Client) Refresh(ctx context.Context, refreshToken string) (*Tokens, error) {
	if refreshToken == "" {
		return nil, errors.New("oidc: refresh token is empty")
	}
	if c.discovery.TokenEndpoint == "" {
		if err := c.Discover(ctx); err != nil {
			return nil, err
		}
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {c.cfg.ClientID},
	}
	var out tokenResponse
	if err := c.postForm(ctx, c.discovery.TokenEndpoint, form, &out); err != nil {
		return nil, err
	}
	return tokensFrom(out, "")
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int64  `json:"expires_in"`
}

func (c *Client) exchange(ctx context.Context, code, redirect, verifier, nonce string) (*Tokens, error) {
	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {redirect},
		"client_id":     {c.cfg.ClientID},
		"code_verifier": {verifier},
	}
	var out tokenResponse
	if err := c.postForm(ctx, c.discovery.TokenEndpoint, form, &out); err != nil {
		return nil, err
	}
	return tokensFrom(out, nonce)
}

func tokensFrom(out tokenResponse, nonce string) (*Tokens, error) {
	if out.AccessToken == "" {
		return nil, errors.New("oidc: token response has no access_token")
	}
	tokens := &Tokens{
		AccessToken:  out.AccessToken,
		RefreshToken: out.RefreshToken,
		IDToken:      out.IDToken,
	}
	if out.ExpiresIn > 0 {
		tokens.Expiry = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	}
	if out.IDToken != "" {
		subject, idNonce, err := decodeIDToken(out.IDToken)
		if err != nil {
			return nil, err
		}
		if nonce != "" && idNonce != "" && idNonce != nonce {
			return nil, errors.New("oidc: id_token nonce mismatch")
		}
		tokens.Subject = subject
	}
	return tokens, nil
}

func decodeIDToken(raw string) (subject, nonce string, err error) {
	parts := strings.Split(raw, ".")
	if len(parts) < 2 {
		return "", "", errors.New("oidc: malformed id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", "", fmt.Errorf("oidc: id_token payload: %w", err)
	}
	var claims struct {
		Subject string `json:"sub"`
		Nonce   string `json:"nonce"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", "", fmt.Errorf("oidc: id_token claims: %w", err)
	}
	return claims.Subject, claims.Nonce, nil
}

func (c *Client) postForm(ctx context.Context, endpoint string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("oidc: token endpoint HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("oidc: token response: %w", err)
	}
	return nil
}

func (c *Client) getJSON(ctx context.Context, endpoint string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("oidc: discovery HTTP %d", resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("oidc: discovery response: %w", err)
	}
	return nil
}

func randomString(bytes int) (string, error) {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
