package capability

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"go-sre-harness/internal/action"
)

// HTTPConfig configures a domain system endpoint. Credentials are read from
// files or environment variables, never from the config body, so configs
// can be committed.
type HTTPConfig struct {
	URL string `json:"url"`
	// TokenFile / TokenEnv provide a bearer token (re-read per request:
	// projected tokens rotate).
	TokenFile string `json:"token_file,omitempty"`
	TokenEnv  string `json:"token_env,omitempty"`
	// TokenScheme is the Authorization scheme (default "Bearer").
	TokenScheme string `json:"token_scheme,omitempty"`
	// Headers are static extra headers (no secrets: use the token fields).
	Headers  map[string]string `json:"headers,omitempty"`
	CAFile   string            `json:"ca_file,omitempty"`
	Timeout  Duration          `json:"timeout,omitempty"`
	MaxBytes int64             `json:"max_bytes,omitempty"`

	// HTTP replaces the client (tests, or a netguard-restricted client).
	HTTP *http.Client `json:"-"`
	// Token is a literal token (tests).
	Token string `json:"-"`
}

// Duration is a JSON duration ("30s").
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		var n float64
		if err2 := json.Unmarshal(b, &n); err2 != nil {
			return err
		}
		*d = Duration(time.Duration(n * float64(time.Second)))
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// Client is a small JSON HTTP client for domain systems.
type Client struct {
	cfg  HTTPConfig
	base *url.URL
}

// NewClient validates the endpoint.
func NewClient(cfg HTTPConfig) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(cfg.URL, "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%w: url %q must be http(s)://host", ErrInvalid, cfg.URL)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = Duration(15 * time.Second)
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 8 << 20
	}
	if cfg.TokenScheme == "" {
		cfg.TokenScheme = "Bearer"
	}
	if cfg.HTTP == nil {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("ca_file %s holds no certificate", cfg.CAFile)
			}
			tc.RootCAs = pool
		}
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.TLSClientConfig = tc
		cfg.HTTP = &http.Client{Timeout: cfg.Timeout.D(), Transport: t}
	}
	return &Client{cfg: cfg, base: u}, nil
}

// URL returns the base URL.
func (c *Client) URL() string { return c.base.String() }

func (c *Client) token() (string, error) {
	switch {
	case c.cfg.Token != "":
		return c.cfg.Token, nil
	case c.cfg.TokenFile != "":
		b, err := os.ReadFile(c.cfg.TokenFile)
		if err != nil {
			return "", fmt.Errorf("token_file: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	case c.cfg.TokenEnv != "":
		v := strings.TrimSpace(os.Getenv(c.cfg.TokenEnv))
		if v == "" {
			return "", fmt.Errorf("token_env %s is empty", c.cfg.TokenEnv)
		}
		return v, nil
	}
	return "", nil
}

// StatusError is a non-2xx response.
type StatusError struct {
	Method, Path string
	Code         int
	Body         string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("%s %s: HTTP %d: %s", e.Method, e.Path, e.Code, e.Body)
}

// IsStatus reports whether err is a StatusError with one of the codes.
func IsStatus(err error, codes ...int) bool {
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	for _, c := range codes {
		if se.Code == c {
			return true
		}
	}
	return false
}

// Request is one call.
type Request struct {
	Method      string
	Path        string // joined to the base URL path
	Query       url.Values
	Body        any    // JSON-encoded unless []byte
	ContentType string // default application/json
	Header      map[string]string
}

// Do sends r and decodes a JSON response into out (when non-nil).
func (c *Client) Do(ctx context.Context, r Request, out any) error {
	var body io.Reader
	var raw []byte
	switch b := r.Body.(type) {
	case nil:
	case []byte:
		raw = b
	default:
		var err error
		if raw, err = json.Marshal(b); err != nil {
			return err
		}
	}
	if raw != nil {
		body = bytes.NewReader(raw)
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(r.Path, "/")
	if len(r.Query) > 0 {
		u.RawQuery = r.Query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, r.Method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if raw != nil {
		ct := r.ContentType
		if ct == "" {
			ct = "application/json"
		}
		req.Header.Set("Content-Type", ct)
	}
	for k, v := range c.cfg.Headers {
		req.Header.Set(k, v)
	}
	for k, v := range r.Header {
		req.Header.Set(k, v)
	}
	tok, err := c.token()
	if err != nil {
		return err
	}
	if tok != "" {
		req.Header.Set("Authorization", c.cfg.TokenScheme+" "+tok)
	}
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, c.cfg.MaxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(b)) > c.cfg.MaxBytes {
		return fmt.Errorf("%s %s: response exceeds %d bytes", r.Method, r.Path, c.cfg.MaxBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		msg := strings.TrimSpace(string(b))
		if len(msg) > 300 {
			msg = msg[:300] + "…"
		}
		return &StatusError{Method: r.Method, Path: r.Path, Code: resp.StatusCode, Body: msg}
	}
	if out == nil || len(b) == 0 {
		return nil
	}
	if rawOut, ok := out.(*json.RawMessage); ok {
		*rawOut = append((*rawOut)[:0], b...)
		return nil
	}
	if err := json.Unmarshal(b, out); err != nil {
		return fmt.Errorf("%s %s: decode response: %w", r.Method, r.Path, err)
	}
	return nil
}

// Get is Do with GET.
func (c *Client) Get(ctx context.Context, path string, q url.Values, out any) error {
	return c.Do(ctx, Request{Method: http.MethodGet, Path: path, Query: q}, out)
}

// ActionError classifies an error from a side-effecting call for the action
// runtime: a 4xx (except 408/429) means the request was refused and nothing
// changed (action.ErrNotApplied); anything else — timeouts, 5xx, transport
// errors after the request was sent — may have changed the system and is
// returned as is, which the runtime records as EFFECT_UNKNOWN.
func ActionError(err error) error {
	if err == nil {
		return nil
	}
	var se *StatusError
	if errors.As(err, &se) && se.Code >= 400 && se.Code < 500 && se.Code != 408 && se.Code != 429 {
		return fmt.Errorf("%w: %v", action.ErrNotApplied, err)
	}
	return err
}

// NotApplied wraps a refusal detected before any side effect.
func NotApplied(format string, args ...any) error {
	return fmt.Errorf("%w: %s", action.ErrNotApplied, fmt.Sprintf(format, args...))
}

// BeforeWrite classifies an error raised before any side-effecting request
// was sent (reads, validation): nothing changed, so it is ErrNotApplied
// regardless of its cause.
func BeforeWrite(err error) error {
	if err == nil || errors.Is(err, action.ErrNotApplied) {
		return err
	}
	return fmt.Errorf("%w: %v", action.ErrNotApplied, err)
}
