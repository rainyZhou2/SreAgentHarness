// Package harness is a small HTTP client for the SRE harness control-plane
// API (/v1). It has no UI dependencies, so it is tested on its own.
package harness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Credentials select how requests authenticate: development headers
// (tenant/actor/roles) or a bearer token (static token or OIDC JWT).
type Credentials struct {
	Mode   string `json:"mode"` // "dev" | "bearer"
	Tenant string `json:"tenant"`
	Actor  string `json:"actor"`
	Roles  string `json:"roles"` // comma separated
	Token  string `json:"token,omitempty"`
}

// Client talks to one harness server.
type Client struct {
	BaseURL string
	Creds   Credentials
	HTTP    *http.Client // request/response calls (with timeout)
	Stream  *http.Client // SSE (no timeout)
}

// New returns a client for baseURL (e.g. http://127.0.0.1:8080).
func New(baseURL string, c Credentials) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid server URL %q (want http(s)://host:port)", baseURL)
	}
	switch c.Mode {
	case "dev":
		if c.Tenant == "" || c.Actor == "" {
			return nil, errors.New("development mode needs a tenant and an actor")
		}
	case "bearer":
		if c.Token == "" {
			return nil, errors.New("bearer mode needs a token")
		}
	default:
		return nil, fmt.Errorf("unknown auth mode %q", c.Mode)
	}
	return &Client{BaseURL: u.String(), Creds: c,
		HTTP: &http.Client{Timeout: 30 * time.Second}, Stream: &http.Client{}}, nil
}

// APIError is a non-2xx response ({"error","message"} body).
type APIError struct {
	Status  int    `json:"status"`
	Kind    string `json:"error"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%d %s: %s", e.Status, e.Kind, e.Message)
	}
	return fmt.Sprintf("%d %s", e.Status, e.Kind)
}

func (c *Client) newRequest(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "sre-harness-desktop")
	if c.Creds.Mode == "bearer" {
		req.Header.Set("Authorization", "Bearer "+c.Creds.Token)
	} else {
		req.Header.Set("X-Tenant-ID", c.Creds.Tenant)
		req.Header.Set("X-Actor-ID", c.Creds.Actor)
		req.Header.Set("X-Roles", c.Creds.Roles)
	}
	return req, nil
}

// Do sends a request and decodes the JSON response into out (may be nil).
// Write requests get a fresh Idempotency-Key.
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	req, err := c.newRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	if method != http.MethodGet {
		req.Header.Set("Idempotency-Key", NewKey())
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		e := &APIError{Status: resp.StatusCode}
		if json.Unmarshal(data, e) != nil || e.Kind == "" {
			e.Kind = http.StatusText(resp.StatusCode)
			e.Message = strings.TrimSpace(string(data))
		}
		return e
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// Get is Do with GET.
func (c *Client) Get(ctx context.Context, path string, out any) error {
	return c.Do(ctx, http.MethodGet, path, nil, out)
}

// Event is one run event as served by the API.
type Event struct {
	Seq        int64           `json:"event_seq"`
	Type       string          `json:"event_type"`
	ActorType  string          `json:"actor_type"`
	ActorID    *string         `json:"actor_id,omitempty"`
	StepID     *string         `json:"step_id,omitempty"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt time.Time       `json:"occurred_at"`
}

// Delta is an ephemeral model stream event (token text, tool-call
// arguments). Deltas are not replayed after a reconnect.
type Delta struct {
	StepID string `json:"step_id"`
	Seq    int64  `json:"delta_seq"`
	Event  struct {
		Type      string `json:"type"`
		Text      string `json:"text,omitempty"`
		ToolName  string `json:"tool_name,omitempty"`
		ArgsDelta string `json:"args_delta,omitempty"`
	} `json:"event"`
}

// StreamEvents follows GET /v1/runs/{id}/events/stream from afterSeq and
// calls fn for every event (and onDelta, when set, for model deltas). It
// reconnects (resuming from the last event id) until the server ends the
// stream of a finished run, ctx is cancelled or the server rejects the
// request.
func (c *Client) StreamEvents(ctx context.Context, runID string, afterSeq int64, fn func(Event), onDelta ...func(Delta)) error {
	var deltaFn func(Delta)
	if len(onDelta) > 0 {
		deltaFn = onDelta[0]
	}
	backoff := 500 * time.Millisecond
	for {
		got, ended, err := c.streamOnce(ctx, runID, &afterSeq, fn, deltaFn)
		if ctx.Err() != nil || ended {
			return nil
		}
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.Status != http.StatusServiceUnavailable {
			return err
		}
		if got {
			backoff = 500 * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 10*time.Second)
	}
}

// streamOnce reads one connection. ended reports the server's "stream.end"
// message: the run is finished and every event was delivered.
func (c *Client) streamOnce(ctx context.Context, runID string, after *int64, fn func(Event), onDelta func(Delta)) (got, ended bool, err error) {
	path := "/v1/runs/" + url.PathEscape(runID) + "/events/stream"
	if onDelta != nil {
		path += "?include_deltas=true"
	}
	req, err := c.newRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", strconv.FormatInt(*after, 10))
	resp, err := c.Stream.Do(req)
	if err != nil {
		return false, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		e := &APIError{Status: resp.StatusCode}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		_ = json.Unmarshal(data, e)
		return false, false, e
	}
	err = ParseSSE(resp.Body, func(id, event, data string) {
		if event == "stream.end" {
			ended = true
			return
		}
		if data == "" {
			return
		}
		if event == "model.delta" {
			var d Delta
			if onDelta != nil && json.Unmarshal([]byte(data), &d) == nil {
				onDelta(d)
			}
			return
		}
		var e Event
		if json.Unmarshal([]byte(data), &e) != nil {
			return
		}
		if e.Seq <= *after {
			return
		}
		*after = e.Seq
		got = true
		fn(e)
	})
	return got, ended, err
}

// ParseSSE reads a text/event-stream and calls fn per dispatched message.
func ParseSSE(r io.Reader, fn func(id, event, data string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	var id, event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if len(data) > 0 || event != "" {
				fn(id, event, strings.Join(data, "\n"))
			}
			event, data = "", nil
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / keep-alive
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			id = value
		case "event":
			event = value
		case "data":
			data = append(data, value)
		}
	}
	return sc.Err()
}

// NewKey returns a random idempotency key.
func NewKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return "desktop-" + hex.EncodeToString(b[:])
}
