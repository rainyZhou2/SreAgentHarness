// Package cli is the command-line client of the harness: an interactive chat
// with inline approvals (`harness chat`). It talks to the same /v1 API as the
// desktop client, so it works against the local runtime and a team server.
package cli

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

// Client is a minimal bearer-token client for the harness API.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client // request/response calls (with a timeout)
	Stream  *http.Client // SSE (no timeout)
}

// NewClient validates the URL and returns a client.
func NewClient(baseURL, token string) (*Client, error) {
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid server URL %q (want http(s)://host:port)", baseURL)
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("a token is required")
	}
	return &Client{BaseURL: u.String(), Token: strings.TrimSpace(token),
		HTTP: &http.Client{Timeout: 30 * time.Second}, Stream: &http.Client{}}, nil
}

// APIError is a non-2xx response.
type APIError struct {
	Status  int
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("%s (%d %s)", e.Message, e.Status, e.Code)
	}
	return fmt.Sprintf("http %d", e.Status)
}

func newKey() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return "cli-" + hex.EncodeToString(b[:])
}

func (c *Client) req(ctx context.Context, method, path string, body any) (*http.Request, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	if method != http.MethodGet {
		r.Header.Set("Idempotency-Key", newKey())
	}
	return r, nil
}

// Do sends a JSON request and decodes a JSON response into out (may be nil).
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	r, err := c.req(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if resp.StatusCode/100 != 2 {
		e := &APIError{Status: resp.StatusCode}
		_ = json.Unmarshal(data, e)
		return e
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

// Event is one run event.
type Event struct {
	Seq     int64           `json:"event_seq"`
	Type    string          `json:"event_type"`
	Payload json.RawMessage `json:"payload"`
}

// Delta is an ephemeral model stream event.
type Delta struct {
	Event struct {
		Type     string `json:"type"`
		Text     string `json:"text,omitempty"`
		ToolName string `json:"tool_name,omitempty"`
	} `json:"event"`
}

// Follow streams a run's events until the server ends the stream (the run is
// finished), ctx is cancelled or the server refuses. It reconnects from the
// last seen event. onEvent may block (for example to ask the user).
func (c *Client) Follow(ctx context.Context, runID string, onEvent func(Event), onDelta func(Delta)) error {
	after := int64(0)
	backoff := 500 * time.Millisecond
	failures := 0
	for {
		got, ended, err := c.followOnce(ctx, runID, &after, onEvent, onDelta)
		if ended {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ae *APIError
		if errors.As(err, &ae) && ae.Status != http.StatusServiceUnavailable {
			return err
		}
		if got {
			failures, backoff = 0, 500*time.Millisecond
		} else if failures++; failures > 8 {
			return fmt.Errorf("event stream: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (c *Client) followOnce(ctx context.Context, runID string, after *int64, onEvent func(Event), onDelta func(Delta)) (got, ended bool, err error) {
	r, err := c.req(ctx, http.MethodGet, "/v1/runs/"+url.PathEscape(runID)+"/events/stream?include_deltas=true", nil)
	if err != nil {
		return false, false, err
	}
	r.Header.Set("Accept", "text/event-stream")
	r.Header.Set("Last-Event-ID", strconv.FormatInt(*after, 10))
	resp, err := c.Stream.Do(r)
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
	err = parseSSE(resp.Body, func(event, data string) {
		switch event {
		case "stream.end":
			ended = true
		case "model.delta":
			var d Delta
			if onDelta != nil && json.Unmarshal([]byte(data), &d) == nil {
				onDelta(d)
			}
		default:
			var e Event
			if data == "" || json.Unmarshal([]byte(data), &e) != nil || e.Seq <= *after {
				return
			}
			*after = e.Seq
			got = true
			onEvent(e)
		}
	})
	return got, ended, err
}

func parseSSE(r io.Reader, fn func(event, data string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			if len(data) > 0 || event != "" {
				fn(event, strings.Join(data, "\n"))
			}
			event, data = "", nil
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "data":
			data = append(data, value)
		}
	}
	return sc.Err()
}
