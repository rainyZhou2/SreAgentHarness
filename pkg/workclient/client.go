// Package workclient is a typed HTTP client for the v2 Work API
// (/v1/works, /v1/events, /v1/world, /v1/actions).
//
//	c := workclient.New("http://localhost:8080", token)
//	w, err := c.CreateWork(ctx, "deploy-123", workclient.CreateWork{Kind: "incident", Title: "...",
//		Goal: workclient.Goal{Intent: "mitigate", SubjectRefs: []string{"service://prod/checkout"}}})
//
// Every mutating call takes an idempotency key: retrying a call with the
// same key is always safe.
package workclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// Re-exported wire types.
type (
	Work          = work.Work
	Goal          = work.GoalSpec
	Template      = work.Template
	WorkEvent     = work.Event
	RunLink       = work.RunLink
	CreateWork    = orchestration.CreateRequest
	Command       = orchestration.CommandRequest
	CommandOpts   = orchestration.CommandPayload
	Envelope      = eventing.Envelope
	InboxRecord   = eventing.Record
	Action        = action.Record
	Evaluation    = outcome.Evaluation
	SuccessSpec   = outcome.Spec
	Objective     = outcome.Objective
	Evidence      = evidence.Record
	Hypothesis    = evidence.Hypothesis
	Fact          = world.Fact
	WorldSnapshot = world.Snapshot
)

// Error is a non-2xx API response.
type Error struct {
	Status  int    `json:"-"`
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("work api: %d %s: %s", e.Status, e.Code, e.Message)
}

// Client calls the Work API.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Header adds headers to every request (e.g. dev auth headers).
	Header http.Header
}

// New returns a client with a 30s timeout.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{Timeout: 30 * time.Second}}
}

func (c *Client) do(ctx context.Context, method, path string, hdr map[string]string, in, out any) (http.Header, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for k, vs := range c.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	for k, v := range hdr {
		if v != "" {
			req.Header.Set(k, v)
		}
	}
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return resp.Header, err
	}
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode}
		_ = json.Unmarshal(data, e)
		if e.Code == "" {
			e.Code = http.StatusText(resp.StatusCode)
		}
		return resp.Header, e
	}
	// Only protocol responses participate; unrelated domain types may expose
	// their own Validate method with entirely different lifecycle semantics.
	var validateContract func() error
	switch v := out.(type) {
	case *protocol.EventPage:
		validateContract = func() error { return v.Validate() }
	case *protocol.View:
		validateContract = func() error { return v.Validate() }
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			if validateContract != nil {
				var protocolErr *protocol.Error
				if errors.As(err, &protocolErr) {
					return resp.Header, err
				}
				return resp.Header, &protocol.Error{Code: protocol.InvalidFrame, Message: "invalid contract JSON: " + err.Error()}
			}
			return resp.Header, fmt.Errorf("decode %s %s: %w", method, path, err)
		}
	}
	if validateContract != nil {
		return resp.Header, validateContract()
	}
	return resp.Header, nil
}

func esc(s string) string { return url.PathEscape(s) }

// CreateWork creates a Work (idempotent on key).
func (c *Client) CreateWork(ctx context.Context, key string, req CreateWork) (Work, error) {
	var w Work
	_, err := c.do(ctx, http.MethodPost, "/v1/works", map[string]string{"Idempotency-Key": key}, req, &w)
	return w, err
}

// GetWork reads a Work.
func (c *Client) GetWork(ctx context.Context, id string) (Work, error) {
	var w Work
	_, err := c.do(ctx, http.MethodGet, "/v1/works/"+esc(id), nil, nil, &w)
	return w, err
}

// ListFilter selects Works.
type ListFilter struct {
	Status  []string
	Kind    string
	Subject string
	Limit   int
}

// ListWorks lists Works (newest first).
func (c *Client) ListWorks(ctx context.Context, f ListFilter) ([]Work, error) {
	q := url.Values{}
	if len(f.Status) > 0 {
		q.Set("status", strings.Join(f.Status, ","))
	}
	if f.Kind != "" {
		q.Set("kind", f.Kind)
	}
	if f.Subject != "" {
		q.Set("subject", f.Subject)
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	var out struct {
		Items []Work `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/works?"+q.Encode(), nil, nil, &out)
	return out.Items, err
}

// Command sends an operator command. expectedVersion (If-Match) may be 0.
func (c *Client) Command(ctx context.Context, workID, key, typ string, opts *CommandOpts, reason string, expectedVersion int64) (Work, error) {
	body := map[string]any{"type": typ}
	if reason != "" {
		body["reason"] = reason
	}
	if opts != nil {
		body["payload"] = opts
	}
	hdr := map[string]string{"Idempotency-Key": key}
	if expectedVersion > 0 {
		hdr["If-Match"] = fmt.Sprintf(`"%d"`, expectedVersion)
	}
	var w Work
	_, err := c.do(ctx, http.MethodPost, "/v1/works/"+esc(workID)+"/commands", hdr, body, &w)
	return w, err
}

// Events pages the Work timeline after a sequence number.
func (c *Client) Events(ctx context.Context, workID string, afterSeq int64, limit int) ([]WorkEvent, error) {
	var out struct {
		Items []WorkEvent `json:"items"`
	}
	path := fmt.Sprintf("/v1/works/%s/events?after_seq=%d&limit=%d", esc(workID), afterSeq, limit)
	_, err := c.do(ctx, http.MethodGet, path, nil, nil, &out)
	return out.Items, err
}

// Actions lists a Work's actions.
func (c *Client) Actions(ctx context.Context, workID string) ([]Action, error) {
	var out struct {
		Items []Action `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/works/"+esc(workID)+"/actions", nil, nil, &out)
	return out.Items, err
}

// Outcomes lists a Work's outcome evaluations (newest first).
func (c *Client) Outcomes(ctx context.Context, workID string) ([]Evaluation, error) {
	var out struct {
		Items []Evaluation `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/works/"+esc(workID)+"/outcomes", nil, nil, &out)
	return out.Items, err
}

// IngestResult is the inbox decision for an event.
type IngestResult struct {
	InboxID   string `json:"inbox_id"`
	Duplicate bool   `json:"duplicate"`
	State     string `json:"state"`
	WorkID    string `json:"work_id"`
	Decision  string `json:"decision"`
}

// IngestEvent submits an event (deduplicated on source + event_id).
func (c *Client) IngestEvent(ctx context.Context, e Envelope) (IngestResult, error) {
	var out IngestResult
	_, err := c.do(ctx, http.MethodPost, "/v1/events", nil, e, &out)
	return out, err
}

// GetAction reads an action.
func (c *Client) GetAction(ctx context.Context, id string) (Action, error) {
	var a Action
	_, err := c.do(ctx, http.MethodGet, "/v1/actions/"+esc(id), nil, nil, &a)
	return a, err
}

// DecideAction approves or rejects an action. intentHash must be the hash
// shown to the approver; a changed plan invalidates the decision.
func (c *Client) DecideAction(ctx context.Context, id, decision, intentHash, comment string) (Action, error) {
	var a Action
	_, err := c.do(ctx, http.MethodPost, "/v1/actions/"+esc(id)+"/approve", nil,
		map[string]string{"decision": decision, "intent_hash": intentHash, "comment": comment}, &a)
	return a, err
}

// Facts reads facts visible at (asOf, knownAt); zero times mean now.
func (c *Client) Facts(ctx context.Context, subjects []string, asOf, knownAt time.Time) ([]Fact, error) {
	q := url.Values{}
	for _, s := range subjects {
		q.Add("subject", s)
	}
	if !asOf.IsZero() {
		q.Set("as_of", asOf.UTC().Format(time.RFC3339Nano))
	}
	if !knownAt.IsZero() {
		q.Set("known_at", knownAt.UTC().Format(time.RFC3339Nano))
	}
	var out struct {
		Items []Fact `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/world/facts?"+q.Encode(), nil, nil, &out)
	return out.Items, err
}

// AssertFact records an observation (stored as untrusted unless the caller
// is an admin).
func (c *Client) AssertFact(ctx context.Context, f Fact) (Fact, error) {
	var out Fact
	_, err := c.do(ctx, http.MethodPost, "/v1/world/facts", nil, f, &out)
	return out, err
}

// WaitFor polls a Work until done returns true or ctx ends.
func (c *Client) WaitFor(ctx context.Context, id string, every time.Duration, done func(Work) bool) (Work, error) {
	if every <= 0 {
		every = time.Second
	}
	for {
		w, err := c.GetWork(ctx, id)
		if err == nil && done(w) {
			return w, nil
		}
		select {
		case <-ctx.Done():
			return w, ctx.Err()
		case <-time.After(every):
		}
	}
}
