package extensions

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// httpTool invokes an HTTP endpoint.
type httpTool struct {
	desc   tools.Descriptor
	spec   HTTPSpec
	client *http.Client
	max    int
}

func newHTTPTool(name string, s HTTPSpec, client *http.Client) *httpTool {
	schema := s.InputSchema
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	timeout := 30 * time.Second
	if d, err := time.ParseDuration(s.Timeout); err == nil && d > 0 {
		timeout = d
	}
	if client == nil {
		client = &http.Client{}
	}
	c := *client
	c.Timeout = timeout
	max := s.MaxOutputBytes
	if max == 0 {
		max = 64 << 10
	}
	return &httpTool{spec: s, client: &c, max: max, desc: tools.Descriptor{
		Name: "http." + name, Version: "1", Risk: s.Risk, Description: s.Description, InputSchema: schema,
		TargetArg: s.TargetArg, Trust: s.Trust, Source: "http:" + name}}
}

func (t *httpTool) Descriptor() tools.Descriptor { return t.desc }

func (t *httpTool) Invoke(ctx context.Context, idempotencyKey string, args json.RawMessage) (json.RawMessage, error) {
	if len(args) == 0 {
		args = json.RawMessage(`{}`)
	}
	var req *http.Request
	var err error
	if t.spec.Method == "GET" {
		var m map[string]any
		if json.Unmarshal(args, &m) != nil {
			return nil, fmt.Errorf("%w: arguments must be an object", domain.ErrNotDispatched)
		}
		u, _ := url.Parse(t.spec.URL)
		q := u.Query()
		for k, v := range m {
			if s, ok := v.(string); ok {
				q.Set(k, s)
			} else {
				b, _ := json.Marshal(v)
				q.Set(k, string(b))
			}
		}
		u.RawQuery = q.Encode()
		req, err = http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, http.MethodPost, t.spec.URL, bytes.NewReader(args))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	for k, v := range t.spec.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Idempotency-Key", idempotencyKey)
	req.Header.Set("User-Agent", "sre-harness-tool")
	if ci, ok := tools.CallInfoFrom(ctx); ok {
		req.Header.Set("X-Harness-Run-ID", ci.RunID)
		req.Header.Set("X-Harness-Step-ID", ci.StepID)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		if t.desc.Risk.HasSideEffects() {
			// The request may or may not have reached the endpoint: the
			// kernel's UNKNOWN handling (verify, then retry or ask a human)
			// takes over; only reads are retried blindly.
			return nil, fmt.Errorf("http tool %s: %w", t.desc.Name, err)
		}
		return nil, domain.Retryable(err, 5*time.Second)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, int64(t.max)+1))
	truncated := len(body) > t.max
	if truncated {
		body = body[:t.max]
	}
	if resp.StatusCode >= 500 && !t.desc.Risk.HasSideEffects() {
		return nil, domain.Retryable(fmt.Errorf("http tool %s: status %d", t.desc.Name, resp.StatusCode), 5*time.Second)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("http tool %s: status %d: %s", t.desc.Name, resp.StatusCode, clipString(string(body), 1024))
	}
	out := map[string]any{"status": resp.StatusCode, "truncated": truncated}
	if !truncated && json.Valid(body) {
		out["content"] = json.RawMessage(body)
	} else {
		out["content"] = string(body)
	}
	return json.Marshal(out)
}

func clipString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
