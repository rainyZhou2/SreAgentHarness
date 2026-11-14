package workclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/quota"
	"go-sre-harness/internal/work"
)

// v3 wire types.
type (
	// Event is one entry of the stable event contract.
	Event         = protocol.Event
	ProtocolError = protocol.Error
	View          = protocol.View
	Graph         = work.Graph
	CreateChild   = orchestration.ChildRequest
	Improvement   = orchestration.Improvement
	Finding       = orchestration.Finding
	Source        = observability.Source
	Query         = observability.Query
	QueryResult   = observability.Result
	SourceProbe   = observability.Health
	Attestation   = governance.Attestation
	Budget        = quota.Budget
	Intent        = orchestration.Intent
	DryRunPlan    = orchestration.DryRunPlan
)

// Graph returns the Work tree a Work belongs to.
func (c *Client) Graph(ctx context.Context, workID string) (Graph, error) {
	var g Graph
	_, err := c.do(ctx, http.MethodGet, "/v1/work-graphs/"+esc(workID), nil, nil, &g)
	return g, err
}

// CreateChild creates a child Work inside its parent's ceiling.
func (c *Client) CreateChild(ctx context.Context, parentID, key string, req CreateChild) (Work, error) {
	var w Work
	_, err := c.do(ctx, http.MethodPost, "/v1/works/"+esc(parentID)+"/children", map[string]string{"Idempotency-Key": key}, req, &w)
	return w, err
}

// View returns the unified lifecycle view of a Work.
func (c *Client) View(ctx context.Context, workID string) (View, error) {
	var v View
	_, err := c.do(ctx, http.MethodGet, "/v1/works/"+esc(workID)+"/view", nil, nil, &v)
	if err != nil {
		return View{}, err
	}
	if v.Work.ID != workID {
		return View{}, &protocol.Error{Code: protocol.InvalidFrame, Message: "snapshot belongs to another Work"}
	}
	return v, nil
}

// Findings lists the validated agent findings of a Work.
func (c *Client) Findings(ctx context.Context, workID string) ([]Finding, error) {
	var out struct {
		Items []Finding `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/works/"+esc(workID)+"/findings", nil, nil, &out)
	return out.Items, err
}

// Improvements opens Improvement Works for a closed Work (items empty: the
// follow-ups of its postmortem report).
func (c *Client) Improvements(ctx context.Context, workID string, items []Improvement) ([]Work, error) {
	var out struct {
		Items []Work `json:"items"`
	}
	_, err := c.do(ctx, http.MethodPost, "/v1/works/"+esc(workID)+"/improvements", nil, map[string]any{"items": items}, &out)
	return out.Items, err
}

// Sources lists the observability Source Catalog.
func (c *Client) Sources(ctx context.Context) ([]Source, error) {
	var out struct {
		Items []Source `json:"items"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/observability/sources", nil, nil, &out)
	return out.Items, err
}

// PutSource creates (version 0) or updates a source at the given version.
func (c *Client) PutSource(ctx context.Context, s Source, version int64, reason string) (Source, error) {
	hdr := map[string]string{}
	if version > 0 {
		hdr["If-Match"] = strconv.FormatInt(version, 10)
	}
	body := struct {
		Source
		Reason string `json:"reason,omitempty"`
	}{s, reason}
	var out Source
	_, err := c.do(ctx, http.MethodPost, "/v1/observability/sources", hdr, body, &out)
	return out, err
}

// ProbeSource checks one source's reachability and authorization.
func (c *Client) ProbeSource(ctx context.Context, id string) (SourceProbe, error) {
	var h SourceProbe
	_, err := c.do(ctx, http.MethodPost, "/v1/observability/sources/"+esc(id)+"/probe", nil, struct{}{}, &h)
	return h, err
}

// Observe runs one authorized read-only query; workID ties it to a Work
// (scope, budget and evidence).
func (c *Client) Observe(ctx context.Context, workID string, q Query) (QueryResult, string, error) {
	body := struct {
		Query
		WorkID string `json:"work_id,omitempty"`
	}{q, workID}
	var out struct {
		Result      QueryResult `json:"result"`
		EvidenceRef string      `json:"evidence_ref"`
	}
	_, err := c.do(ctx, http.MethodPost, "/v1/observability/query", nil, body, &out)
	return out.Result, out.EvidenceRef, err
}

// Attestation returns the deployment's launch attestation.
func (c *Client) Attestation(ctx context.Context) (Attestation, error) {
	var a Attestation
	_, err := c.do(ctx, http.MethodGet, "/v1/governance/attestation", nil, nil, &a)
	return a, err
}

// Quotas returns the tenant's cluster-wide budgets.
func (c *Client) Quotas(ctx context.Context) ([]Budget, error) {
	var out struct {
		Budgets []Budget `json:"budgets"`
	}
	_, err := c.do(ctx, http.MethodGet, "/v1/quotas", nil, nil, &out)
	return out.Budgets, err
}

// RouteIntent classifies a request into a Work kind, template and ceiling.
func (c *Client) RouteIntent(ctx context.Context, text string) (Intent, error) {
	var i Intent
	_, err := c.do(ctx, http.MethodPost, "/v1/work-intents", nil, map[string]string{"text": text}, &i)
	return i, err
}

// DryRun simulates a template for a goal without side effects.
func (c *Client) DryRun(ctx context.Context, templateID string, goal Goal) (DryRunPlan, error) {
	var p DryRunPlan
	_, err := c.do(ctx, http.MethodPost, "/v1/work-templates/"+esc(templateID)+"/dry-run", nil, map[string]any{"goal": goal}, &p)
	return p, err
}

// ContractEvents reads a page of the Work timeline in the stable event
// contract.
func (c *Client) ContractEvents(ctx context.Context, workID string, afterSeq int64, limit int) ([]Event, error) {
	var out protocol.EventPage
	_, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/works/%s/events?protocol=1&after_seq=%d&limit=%d", esc(workID), afterSeq, limit),
		nil, nil, &out)
	if err != nil {
		return nil, err
	}
	last := afterSeq
	for _, ev := range out.Items {
		if !ev.Durable() || ev.WorkID != workID || ev.Sequence != last+1 {
			return nil, &protocol.Error{Code: protocol.InvalidFrame, Message: "event page must contain contiguous durable events of the requested Work"}
		}
		last = ev.Sequence
	}
	if out.NextAfterSeq != last {
		return nil, &protocol.Error{Code: protocol.InvalidFrame, Message: "event page cursor does not match its events"}
	}
	return out.Items, nil
}

// Subscribe streams the events of a Work in the stable contract, starting
// after afterSeq. The stream survives disconnects: the client reconnects
// with the last sequence it delivered, so every persisted event arrives
// exactly once and in order. The channel is closed when ctx ends or the
// client has delivered through a captured terminal snapshot LastSeq. Later
// audit events require another subscription. The error channel yields the reason
// (nil for a clean end).
//
//	events, errs := client.Subscribe(ctx, work.ID, work.LastSeq)
//	for ev := range events {
//		fmt.Println(ev.Type, string(ev.Payload))
//	}
//	if err := <-errs; err != nil { ... }
func (c *Client) Subscribe(ctx context.Context, workID string, afterSeq int64) (<-chan Event, <-chan error) {
	out := make(chan Event, 64)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		last := afterSeq
		var terminalTarget *int64
		backoff := 500 * time.Millisecond
		for {
			// A completed event can precede the final events of its transaction.
			// Capture one committed terminal watermark, then keep it across
			// disconnects; later audit events belong to a later subscription.
			var err error
			if terminalTarget == nil {
				var snapshot View
				snapshot, err = c.View(ctx, workID)
				if err == nil && snapshot.Work.Status.Terminal() {
					if snapshot.LastSeq < last {
						err = &protocol.Error{Code: protocol.InvalidFrame, Message: "terminal snapshot precedes delivered cursor"}
					} else {
						target := snapshot.LastSeq
						terminalTarget = &target
					}
				}
			}
			if err == nil && terminalTarget != nil {
				err = c.drainTerminal(ctx, workID, &last, *terminalTarget, out)
				if err == nil {
					errs <- nil
					return
				}
			} else if err == nil {
				var completed bool
				completed, err = c.stream(ctx, workID, &last, out)
				if completed && ctx.Err() == nil {
					continue // capture the terminal transaction's full watermark
				}
			}
			switch {
			case ctx.Err() != nil:
				errs <- nil
				return
			case err != nil:
				var protocolErr *protocol.Error
				if errors.As(err, &protocolErr) {
					errs <- err
					return
				}
				var apiErr *Error
				if errors.As(err, &apiErr) && apiErr.Status >= 400 && apiErr.Status < 500 {
					errs <- err
					return
				}
			}
			select {
			case <-ctx.Done():
				errs <- nil
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
		}
	}()
	return out, errs
}

// drainTerminal delivers only through the captured terminal watermark. Missing
// events fail closed; transport failures let Subscribe retry from last.
func (c *Client) drainTerminal(ctx context.Context, workID string, last *int64, target int64, out chan<- Event) error {
	for *last < target {
		limit := 200
		if remaining := target - *last; remaining < int64(limit) {
			limit = int(remaining)
		}
		events, err := c.ContractEvents(ctx, workID, *last, limit)
		if err != nil {
			return err
		}
		if len(events) == 0 || len(events) > limit {
			return &protocol.Error{Code: protocol.InvalidFrame, Message: "terminal event page does not respect the captured watermark"}
		}
		for _, ev := range events {
			select {
			case out <- ev:
				*last = ev.Sequence
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// stream reads one SSE connection. It reports done when the Work
// completed.
func (c *Client) stream(ctx context.Context, workID string, last *int64, out chan<- Event) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/v1/works/"+esc(workID)+"/events/stream?protocol=1", nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "text/event-stream, "+protocol.MediaType)
	req.Header.Set("Last-Event-ID", strconv.FormatInt(*last, 10))
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for k, vs := range c.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	// A stream has no overall timeout; ctx ends it.
	hc := &http.Client{}
	if c.HTTP != nil {
		hc.Transport = c.HTTP.Transport
	}
	resp, err := hc.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, &Error{Status: resp.StatusCode, Code: "stream", Message: resp.Status}
	}
	if err := streamVersions(resp.Header); err != nil {
		return false, err
	}
	sc := bufio.NewScanner(resp.Body)
	const maxFrame = 4 << 20
	sc.Buffer(make([]byte, 64<<10), maxFrame)
	var data strings.Builder
	var eventType string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			part := strings.TrimPrefix(line, "data:")
			part = strings.TrimPrefix(part, " ")
			if data.Len()+len(part)+1 > maxFrame {
				return false, &protocol.Error{Code: protocol.InvalidFrame, Message: "SSE frame exceeds size limit"}
			}
			data.WriteString(part)
			data.WriteByte('\n')
		case line == "":
			if data.Len() == 0 {
				eventType = ""
				continue
			}
			if eventType == "error" {
				return false, &Error{Status: http.StatusServiceUnavailable, Code: "stream", Message: "server could not read the event log"}
			}
			var ev Event
			err := json.Unmarshal([]byte(data.String()), &ev)
			data.Reset()
			eventType = ""
			if err != nil {
				var protocolErr *protocol.Error
				if errors.As(err, &protocolErr) {
					return false, err
				}
				return false, &protocol.Error{Code: protocol.InvalidFrame, Message: "invalid SSE event JSON: " + err.Error()}
			}
			if err := ev.Validate(); err != nil {
				return false, err
			}
			if ev.WorkID != "" && ev.WorkID != workID {
				return false, &protocol.Error{Code: protocol.InvalidFrame, Message: "event belongs to another Work"}
			}
			if ev.Durable() && ev.Sequence <= *last {
				continue
			}
			if ev.Durable() && ev.Sequence != *last+1 {
				return false, &protocol.Error{Code: protocol.InvalidFrame, Message: "durable event sequence has a gap"}
			}
			select {
			case out <- ev:
				if ev.Durable() {
					*last = ev.Sequence
				}
			case <-ctx.Done():
				return false, ctx.Err()
			}
			if ev.Durable() && ev.Type == protocol.WorkCompleted {
				return true, nil
			}
		}
	}
	if errors.Is(sc.Err(), bufio.ErrTooLong) {
		return false, &protocol.Error{Code: protocol.InvalidFrame, Message: "SSE line exceeds size limit"}
	}
	if sc.Err() != nil {
		return false, sc.Err()
	}
	if data.Len() > 0 {
		return false, io.ErrUnexpectedEOF
	}
	return false, nil
}

// Old streams may omit headers; each event still has to declare legacy v1 or
// the complete current tuple. New streams can be rejected before any frame.
func streamVersions(h http.Header) error {
	names := []string{protocol.HeaderSchemaVersion, protocol.HeaderContractVersion, protocol.HeaderMinimumReaderVersion}
	values := [3]int{}
	present := 0
	for i, name := range names {
		value := h.Get(name)
		if value == "" {
			continue
		}
		present++
		n, err := strconv.Atoi(value)
		if err != nil {
			return &protocol.Error{Code: protocol.InvalidFrame, Message: "invalid protocol header " + name}
		}
		values[i] = n
	}
	if present == 0 {
		return nil
	}
	if present == 1 && values[0] == 1 {
		return nil
	}
	if present != 3 || values[0] <= 0 || values[1] <= 0 || values[2] <= 0 {
		return &protocol.Error{Code: protocol.IncompatibleVersion, Message: "incomplete protocol headers"}
	}
	return protocol.ValidateVersions(values[0], values[1], values[2])
}

// Works is a resource-style facade over the Work calls:
//
//	client := workclient.New(baseURL, token)
//	w, err := client.Works().Create(ctx, "incident-20261110-search-p99", workclient.CreateWork{...})
//	events, errs := client.Works().Subscribe(ctx, w.ID, w.LastSeq)
type Works struct{ c *Client }

// Works returns the Work facade.
func (c *Client) Works() Works { return Works{c} }

func (w Works) Create(ctx context.Context, key string, req CreateWork) (Work, error) {
	return w.c.CreateWork(ctx, key, req)
}
func (w Works) Get(ctx context.Context, id string) (Work, error)  { return w.c.GetWork(ctx, id) }
func (w Works) View(ctx context.Context, id string) (View, error) { return w.c.View(ctx, id) }
func (w Works) Subscribe(ctx context.Context, id string, afterSeq int64) (<-chan Event, <-chan error) {
	return w.c.Subscribe(ctx, id, afterSeq)
}
