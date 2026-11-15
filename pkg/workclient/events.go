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

	"go-sre-harness/internal/protocol"
	"go-sre-harness/pkg/eventcursor"
)

// ContractEvents reads a page of the Work timeline in the stable event
// contract.
func (c *Client) ContractEvents(ctx context.Context, workID string, afterSeq int64, limit int) ([]Event, error) {
	var out protocol.EventPage
	_, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/works/%s/events?protocol=1&after_seq=%d&limit=%d", esc(workID), afterSeq, limit),
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
	return c.subscribe(ctx, workID, afterSeq, nil, eventcursor.Key{})
}

type SubscriptionIdentity struct {
	ServerURL string
	TenantID  string
	SubjectID string
}

func (c *Client) SubscribePersistent(ctx context.Context, workID string, store eventcursor.Store, identity SubscriptionIdentity) (<-chan Event, <-chan error) {
	key := eventcursor.Key{
		ServerURL:  identity.ServerURL,
		TenantID:   identity.TenantID,
		SubjectID:  identity.SubjectID,
		Stream:     "work",
		ResourceID: workID,
		Purpose:    "contract-v1",
	}
	return c.subscribe(ctx, workID, 0, store, key)
}

type eventCommit func(context.Context, Event) error

type commitError struct{ err error }

func (e *commitError) Error() string { return "commit event cursor: " + e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }

func (c *Client) subscribe(ctx context.Context, workID string, afterSeq int64, store eventcursor.Store, key eventcursor.Key) (<-chan Event, <-chan error) {
	out := make(chan Event, 64)
	errs := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errs)
		last := afterSeq
		var commit eventCommit
		if store != nil {
			cursor, ok, err := store.Load(ctx, key)
			if err != nil {
				errs <- err
				return
			}
			if ok {
				last = cursor.Sequence
			}
			commit = func(ctx context.Context, ev Event) error {
				if err := store.Advance(ctx, key, ev.Sequence, ev.EventID); err != nil {
					return &commitError{err: err}
				}
				return nil
			}
		}
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
				err = c.drainTerminal(ctx, workID, &last, *terminalTarget, out, commit)
				if err == nil {
					errs <- nil
					return
				}
			} else if err == nil {
				var completed bool
				completed, err = c.streamCommitted(ctx, workID, &last, out, commit)
				if completed && ctx.Err() == nil {
					continue // capture the terminal transaction's full watermark
				}
			}
			switch {
			case ctx.Err() != nil:
				errs <- nil
				return
			case err != nil:
				var commitErr *commitError
				if errors.As(err, &commitErr) {
					errs <- err
					return
				}
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
func (c *Client) drainTerminal(ctx context.Context, workID string, last *int64, target int64, out chan<- Event, commit eventCommit) error {
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
				if commit != nil {
					if err := commit(ctx, ev); err != nil {
						return err
					}
				}
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
	return c.streamCommitted(ctx, workID, last, out, nil)
}

func (c *Client) streamCommitted(ctx context.Context, workID string, last *int64, out chan<- Event, commit eventCommit) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/works/"+esc(workID)+"/events/stream?protocol=1", nil)
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
					if commit != nil {
						if err := commit(ctx, ev); err != nil {
							return false, err
						}
					}
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
