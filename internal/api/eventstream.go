package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventbus"
	"go-sre-harness/internal/stream"
)

const (
	StreamPoll = "poll"
	StreamPush = "push"
)

func (s *Server) pushEnabled() bool {
	return s.Events != nil && s.StreamMode == StreamPush
}

// eventHub shares one bus subscription per run between the connections of a
// replica. A run with no subscribers holds no consumer.
type eventHub struct {
	bus  eventbus.Bus
	log  *slog.Logger
	mu   sync.Mutex
	runs map[string]*hubRun
}

type hubRun struct {
	subs      map[chan domain.Event]struct{}
	cancel    context.CancelFunc
	ready     chan struct{}
	readyOnce sync.Once
	closed    bool
}

func newEventHub(bus eventbus.Bus, log *slog.Logger) *eventHub {
	if log == nil {
		log = slog.Default()
	}
	return &eventHub{bus: bus, log: log, runs: map[string]*hubRun{}}
}

func (h *eventHub) subscribe(tenantID, runID string) (<-chan domain.Event, func()) {
	key := tenantID + "/" + runID
	ch := make(chan domain.Event, 512)

	h.mu.Lock()
	hr := h.runs[key]
	if hr == nil {
		ctx, cancel := context.WithCancel(context.Background())
		hr = &hubRun{subs: map[chan domain.Event]struct{}{}, cancel: cancel, ready: make(chan struct{})}
		h.runs[key] = hr
		go h.consume(ctx, key, tenantID, runID, hr)
	}
	hr.subs[ch] = struct{}{}
	ready := hr.ready
	h.mu.Unlock()

	// Wait for the bus subscription so the caller's cursor read happens after
	// live delivery is established.
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
	}

	var once sync.Once
	return ch, func() { once.Do(func() { h.remove(key, hr, ch) }) }
}

func (h *eventHub) remove(key string, hr *hubRun, ch chan domain.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := hr.subs[ch]; !ok {
		return
	}
	delete(hr.subs, ch)
	close(ch)
	if len(hr.subs) == 0 && !hr.closed {
		hr.closed = true
		hr.cancel()
		if h.runs[key] == hr {
			delete(h.runs, key)
		}
	}
}

func (h *eventHub) consume(ctx context.Context, key, tenantID, runID string, hr *hubRun) {
	src, stop, err := h.bus.Subscribe(ctx, tenantID, runID)
	h.signalReady(hr)
	if err != nil {
		h.log.Warn("event subscription unavailable", "run_id", runID, "err", err)
		h.closeAll(key, hr)
		return
	}
	defer stop()
	for {
		select {
		case <-ctx.Done():
			h.closeAll(key, hr)
			return
		case ev, ok := <-src:
			if !ok {
				h.closeAll(key, hr)
				return
			}
			h.broadcast(hr, ev)
		}
	}
}

func (h *eventHub) signalReady(hr *hubRun) {
	hr.readyOnce.Do(func() { close(hr.ready) })
}

func (h *eventHub) broadcast(hr *hubRun, ev domain.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hr.closed {
		return
	}
	for ch := range hr.subs {
		select {
		case ch <- ev:
		default:
			delete(hr.subs, ch)
			close(ch)
		}
	}
}

func (h *eventHub) closeAll(key string, hr *hubRun) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if hr.closed {
		return
	}
	hr.closed = true
	hr.cancel()
	if h.runs[key] == hr {
		delete(h.runs, key)
	}
	for ch := range hr.subs {
		delete(hr.subs, ch)
		close(ch)
	}
}

// streamEvents serves SSE from the durable cursor. Live delivery is a hint:
// every event is confirmed against run_events, so a lost bus message only
// delays output until the next durable check.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	runID := r.PathValue("run_id")
	run, err := s.Store.GetRun(r.Context(), id.tenant, runID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "no_streaming", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	cursor := afterSeq(r)
	if run.Status.Terminal() && run.LastEventSeq <= cursor {
		fmt.Fprintf(w, "event: stream.end\ndata: {\"status\":%q}\n\n", run.Status)
		flusher.Flush()
		return
	}
	if s.Metrics != nil {
		s.Metrics.StreamOpened()
		defer s.Metrics.StreamClosed()
	}

	ctx := r.Context()
	var live <-chan domain.Event
	if s.pushEnabled() && s.hub != nil {
		var cancel func()
		live, cancel = s.hub.subscribe(id.tenant, runID)
		defer cancel()
	}

	var deltas <-chan stream.Delta
	if s.Deltas != nil && r.URL.Query().Get("include_deltas") == "true" {
		ch, cancel := s.Deltas.Subscribe(id.tenant, runID)
		defer cancel()
		deltas = ch
	}

	tick := s.StreamTick
	if live == nil {
		tick = s.PollInterval
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	pollDelay := s.PollInterval
	maxPoll := 5 * time.Second
	if s.StreamTick > 0 && s.StreamTick < maxPoll {
		maxPoll = s.StreamTick
	}
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	var pending []domain.Event
	needRead := true
	readEmpty := false
	sawTerminal := false
	var retry <-chan time.Time
	lastWrite := time.Now()

	for {
		progressed := false
		didRead := false

		for len(pending) > 0 && pending[0].Seq <= cursor+1 {
			ev := pending[0]
			pending = pending[1:]
			if ev.Seq <= cursor {
				continue
			}
			if !s.writeEvent(w, flusher, ev) {
				return
			}
			cursor = ev.Seq
			progressed = true
			lastWrite = time.Now()
			if s.Metrics != nil {
				s.Metrics.ObserveStreamEvent("live")
			}
			if isTerminalEvent(ev.Type) {
				sawTerminal = true
			}
		}

		gap := len(pending) > 0 && pending[0].Seq > cursor+1
		if needRead || gap {
			needRead = false
			didRead = true
			reason := "catchup"
			if live == nil {
				reason = "poll"
			}
			if s.Metrics != nil {
				s.Metrics.ObserveStreamRead(reason)
			}
			events, err := s.Store.ListEvents(ctx, id.tenant, runID, cursor, 500)
			if err != nil {
				return
			}
			readEmpty = len(events) == 0
			for _, ev := range events {
				if ev.Seq <= cursor {
					continue
				}
				if !s.writeEvent(w, flusher, ev) {
					return
				}
				cursor = ev.Seq
				lastWrite = time.Now()
				if s.Metrics != nil {
					s.Metrics.ObserveStreamEvent("durable")
				}
				if isTerminalEvent(ev.Type) {
					sawTerminal = true
				}
			}
			if len(events) > 0 {
				progressed = true
				if len(events) == 500 {
					needRead = true
				}
			}
			if len(pending) > 0 && pending[0].Seq > cursor+1 && readEmpty {
				retry = time.After(250 * time.Millisecond)
			}
		}

		if live == nil && didRead {
			if readEmpty {
				if pollDelay < maxPoll {
					pollDelay = min(pollDelay*2, maxPoll)
					ticker.Reset(pollDelay)
				}
			} else if pollDelay != s.PollInterval {
				pollDelay = s.PollInterval
				ticker.Reset(pollDelay)
			}
		}

		if sawTerminal {
			final, err := s.Store.GetRun(ctx, id.tenant, runID)
			if err == nil && final.Status.Terminal() && final.LastEventSeq <= cursor {
				fmt.Fprintf(w, "event: stream.end\ndata: {\"status\":%q}\n\n", final.Status)
				flusher.Flush()
				return
			}
			if err == nil && final.LastEventSeq > cursor {
				needRead = true
				if readEmpty {
					retry = time.After(250 * time.Millisecond)
				}
			}
		}

		if progressed || (needRead && !readEmpty) {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case ev, ok := <-live:
			if !ok {
				live = nil
				pollDelay = s.PollInterval
				ticker.Reset(pollDelay)
				needRead = true
				continue
			}
			if ev.Seq <= cursor {
				continue
			}
			if len(pending) >= 4096 {
				needRead = true
				continue
			}
			pending = append(pending, ev)
		case <-ticker.C:
			needRead = true
		case <-retry:
			retry = nil
			needRead = true
		case d, ok := <-deltas:
			if !ok {
				deltas = nil
				continue
			}
			data, _ := json.Marshal(d)
			fmt.Fprintf(w, "event: model.delta\ndata: %s\n\n", data)
			flusher.Flush()
			lastWrite = time.Now()
		case <-keepalive.C:
			if time.Since(lastWrite) >= 15*time.Second {
				fmt.Fprint(w, ": keepalive\n\n")
				flusher.Flush()
				lastWrite = time.Now()
			}
		}
	}
}

func (s *Server) writeEvent(w http.ResponseWriter, f http.Flusher, e domain.Event) bool {
	data, err := json.Marshal(e)
	if err != nil {
		return false
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, data); err != nil {
		return false
	}
	f.Flush()
	return true
}

func isTerminalEvent(t string) bool {
	switch t {
	case "run.completed", "run.failed", "run.cancelled":
		return true
	}
	return false
}
