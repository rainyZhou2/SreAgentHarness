// Package stream is the token delta bus (§35.4). Deltas are ephemeral and
// best-effort: a lost delta only degrades the live view. Durable state is the
// committed decision artifact plus bounded stream segments in object storage.
package stream

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"go-sre-harness/internal/artifact"
	gw "go-sre-harness/internal/gateway"
)

// Delta is one normalized model event addressed to a run.
type Delta struct {
	TenantID       string    `json:"tenant_id"`
	RunID          string    `json:"run_id"`
	StepID         string    `json:"step_id"`
	ModelRequestID string    `json:"model_request_id,omitempty"`
	Seq            int64     `json:"delta_seq"`
	Event          gw.Event  `json:"event"`
	At             time.Time `json:"at"`
}

type Bus interface {
	Publish(d Delta)
	// Subscribe returns deltas for one run until cancel is called.
	Subscribe(tenantID, runID string) (<-chan Delta, func())
}

// ---------- in-process ----------

type Local struct {
	mu   sync.Mutex
	subs map[string]map[chan Delta]struct{}
}

func NewLocal() *Local { return &Local{subs: map[string]map[chan Delta]struct{}{}} }

func key(tenant, run string) string { return tenant + "/" + run }

func (l *Local) Publish(d Delta) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs[key(d.TenantID, d.RunID)] {
		select {
		case ch <- d:
		default: // slow consumer: drop (best-effort)
		}
	}
}

func (l *Local) Subscribe(tenant, run string) (<-chan Delta, func()) {
	ch := make(chan Delta, 256)
	l.mu.Lock()
	k := key(tenant, run)
	if l.subs[k] == nil {
		l.subs[k] = map[chan Delta]struct{}{}
	}
	l.subs[k][ch] = struct{}{}
	l.mu.Unlock()
	return ch, func() {
		l.mu.Lock()
		delete(l.subs[k], ch)
		if len(l.subs[k]) == 0 {
			delete(l.subs, k)
		}
		l.mu.Unlock()
	}
}

// ---------- NATS core (multi-process) ----------

// NATS publishes deltas on core NATS (no JetStream persistence by design).
type NATS struct {
	nc  *nats.Conn
	log *slog.Logger
}

func ConnectNATS(url string, log *slog.Logger) (*NATS, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	return &NATS{nc: nc, log: log}, nil
}

func (n *NATS) Close() { n.nc.Close() }

func subject(tenant, run string) string {
	clean := func(s string) string { return strings.NewReplacer(".", "_", "*", "_", ">", "_", " ", "_").Replace(s) }
	return "harness.delta." + clean(tenant) + "." + clean(run)
}

func (n *NATS) Publish(d Delta) {
	b, _ := json.Marshal(d)
	_ = n.nc.Publish(subject(d.TenantID, d.RunID), b)
}

func (n *NATS) Subscribe(tenant, run string) (<-chan Delta, func()) {
	ch := make(chan Delta, 256)
	sub, err := n.nc.Subscribe(subject(tenant, run), func(m *nats.Msg) {
		var d Delta
		if json.Unmarshal(m.Data, &d) == nil && d.TenantID == tenant {
			select {
			case ch <- d:
			default:
			}
		}
	})
	if err != nil {
		close(ch)
		return ch, func() {}
	}
	return ch, func() { _ = sub.Unsubscribe() }
}

// ---------- segment recorder ----------

// SegmentStore persists bounded segment metadata (model_stream_segments).
type SegmentStore interface {
	InsertStreamSegment(ctx context.Context, seg Segment) error
}

type Segment struct {
	TenantID       string
	RunID          string
	ModelRequestID string
	No             int
	FirstSeq       int64
	LastSeq        int64
	Ref            string
	Hash           string
	ExpiresAt      time.Time
}

// Recorder implements cognitive.DeltaPublisher: it numbers deltas, forwards
// them to the bus and writes bounded segments (never one row per token).
type Recorder struct {
	Bus         Bus
	Store       SegmentStore
	Artifacts   artifact.Store
	SegmentSize int
	Retention   time.Duration
	Log         *slog.Logger
	mu          sync.Mutex
	open        map[string]*segBuf // by step id (one model request in flight per step)
}

type segBuf struct {
	tenant, run, reqID string
	seq                int64
	no                 int
	first              int64
	buf                []Delta
}

func (r *Recorder) PublishDelta(tenantID, runID, stepID, modelRequestID string, ev gw.Event) {
	r.mu.Lock()
	if r.open == nil {
		r.open = map[string]*segBuf{}
	}
	sb := r.open[stepID]
	if sb == nil || (modelRequestID != "" && sb.reqID != "" && sb.reqID != modelRequestID) {
		sb = &segBuf{tenant: tenantID, run: runID, reqID: modelRequestID}
		r.open[stepID] = sb
	}
	if sb.reqID == "" {
		sb.reqID = modelRequestID
	}
	sb.seq++
	d := Delta{TenantID: tenantID, RunID: runID, StepID: stepID, ModelRequestID: sb.reqID, Seq: sb.seq, Event: ev, At: time.Now()}
	if len(sb.buf) == 0 {
		sb.first = sb.seq
	}
	sb.buf = append(sb.buf, d)
	var flush []Delta
	var no int
	size := r.SegmentSize
	if size <= 0 {
		size = 256
	}
	final := ev.Type == gw.EvCompleted || ev.Type == gw.EvFailed || ev.Type == gw.EvCancelled
	if len(sb.buf) >= size || final {
		flush, no = sb.buf, sb.no
		sb.buf, sb.no = nil, sb.no+1
	}
	if final {
		delete(r.open, stepID)
	}
	first := sb.first
	r.mu.Unlock()

	if r.Bus != nil {
		r.Bus.Publish(d)
	}
	if len(flush) > 0 && r.Store != nil && r.Artifacts != nil && sb.reqID != "" {
		go r.flush(sb, no, first, flush)
	}
}

func (r *Recorder) flush(sb *segBuf, no int, first int64, ds []Delta) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, _ := json.Marshal(ds)
	ref, hash, err := r.Artifacts.Put(ctx, sb.tenant, "stream-segment", b)
	if err == nil {
		ret := r.Retention
		if ret == 0 {
			ret = 7 * 24 * time.Hour
		}
		err = r.Store.InsertStreamSegment(ctx, Segment{TenantID: sb.tenant, RunID: sb.run, ModelRequestID: sb.reqID, No: no,
			FirstSeq: first, LastSeq: ds[len(ds)-1].Seq, Ref: ref, Hash: hash, ExpiresAt: time.Now().Add(ret)})
	}
	if err != nil && r.Log != nil {
		r.Log.Warn("stream segment not stored", "model_request_id", sb.reqID, "err", err)
	}
}
