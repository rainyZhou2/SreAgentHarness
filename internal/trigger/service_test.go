package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// ---------- fakes ----------

type fakeRuns struct {
	mu       sync.Mutex
	runs     map[string]*domain.Run
	byKey    map[string]string
	reqs     []api.StartRunRequest
	evidence []string
	startErr error
}

func newFakeRuns() *fakeRuns {
	return &fakeRuns{runs: map[string]*domain.Run{}, byKey: map[string]string{}}
}

func (f *fakeRuns) StartRun(_ context.Context, req api.StartRunRequest) (domain.Run, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return domain.Run{}, false, f.startErr
	}
	if id, ok := f.byKey[req.IdempotencyKey]; ok {
		return *f.runs[id], false, nil
	}
	id := fmt.Sprintf("run-%d", len(f.runs)+1)
	f.runs[id] = &domain.Run{ID: id, TenantID: req.TenantID, Status: domain.RunRunning}
	f.byKey[req.IdempotencyKey] = id
	f.reqs = append(f.reqs, req)
	return *f.runs[id], true, nil
}

func (f *fakeRuns) GetRun(_ context.Context, _, runID string) (domain.Run, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.runs[runID]
	if !ok {
		return domain.Run{}, errors.New("not found")
	}
	return *r, nil
}

func (f *fakeRuns) InjectEvidence(_ context.Context, _, runID, _, source, content, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evidence = append(f.evidence, runID+"|"+source+"|"+content)
	return nil
}

func (f *fakeRuns) finish(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.runs[id].Status = domain.RunCompleted
}

type claimRec struct {
	id      string
	runID   string
	hits    int
	expires time.Time
}

type fakeDedup struct {
	mu     sync.Mutex
	now    func() time.Time
	claims map[string]*claimRec
	seq    int
}

func newFakeDedup(now func() time.Time) *fakeDedup {
	return &fakeDedup{now: now, claims: map[string]*claimRec{}}
}

func (d *fakeDedup) ClaimTrigger(_ context.Context, tenantID, key string, window time.Duration, extend bool) (domain.TriggerClaim, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := tenantID + "/" + key
	now := d.now()
	if c, ok := d.claims[k]; ok && now.Before(c.expires) {
		c.hits++
		if extend {
			c.expires = now.Add(window)
		}
		return domain.TriggerClaim{ID: c.id, RunID: c.runID, Hits: c.hits, ExpiresAt: c.expires}, nil
	}
	d.seq++
	c := &claimRec{id: fmt.Sprintf("claim-%d", d.seq), hits: 1, expires: now.Add(window)}
	d.claims[k] = c
	return domain.TriggerClaim{ID: c.id, Fresh: true, Hits: 1, ExpiresAt: c.expires}, nil
}

func (d *fakeDedup) BindTrigger(_ context.Context, tenantID, key, claimID, runID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.claims[tenantID+"/"+key]; ok && c.id == claimID {
		c.runID = runID
	}
	return nil
}

func (d *fakeDedup) ReleaseTrigger(_ context.Context, tenantID, key, claimID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c, ok := d.claims[tenantID+"/"+key]; ok && c.id == claimID {
		delete(d.claims, tenantID+"/"+key)
	}
	return nil
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time      { return c.t }
func (c *clock) add(d time.Duration) { c.t = c.t.Add(d) }

func newService(t *testing.T, cfgs ...Config) (*Service, *fakeRuns, *fakeDedup, *clock) {
	t.Helper()
	clk := &clock{t: time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)}
	runs, dedup := newFakeRuns(), newFakeDedup(clk.now)
	s, err := New(cfgs, runs, dedup, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	s.Now = clk.now
	return s, runs, dedup, clk
}

func alertTrigger() Config {
	return Config{
		Name:        "alerts",
		Items:       "payload.alerts",
		Filter:      "item.labels.severity != 'info'",
		Resolve:     "item.status == 'resolved'",
		Goal:        "Investigate ${item.labels.alertname} on ${item.labels.service}",
		Fingerprint: "${item.fingerprint}",
		Services:    []string{"${item.labels.service}"},
		Labels:      map[string]string{"severity": "${item.labels.severity}"},
		Window:      &WindowConfig{Start: "${item.startsAt}", Lookback: tools30m()},
		Mode:        "react",
	}
}

func alert(fp, status, sev string) map[string]any {
	return map[string]any{"fingerprint": fp, "status": status, "startsAt": "2026-11-07T09:50:00Z",
		"labels": map[string]any{"alertname": "HighLatency", "service": "Checkout", "severity": sev}}
}

func deliver(t *testing.T, s *Service, name string, payload any) Result {
	t.Helper()
	b, _ := json.Marshal(payload)
	res, err := s.Handle(context.Background(), "t1", "alertmanager", name, b, http.Header{})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	return res
}

// ---------- tests ----------

func TestHandleCreatesRunsWithRenderedContext(t *testing.T) {
	s, runs, _, _ := newService(t, alertTrigger())
	res := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical"), alert("a2", "firing", "info")}})
	if len(res.Items) != 2 || res.Items[0].Action != ActionCreated || res.Items[1].Action != ActionFiltered {
		t.Fatalf("results %+v", res.Items)
	}
	if len(runs.reqs) != 1 {
		t.Fatalf("expected one run, got %d", len(runs.reqs))
	}
	req := runs.reqs[0]
	if req.Goal != "Investigate HighLatency on Checkout" || req.InitiatorType != "trigger" {
		t.Fatalf("request %+v", req)
	}
	gc := req.Context
	if len(gc.Services) != 1 || gc.Labels["severity"] != "critical" || gc.Mode != "react" || gc.Source != "trigger:alerts" {
		t.Fatalf("context %+v", gc)
	}
	wantStart := time.Date(2026, 11, 7, 9, 20, 0, 0, time.UTC)
	if gc.Window == nil || !gc.Window.Start.Equal(wantStart) {
		t.Fatalf("window %+v, want start %s", gc.Window, wantStart)
	}
	if !strings.HasPrefix(req.IdempotencyKey, "trigger:claim-") {
		t.Fatalf("idempotency key %q", req.IdempotencyKey)
	}
}

func TestHandleDeduplicatesAndInjectsEvidence(t *testing.T) {
	s, runs, _, clk := newService(t, alertTrigger())
	first := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	clk.add(time.Minute)
	second := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	if second.Items[0].Action != ActionDuplicate || second.Items[0].RunID != first.Items[0].RunID || second.Items[0].Hits != 2 {
		t.Fatalf("second delivery %+v", second.Items[0])
	}
	if len(runs.reqs) != 1 || len(runs.evidence) != 1 {
		t.Fatalf("runs=%d evidence=%d", len(runs.reqs), len(runs.evidence))
	}
	// A resolve event attaches to the run and never starts one.
	third := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "resolved", "critical")}})
	if third.Items[0].Action != ActionResolved || !strings.Contains(runs.evidence[1], "trigger:alerts:resolved") {
		t.Fatalf("resolve %+v evidence %v", third.Items[0], runs.evidence)
	}
}

func TestResolveWithoutRunIsIgnored(t *testing.T) {
	s, runs, dedup, _ := newService(t, alertTrigger())
	res := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("zz", "resolved", "critical")}})
	if res.Items[0].Action != ActionIgnored || len(runs.reqs) != 0 || len(dedup.claims) != 0 {
		t.Fatalf("got %+v runs=%d claims=%d", res.Items[0], len(runs.reqs), len(dedup.claims))
	}
}

func TestTerminalRunStartsNewOccurrence(t *testing.T) {
	s, runs, _, clk := newService(t, alertTrigger())
	first := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	runs.finish(first.Items[0].RunID)
	clk.add(time.Minute)
	second := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	if second.Items[0].Action != ActionCreated || second.Items[0].RunID == first.Items[0].RunID {
		t.Fatalf("expected a new run, got %+v", second.Items[0])
	}
}

func TestDedupWindowExpires(t *testing.T) {
	cfg := alertTrigger()
	cfg.Dedup.Window = tools30m()
	s, runs, _, clk := newService(t, cfg)
	deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	clk.add(31 * time.Minute)
	res := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	if res.Items[0].Action != ActionCreated || len(runs.reqs) != 2 {
		t.Fatalf("after the window: %+v runs=%d", res.Items[0], len(runs.reqs))
	}
}

func TestStartFailureReleasesClaim(t *testing.T) {
	s, runs, dedup, _ := newService(t, alertTrigger())
	runs.startErr = errors.New("db down")
	res := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	if res.Items[0].Action != ActionError || len(dedup.claims) != 0 {
		t.Fatalf("got %+v claims=%d", res.Items[0], len(dedup.claims))
	}
	runs.startErr = nil
	res = deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical")}})
	if res.Items[0].Action != ActionCreated {
		t.Fatalf("retry: %+v", res.Items[0])
	}
}

func TestRateLimit(t *testing.T) {
	cfg := alertTrigger()
	cfg.RateLimit = RateConfig{PerMinute: 1, Burst: 1}
	s, runs, _, clk := newService(t, cfg)
	a := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a1", "firing", "critical"), alert("a2", "firing", "critical")}})
	if a.Items[0].Action != ActionCreated || a.Items[1].Action != ActionRateLimited {
		t.Fatalf("results %+v", a.Items)
	}
	clk.add(time.Minute)
	b := deliver(t, s, "alerts", map[string]any{"alerts": []any{alert("a2", "firing", "critical")}})
	if b.Items[0].Action != ActionCreated || len(runs.reqs) != 2 {
		t.Fatalf("after refill %+v", b.Items)
	}
}

func TestSignatureAndTenantScope(t *testing.T) {
	cfg := alertTrigger()
	cfg.SecretEnv = "TEST_TRIGGER_SECRET"
	cfg.Tenant = "t1"
	t.Setenv("TEST_TRIGGER_SECRET", "s3cret")
	s, _, _, _ := newService(t, cfg)
	body := []byte(`{"alerts":[]}`)
	ctx := context.Background()
	if _, err := s.Handle(ctx, "t1", "a", "alerts", body, http.Header{}); !errors.Is(err, ErrSignature) {
		t.Fatalf("missing signature: %v", err)
	}
	h := http.Header{}
	h.Set("X-Harness-Signature", Sign("wrong", body))
	if _, err := s.Handle(ctx, "t1", "a", "alerts", body, h); !errors.Is(err, ErrSignature) {
		t.Fatalf("bad signature: %v", err)
	}
	h.Set("X-Harness-Signature", Sign("s3cret", body))
	if _, err := s.Handle(ctx, "t1", "a", "alerts", body, h); err != nil {
		t.Fatalf("valid signature: %v", err)
	}
	if _, err := s.Handle(ctx, "t2", "a", "alerts", body, h); !errors.Is(err, ErrUnknownTrigger) {
		t.Fatalf("other tenant must not see the trigger: %v", err)
	}
	if got := s.List("t2"); len(got) != 0 {
		t.Fatalf("List(t2) = %+v", got)
	}
	if got := s.List("t1"); len(got) != 1 || !got[0].Signed {
		t.Fatalf("List(t1) = %+v", got)
	}
	t.Setenv("TEST_TRIGGER_SECRET", "")
	if _, err := s.Handle(ctx, "t1", "a", "alerts", body, h); !errors.Is(err, ErrMisconfigured) {
		t.Fatalf("unset secret: %v", err)
	}
}

func TestPayloadErrors(t *testing.T) {
	s, _, _, _ := newService(t, alertTrigger())
	ctx := context.Background()
	if _, err := s.Handle(ctx, "t1", "a", "alerts", []byte("{"), nil); !errors.Is(err, ErrPayload) {
		t.Fatalf("invalid JSON: %v", err)
	}
	if _, err := s.Handle(ctx, "t1", "a", "nope", []byte("{}"), nil); !errors.Is(err, ErrUnknownTrigger) {
		t.Fatalf("unknown: %v", err)
	}
	many := make([]any, MaxItems+1)
	for i := range many {
		many[i] = alert(fmt.Sprint(i), "firing", "critical")
	}
	b, _ := json.Marshal(map[string]any{"alerts": many})
	if _, err := s.Handle(ctx, "t1", "a", "alerts", b, nil); !errors.Is(err, ErrPayload) {
		t.Fatalf("fan-out limit: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	bad := []Config{
		{Name: "Bad Name", Goal: "x"},
		{Name: "nogoal"},
		{Name: "expr", Goal: "x", Filter: "item.(("},
		{Name: "sched", Goal: "x", Schedule: &ScheduleConfig{Interval: tools30m()}},                                 // no tenant
		{Name: "sched2", Goal: "x", Tenant: "t", Items: "payload", Schedule: &ScheduleConfig{Interval: tools30m()}}, // items on a schedule
	}
	for _, c := range bad {
		if _, err := New([]Config{c}, newFakeRuns(), newFakeDedup(time.Now), nil); err == nil {
			t.Errorf("config %+v should be rejected", c)
		}
	}
	if _, err := New([]Config{{Name: "a", Goal: "x"}, {Name: "a", Goal: "y"}}, newFakeRuns(), newFakeDedup(time.Now), nil); err == nil {
		t.Error("duplicate names should be rejected")
	}
}

func TestSchedulesFireOncePerBucket(t *testing.T) {
	cfg := Config{Name: "patrol", Tenant: "t1", Goal: "Daily review at ${bucket}",
		Schedule: &ScheduleConfig{Interval: toolsDur(time.Hour)}}
	s, runs, _, clk := newService(t, cfg)
	ctx := context.Background()
	s.fireSchedules(ctx)
	s.fireSchedules(ctx) // a second replica in the same bucket
	if len(runs.reqs) != 1 || runs.reqs[0].InitiatorType != "schedule" {
		t.Fatalf("runs %+v", runs.reqs)
	}
	clk.add(time.Hour)
	s.fireSchedules(ctx)
	if len(runs.reqs) != 2 {
		t.Fatalf("next bucket: %d runs", len(runs.reqs))
	}
	if _, err := s.Handle(ctx, "t1", "a", "patrol", []byte("{}"), nil); !errors.Is(err, ErrUnknownTrigger) {
		t.Fatalf("schedules are not deliverable: %v", err)
	}
}

func TestParseTime(t *testing.T) {
	want := time.Date(2026, 11, 7, 9, 0, 0, 0, time.UTC)
	for _, s := range []string{"2026-11-07T09:00:00Z", fmt.Sprint(want.Unix()), fmt.Sprint(want.UnixMilli())} {
		got, ok := parseTime(s)
		if !ok || !got.Equal(want) {
			t.Errorf("parseTime(%q) = %s %v", s, got, ok)
		}
	}
	for _, s := range []string{"", "0001-01-01T00:00:00Z", "soon"} {
		if _, ok := parseTime(s); ok {
			t.Errorf("parseTime(%q) should fail", s)
		}
	}
}

func TestAPIStatusCodes(t *testing.T) {
	s, _, _, _ := newService(t, alertTrigger())
	mux := http.NewServeMux()
	(&API{Service: s}).Register(mux)
	call := func(name, body string, authed bool) int {
		req := httptest.NewRequest(http.MethodPost, "/v1/triggers/"+name, strings.NewReader(body))
		if authed {
			req = req.WithContext(auth.WithPrincipal(req.Context(), auth.Principal{Tenant: "t1", Subject: "am"}))
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}
	if c := call("alerts", `{"alerts":[]}`, false); c != http.StatusUnauthorized {
		t.Errorf("unauthenticated: %d", c)
	}
	if c := call("nope", `{}`, true); c != http.StatusNotFound {
		t.Errorf("unknown: %d", c)
	}
	if c := call("alerts", `{`, true); c != http.StatusBadRequest {
		t.Errorf("bad payload: %d", c)
	}
	if c := call("alerts", `{"alerts":[{"fingerprint":"x","status":"firing","labels":{"alertname":"A","service":"s","severity":"page"}}]}`, true); c != http.StatusAccepted {
		t.Errorf("accepted: %d", c)
	}
}

func toolsDur(d time.Duration) tools.Duration { return tools.Duration(d) }
func tools30m() tools.Duration                { return toolsDur(30 * time.Minute) }

type recMirror struct {
	got []MirrorEvent
	err error
}

func (m *recMirror) MirrorTriggerEvent(_ context.Context, e MirrorEvent) error {
	m.got = append(m.got, e)
	return m.err
}

// The v2 mirror sees every rendered event with v1's outcome, and a failing
// mirror never changes what v1 does.
func TestMirrorReceivesV1Outcomes(t *testing.T) {
	s, runs, _, _ := newService(t, alertTrigger())
	m := &recMirror{err: errors.New("inbox unavailable")}
	s.Mirror = m
	res := deliver(t, s, "alerts", map[string]any{"alerts": []any{
		alert("fp1", "firing", "critical"), alert("fp1", "firing", "critical"), alert("fp2", "firing", "info"),
	}})
	if len(res.Items) != 3 || res.Items[0].Action != ActionCreated || res.Items[1].Action != ActionDuplicate ||
		res.Items[2].Action != ActionFiltered {
		t.Fatalf("v1 results: %+v", res.Items)
	}
	if len(runs.runs) != 1 {
		t.Fatalf("runs: %d", len(runs.runs))
	}
	// Filtered events are not mirrored (v1 never rendered them).
	if len(m.got) != 2 || m.got[0].V1Action != ActionCreated || m.got[0].V1RunID == "" ||
		m.got[1].V1Action != ActionDuplicate || m.got[0].Fingerprint != m.got[1].Fingerprint ||
		m.got[0].Trigger != "alerts" || m.got[0].TenantID != "t1" || len(m.got[0].Services) != 1 {
		t.Fatalf("mirrored: %+v", m.got)
	}
}
