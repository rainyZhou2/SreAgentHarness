// Package srework registers "sre-sim", a work_extension that simulates a
// small production estate for demos and end-to-end tests of the v2 control
// plane without real infrastructure:
//
//   - deployments with an image and a generation, published as world facts
//     (source "sim-k8s", trust T2) and refreshed periodically;
//   - a "sim" metric source: error_rate is high while a deployment runs its
//     bad image and low otherwise; latency_p99 follows;
//   - actions deployment.rollback (R2, needs approval) and traffic.shift
//     (R1) with native idempotency (the effect key dedups), a pinned target
//     generation (re-checked at dispatch) and read-only inspection;
//   - optionally an alert.firing event for every deployment on a bad image
//     (alert_every), which correlation rules turn into incident Works.
//
// Config (plugins.work_extension / local work_extensions):
//
//	{"name": "sre-sim", "config": {"tenant": "local", "alert_every": "30s",
//	  "deployments": [{"ref": "deployment://prod/checkout", "image": "checkout:v2",
//	                   "good_image": "checkout:v1", "bad_images": ["checkout:v2"]}]}}
package srework

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/world"
)

// DeploymentConfig is one simulated deployment.
type DeploymentConfig struct {
	Ref       string   `json:"ref"`
	Service   string   `json:"service,omitempty"` // service://... (default derived from the ref)
	Image     string   `json:"image"`
	GoodImage string   `json:"good_image,omitempty"`
	BadImages []string `json:"bad_images,omitempty"`
	Replicas  int      `json:"replicas,omitempty"`
}

// Config configures the simulator.
type Config struct {
	Tenant      string             `json:"tenant"`
	Deployments []DeploymentConfig `json:"deployments"`
	// RefreshEvery re-publishes facts (default 30s); AlertEvery emits
	// alert.firing for unhealthy deployments (0 = never).
	RefreshEvery plugin.Duration `json:"refresh_every,omitempty"`
	AlertEvery   plugin.Duration `json:"alert_every,omitempty"`
}

func init() {
	plugin.Register(plugin.WorkExt, "sre-sim", "simulated deployments, metrics and rollback/traffic actions for the v2 demo",
		func(ctx context.Context, h *plugin.Host, c plugin.Config) (plugin.WorkExtension, error) {
			var cfg Config
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return New(cfg)
		})
}

type deployment struct {
	DeploymentConfig
	generation int
	traffic    int // percent of traffic on this deployment
	changedAt  time.Time
	// prevBad and prevTraffic describe the state before changedAt.
	prevBad     bool
	prevTraffic int
}

// Sim is the simulated estate.
type Sim struct {
	cfg   Config
	clock cpkit.Clock
	mu    sync.Mutex
	deps  map[string]*deployment
	// applied records effect keys (native idempotency).
	applied map[string]json.RawMessage
	// dirty deployments are re-published immediately.
	notify chan struct{}
	world  func(ctx context.Context, f world.Fact) error
}

// New validates the config.
func New(cfg Config) (*Sim, error) {
	if cfg.Tenant == "" {
		cfg.Tenant = "local"
	}
	s := &Sim{cfg: cfg, deps: map[string]*deployment{}, applied: map[string]json.RawMessage{}, notify: make(chan struct{}, 1)}
	for _, d := range cfg.Deployments {
		ref, err := cpkit.NormalizeRef(d.Ref)
		if err != nil || cpkit.RefScheme(ref) != "deployment" {
			return nil, fmt.Errorf("sre-sim: deployment ref %q must be deployment://...", d.Ref)
		}
		d.Ref = ref
		if d.Image == "" {
			return nil, fmt.Errorf("sre-sim: deployment %s needs an image", ref)
		}
		if d.Service == "" {
			d.Service = "service://" + strings.TrimPrefix(ref, "deployment://")
		}
		if d.Replicas <= 0 {
			d.Replicas = 3
		}
		s.deps[ref] = &deployment{DeploymentConfig: d, generation: 1, traffic: 100}
	}
	return s, nil
}

// SetClock replaces the clock (tests).
func (s *Sim) SetClock(c cpkit.Clock) { s.clock = c }

func (s *Sim) healthy(d *deployment) bool { return !slices.Contains(d.BadImages, d.Image) }

// Actions implements plugin.WorkExtension.
func (s *Sim) Actions() []action.Executor {
	return []action.Executor{&rollback{s}, &shift{s}}
}

// MetricSources implements plugin.WorkExtension.
func (s *Sim) MetricSources() map[string]outcome.MetricSource {
	return map[string]outcome.MetricSource{"sim": metrics{s}}
}

// RunWork publishes facts and alerts until ctx ends.
func (s *Sim) RunWork(ctx context.Context, env plugin.WorkEnv) error {
	s.mu.Lock()
	s.world = env.AssertFact
	s.mu.Unlock()
	refresh := s.cfg.RefreshEvery.D()
	if refresh <= 0 {
		refresh = 30 * time.Second
	}
	t := time.NewTicker(refresh)
	defer t.Stop()
	var alerts <-chan time.Time
	if d := s.cfg.AlertEvery.D(); d > 0 {
		at := time.NewTicker(d)
		defer at.Stop()
		alerts = at.C
	}
	s.publish(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			s.publish(ctx)
		case <-s.notify:
			s.publish(ctx)
		case <-alerts:
			s.alert(ctx, env.Ingest)
		}
	}
}

// Publish writes the current facts of every deployment.
func (s *Sim) publish(ctx context.Context) {
	s.mu.Lock()
	assert := s.world
	var facts []world.Fact
	now := s.clock.Now()
	for _, d := range s.deps {
		for p, v := range map[string]string{"image": d.Image, "generation": strconv.Itoa(d.generation),
			"replicas": strconv.Itoa(d.Replicas), "traffic_percent": strconv.Itoa(d.traffic)} {
			facts = append(facts, world.Fact{TenantID: s.cfg.Tenant, SubjectRef: d.Ref, Predicate: p, Value: v,
				Source: "sim-k8s", TrustTier: world.TrustFirstParty, ObservedAt: now})
		}
		facts = append(facts, world.Fact{TenantID: s.cfg.Tenant, SubjectRef: d.Service, Predicate: "deployment", Value: d.Ref,
			Source: "sim-k8s", TrustTier: world.TrustFirstParty, ObservedAt: now})
	}
	s.mu.Unlock()
	if assert == nil {
		return
	}
	for _, f := range facts {
		_ = assert(ctx, f)
	}
}

func (s *Sim) alert(ctx context.Context, ingest func(context.Context, eventing.Envelope) error) {
	if ingest == nil {
		return
	}
	s.mu.Lock()
	var evs []eventing.Envelope
	now := s.clock.Now()
	for _, d := range s.deps {
		if s.healthy(d) {
			continue
		}
		// One alert episode per (deployment, image, generation): the
		// fingerprint is the dedup key, the event id changes per firing.
		fp := fmt.Sprintf("HighErrorRate|%s|%d", d.Ref, d.generation)
		evs = append(evs, eventing.Envelope{ID: fmt.Sprintf("%s|%d", fp, now.Unix()), TenantID: s.cfg.Tenant,
			Source: "sim-alertmanager", Type: "alert.firing", SubjectRef: d.Ref, DedupKey: fp, OccurredAt: now,
			Labels:  map[string]string{"alertname": "HighErrorRate", "severity": "critical", "image": d.Image},
			Payload: json.RawMessage(fmt.Sprintf(`{"summary":"error rate above 5%% on %s (image %s)"}`, d.Ref, d.Image))})
	}
	s.mu.Unlock()
	for _, e := range evs {
		_ = ingest(ctx, e)
	}
}

func (s *Sim) changed() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// ---- metrics ----

type metrics struct{ s *Sim }

// QueryRange serves "error_rate", "latency_p99" and "traffic_percent" for a
// deployment:// or service:// target. Samples are synthesized every 15s.
func (m metrics) QueryRange(ctx context.Context, q outcome.MetricQuery) (outcome.MetricResult, error) {
	m.s.mu.Lock()
	defer m.s.mu.Unlock()
	var d *deployment
	for _, x := range m.s.deps {
		if x.Ref == q.TargetRef || x.Service == q.TargetRef {
			d = x
		}
	}
	if d == nil {
		return outcome.MetricResult{}, fmt.Errorf("unknown target %q", q.TargetRef)
	}
	var res outcome.MetricResult
	for t := q.Start.Truncate(15 * time.Second); !t.After(q.End); t = t.Add(15 * time.Second) {
		if t.Before(q.Start) {
			continue
		}
		// Samples before the last change reflect the previous state.
		bad, share := !m.s.healthy(d), float64(d.traffic)/100
		if !d.changedAt.IsZero() && t.Before(d.changedAt) {
			bad, share = d.prevBad, float64(d.prevTraffic)/100
		}
		var v float64
		switch q.Query {
		case "error_rate":
			v = 0.002
			if bad {
				v = 0.12*share + 0.002*(1-share)
			}
		case "latency_p99":
			v = 0.18
			if bad {
				v = 1.4*share + 0.18*(1-share)
			}
		case "traffic_percent":
			v = float64(d.traffic)
		default:
			return outcome.MetricResult{}, fmt.Errorf("unknown query %q", q.Query)
		}
		res.Samples = append(res.Samples, outcome.Sample{T: t, V: math.Round(v*1e6) / 1e6})
	}
	res.EvidenceRef = fmt.Sprintf("sim://metrics/%s?%s", q.Query, strings.TrimPrefix(d.Ref, "deployment://"))
	return res, nil
}

// ---- actions ----

func parseArgs(p action.Plan, v any) error {
	if err := json.Unmarshal(p.Args, v); err != nil {
		return fmt.Errorf("%w: args: %v", action.ErrNotApplied, err)
	}
	return nil
}

func (s *Sim) dep(ref string) (*deployment, error) {
	d, ok := s.deps[ref]
	if !ok {
		return nil, fmt.Errorf("%w: unknown deployment %s", action.ErrNotApplied, ref)
	}
	return d, nil
}

type rollback struct{ s *Sim }

func (r *rollback) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "deployment.rollback", Version: "1", ResourceKinds: []string{"deployment"},
		Risk: domain.RiskR2, Description: "Roll a deployment back to a known-good image.",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{"to_image":{"type":"string"}}}`),
		IdempotencyMode: action.IdemNative, SupportsRollback: true, RequiredFacts: []string{"image"},
		VersionFact: "generation", BlastRadius: "single_deployment", Timeout: time.Minute}
}

func (r *rollback) target(p action.Plan, d *deployment) (string, error) {
	var a struct {
		ToImage string `json:"to_image"`
	}
	if err := parseArgs(p, &a); err != nil {
		return "", err
	}
	if a.ToImage == "" {
		a.ToImage = d.GoodImage
	}
	if a.ToImage == "" {
		return "", fmt.Errorf("%w: to_image is required (no good_image configured)", action.ErrNotApplied)
	}
	return a.ToImage, nil
}

func (r *rollback) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	d, err := r.s.dep(p.TargetRef)
	if err != nil {
		return action.PreflightResult{Allowed: false, Reasons: []string{err.Error()}}, nil
	}
	to, err := r.target(p, d)
	if err != nil {
		return action.PreflightResult{Allowed: false, Reasons: []string{err.Error()}}, nil
	}
	if to == d.Image {
		return action.PreflightResult{Allowed: false, Reasons: []string{"deployment already runs " + to}}, nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{"sim://k8s/" + strings.TrimPrefix(d.Ref, "deployment://")}}, nil
}

func (r *rollback) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	if res, ok := r.s.applied[key]; ok {
		return res, nil // native idempotency: same key, same effect
	}
	d, err := r.s.dep(p.TargetRef)
	if err != nil {
		return nil, err
	}
	if p.TargetVersion != "" && p.TargetVersion != strconv.Itoa(d.generation) {
		return nil, fmt.Errorf("%w: deployment changed (generation %d, plan pinned %s)", action.ErrNotApplied, d.generation, p.TargetVersion)
	}
	to, err := r.target(p, d)
	if err != nil {
		return nil, err
	}
	from := d.Image
	d.prevBad, d.prevTraffic = !r.s.healthy(d), d.traffic
	d.Image, d.generation, d.changedAt = to, d.generation+1, r.s.clock.Now()
	res, _ := json.Marshal(map[string]any{"from_image": from, "to_image": to, "generation": d.generation})
	r.s.applied[key] = res
	r.s.changed()
	return res, nil
}

func (r *rollback) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	r.s.mu.Lock()
	defer r.s.mu.Unlock()
	_, ok := r.s.applied[p.EffectKey]
	return action.InspectResult{Applied: &ok, Detail: json.RawMessage(`{"method":"effect_key_lookup"}`)}, nil
}

type shift struct{ s *Sim }

func (x *shift) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "traffic.shift", Version: "1", ResourceKinds: []string{"deployment"}, Risk: domain.RiskR1,
		Description:     "Set the share of traffic a deployment receives (0-100).",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{"percent":{"type":"integer","minimum":0,"maximum":100}},"required":["percent"]}`),
		IdempotencyMode: action.IdemNative, SupportsRollback: true, VersionFact: "generation", BlastRadius: "single_deployment",
		Timeout: time.Minute}
}

func (x *shift) percent(p action.Plan) (int, error) {
	var a struct {
		Percent *int `json:"percent"`
	}
	if err := parseArgs(p, &a); err != nil {
		return 0, err
	}
	if a.Percent == nil || *a.Percent < 0 || *a.Percent > 100 {
		return 0, fmt.Errorf("%w: percent must be 0-100", action.ErrNotApplied)
	}
	return *a.Percent, nil
}

func (x *shift) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	x.s.mu.Lock()
	defer x.s.mu.Unlock()
	if _, err := x.s.dep(p.TargetRef); err != nil {
		return action.PreflightResult{Reasons: []string{err.Error()}}, nil
	}
	if _, err := x.percent(p); err != nil {
		return action.PreflightResult{Reasons: []string{err.Error()}}, nil
	}
	return action.PreflightResult{Allowed: true}, nil
}

func (x *shift) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	x.s.mu.Lock()
	defer x.s.mu.Unlock()
	if res, ok := x.s.applied[key]; ok {
		return res, nil
	}
	d, err := x.s.dep(p.TargetRef)
	if err != nil {
		return nil, err
	}
	if p.TargetVersion != "" && p.TargetVersion != strconv.Itoa(d.generation) {
		return nil, fmt.Errorf("%w: deployment changed since the plan", action.ErrNotApplied)
	}
	pct, err := x.percent(p)
	if err != nil {
		return nil, err
	}
	d.prevBad, d.prevTraffic = !x.s.healthy(d), d.traffic
	d.traffic, d.generation, d.changedAt = pct, d.generation+1, x.s.clock.Now()
	res, _ := json.Marshal(map[string]any{"percent": pct, "generation": d.generation})
	x.s.applied[key] = res
	x.s.changed()
	return res, nil
}

func (x *shift) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	x.s.mu.Lock()
	defer x.s.mu.Unlock()
	_, ok := x.s.applied[p.EffectKey]
	return action.InspectResult{Applied: &ok}, nil
}
