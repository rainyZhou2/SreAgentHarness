package capability

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// Options are the operator-owned settings applied to every capability.
type Options struct {
	// Tenant receives collected facts and events.
	Tenant string
	// RiskFloor raises the risk of named actions (operator config). A
	// floor below the declared risk has no effect.
	RiskFloor map[string]domain.RiskClass
	// Authoritative capabilities may assert T1 facts (e.g. the CMDB of
	// record). Others are capped at T2.
	Authoritative map[string]bool
	// Disabled actions are not registered (operator kill switch).
	Disabled map[string]bool
	Clock    cpkit.Clock
	Log      *slog.Logger
}

// Info describes a registered capability (admin listing).
type Info struct {
	ID         string   `json:"id"`
	Version    string   `json:"version"`
	Sources    []string `json:"sources,omitempty"`
	Rules      []string `json:"rules,omitempty"`
	Collectors []string `json:"collectors,omitempty"`
	Tools      []string `json:"tools,omitempty"`
	Actions    []string `json:"actions,omitempty"`
	Metrics    []string `json:"metric_sources,omitempty"`
	Templates  []string `json:"templates,omitempty"`
}

type collectorEntry struct {
	capID, name string
	every       time.Duration
	c           Collector
}

// Set is an assembled group of capabilities. It implements
// plugin.WorkExtension, plugin.WorkRunner, plugin.EventContributor and
// plugin.SourceNormalizers, and exposes its read tools.
type Set struct {
	opts       Options
	infos      []Info
	rules      []eventing.Rule
	sources    map[string]Normalizer
	collectors []collectorEntry
	tools      []tools.Tool
	actions    []action.Executor
	metrics    map[string]outcome.MetricSource
	templates  []work.Template
	names      map[string]string // kind:name -> capability id

	mu   sync.Mutex
	last map[string]map[string]world.Relation // collector -> edge key -> edge
}

var (
	_ plugin.WorkExtension       = (*Set)(nil)
	_ plugin.WorkRunner          = (*Set)(nil)
	_ plugin.EventContributor    = (*Set)(nil)
	_ plugin.SourceNormalizers   = (*Set)(nil)
	_ plugin.TemplateContributor = (*Set)(nil)
)

// NewSet registers the capabilities; any refused registration fails the
// whole set (a half-registered domain is worse than none).
func NewSet(opts Options, caps ...Capability) (*Set, error) {
	if opts.Tenant == "" {
		return nil, fmt.Errorf("%w: tenant is required", ErrInvalid)
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	s := &Set{opts: opts, sources: map[string]Normalizer{}, metrics: map[string]outcome.MetricSource{},
		names: map[string]string{}, last: map[string]map[string]world.Relation{}}
	seen := map[string]bool{}
	for _, c := range caps {
		id := c.ID()
		if err := checkName("capability", id); err != nil {
			return nil, err
		}
		if seen[id] {
			return nil, fmt.Errorf("%w: capability %q registered twice", ErrInvalid, id)
		}
		seen[id] = true
		r := &registrar{s: s, info: Info{ID: id, Version: c.Version()}}
		for _, step := range []struct {
			what string
			fn   func() error
		}{
			{"events", func() error { return c.RegisterEvents(r) }},
			{"world", func() error { return c.RegisterWorld(r) }},
			{"tools", func() error { return c.RegisterTools(r) }},
			{"actions", func() error { return c.RegisterActions(r) }},
			{"outcomes", func() error { return c.RegisterOutcomes(r) }},
		} {
			if err := step.fn(); err != nil {
				return nil, fmt.Errorf("capability %s: register %s: %w", id, step.what, err)
			}
		}
		if tp, ok := c.(TemplateProvider); ok {
			for _, t := range tp.WorkTemplates() {
				if err := t.Normalize(); err != nil {
					return nil, fmt.Errorf("capability %s: template %s: %w", id, t.ID, err)
				}
				if err := r.claim("template", t.ID); err != nil {
					return nil, err
				}
				s.templates = append(s.templates, t)
				r.info.Templates = append(r.info.Templates, t.ID)
			}
		}
		s.infos = append(s.infos, r.info)
	}
	return s, nil
}

// WorkTemplates returns the scenario templates (plugin.TemplateContributor).
func (s *Set) WorkTemplates() []work.Template { return append([]work.Template(nil), s.templates...) }

// Infos lists the registered capabilities.
func (s *Set) Infos() []Info { return append([]Info(nil), s.infos...) }

// Actions implements plugin.WorkExtension.
func (s *Set) Actions() []action.Executor { return append([]action.Executor(nil), s.actions...) }

// MetricSources implements plugin.WorkExtension.
func (s *Set) MetricSources() map[string]outcome.MetricSource {
	out := make(map[string]outcome.MetricSource, len(s.metrics))
	for k, v := range s.metrics {
		out[k] = v
	}
	return out
}

// Tools returns the read tools.
func (s *Set) Tools() []tools.Tool { return append([]tools.Tool(nil), s.tools...) }

// EventRules implements plugin.EventContributor.
func (s *Set) EventRules() []eventing.Rule { return append([]eventing.Rule(nil), s.rules...) }

// Normalizer implements plugin.SourceNormalizers.
func (s *Set) Normalizer(source string) (func(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error), bool) {
	n, ok := s.sources[source]
	if !ok {
		return nil, false
	}
	return n.Normalize, true
}

// Sources implements plugin.SourceNormalizers.
func (s *Set) Sources() []string {
	out := make([]string, 0, len(s.sources))
	for k := range s.sources {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// RunWork runs every collector until ctx ends (plugin.WorkRunner).
func (s *Set) RunWork(ctx context.Context, env plugin.WorkEnv) error {
	var wg sync.WaitGroup
	for _, c := range s.collectors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runCollector(ctx, env, c)
		}()
	}
	wg.Wait()
	return nil
}

func (s *Set) runCollector(ctx context.Context, env plugin.WorkEnv, c collectorEntry) {
	log := s.opts.Log.With("capability", c.capID, "collector", c.name)
	// Jitter the first run so replicas and collectors do not align.
	wait := time.Duration(rand.Int64N(int64(min(c.every, 10*time.Second)) + 1))
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		if err := s.CollectOnce(ctx, env, c.capID, c.name); err != nil && ctx.Err() == nil {
			failures++
			log.Warn("collection failed", "err", err, "failures", failures)
			wait = min(c.every*time.Duration(1<<min(failures, 5)), 15*time.Minute)
			continue
		}
		failures = 0
		wait = c.every
	}
}

// CollectOnce runs one collector and applies its observation (exported for
// tests and on-demand refresh).
func (s *Set) CollectOnce(ctx context.Context, env plugin.WorkEnv, capID, name string) error {
	var entry *collectorEntry
	for i := range s.collectors {
		if s.collectors[i].capID == capID && s.collectors[i].name == name {
			entry = &s.collectors[i]
		}
	}
	if entry == nil {
		return fmt.Errorf("unknown collector %s/%s", capID, name)
	}
	timeout := min(entry.every, 30*time.Second)
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	now := s.opts.Clock.Now()
	obs, err := entry.c.Collect(cctx, now)
	if err != nil {
		return err
	}
	return s.apply(ctx, env, *entry, obs, now)
}

func relKey(r world.Relation) string { return r.FromRef + "\x00" + r.Type + "\x00" + r.ToRef }

func (s *Set) apply(ctx context.Context, env plugin.WorkEnv, c collectorEntry, o Observation, now time.Time) error {
	source := c.capID
	maxTrust := world.TrustFirstParty
	if s.opts.Authoritative[c.capID] {
		maxTrust = world.TrustAuthoritative
	}
	var errs []string
	for _, f := range o.Facts {
		f.TenantID = s.opts.Tenant
		if f.Source == "" || !strings.HasPrefix(f.Source, source) {
			f.Source = source
		}
		f.TrustTier = capTrust(f.TrustTier, maxTrust)
		if f.ObservedAt.IsZero() {
			f.ObservedAt = now
		}
		if env.AssertFact != nil {
			if err := env.AssertFact(ctx, f); err != nil {
				errs = append(errs, fmt.Sprintf("fact %s %s: %v", f.SubjectRef, f.Predicate, err))
			}
		}
	}
	seen := map[string]world.Relation{}
	for _, r := range o.Relations {
		r.TenantID, r.Source = s.opts.Tenant, source
		if r.ObservedAt.IsZero() {
			r.ObservedAt = now
		}
		if r.ValidFrom.IsZero() {
			r.ValidFrom = now
		}
		seen[relKey(r)] = r
		if env.AssertRelation != nil {
			if err := env.AssertRelation(ctx, r); err != nil {
				errs = append(errs, fmt.Sprintf("relation %s %s %s: %v", r.FromRef, r.Type, r.ToRef, err))
			}
		}
	}
	key := c.capID + "/" + c.name
	s.mu.Lock()
	prev := s.last[key]
	s.last[key] = seen
	s.mu.Unlock()
	if env.RetractRelation != nil {
		for k, r := range prev {
			if _, still := seen[k]; still {
				continue
			}
			if err := env.RetractRelation(ctx, s.opts.Tenant, r.FromRef, r.Type, r.ToRef, source, now); err != nil {
				errs = append(errs, fmt.Sprintf("retract %s %s %s: %v", r.FromRef, r.Type, r.ToRef, err))
			}
		}
	}
	for _, e := range o.Events {
		e.TenantID = s.opts.Tenant
		if e.Source == "" {
			e.Source = source
		}
		if env.Ingest != nil {
			if err := env.Ingest(ctx, e); err != nil {
				errs = append(errs, fmt.Sprintf("event %s: %v", e.ID, err))
			}
		}
	}
	if len(errs) > 0 {
		if len(errs) > 5 {
			errs = append(errs[:5], fmt.Sprintf("and %d more", len(errs)-5))
		}
		return fmt.Errorf("apply observation: %s", strings.Join(errs, "; "))
	}
	return nil
}

// capTrust keeps a fact's trust no higher than max (T1 > T2 > T3 > T4).
func capTrust(t, max string) string {
	switch t {
	case world.TrustAuthoritative, world.TrustFirstParty, world.TrustTelemetry, world.TrustUntrusted:
	default:
		return world.TrustUntrusted
	}
	if t < max { // "T1" < "T2": t claims more trust than allowed
		return max
	}
	return t
}

// registrar implements the registries for one capability.
type registrar struct {
	s    *Set
	info Info
}

func (r *registrar) claim(kind, name string) error {
	k := kind + ":" + name
	if other, dup := r.s.names[k]; dup {
		return fmt.Errorf("%w: %s %q is already registered by %s", ErrInvalid, kind, name, other)
	}
	r.s.names[k] = r.info.ID
	return nil
}

func (r *registrar) AddSource(source string, n Normalizer) error {
	if err := checkName("source", source); err != nil {
		return err
	}
	if n == nil {
		return fmt.Errorf("%w: nil normalizer for %s", ErrInvalid, source)
	}
	if err := r.claim("source", source); err != nil {
		return err
	}
	r.s.sources[source] = n
	r.info.Sources = append(r.info.Sources, source)
	return nil
}

func (r *registrar) AddRule(rule eventing.Rule) error {
	if err := rule.Normalize(); err != nil {
		return err
	}
	if err := r.claim("rule", rule.Name); err != nil {
		return err
	}
	r.s.rules = append(r.s.rules, rule)
	r.info.Rules = append(r.info.Rules, rule.Name)
	return nil
}

func (r *registrar) AddCollector(name string, every time.Duration, c Collector) error {
	if err := checkName("collector", name); err != nil {
		return err
	}
	if c == nil {
		return fmt.Errorf("%w: nil collector %s", ErrInvalid, name)
	}
	if every < 5*time.Second {
		return fmt.Errorf("%w: collector %s interval %s is below 5s", ErrInvalid, name, every)
	}
	if err := r.claim("collector", r.info.ID+"/"+name); err != nil {
		return err
	}
	r.s.collectors = append(r.s.collectors, collectorEntry{capID: r.info.ID, name: name, every: every, c: c})
	r.info.Collectors = append(r.info.Collectors, name)
	return nil
}

func (r *registrar) AddTool(t tools.Tool) error {
	d := t.Descriptor()
	if err := checkName("tool", d.Name); err != nil {
		return err
	}
	if d.Risk != domain.RiskR0 {
		return fmt.Errorf("%w: domain tool %s must be read-only (R0, got %s); changes are actions", ErrInvalid, d.Name, d.Risk)
	}
	if err := r.claim("tool", d.Name); err != nil {
		return err
	}
	r.s.tools = append(r.s.tools, t)
	r.info.Tools = append(r.info.Tools, d.Name)
	return nil
}

func (r *registrar) AddAction(e action.Executor) error {
	d := e.Descriptor()
	if err := checkName("action", d.Name); err != nil {
		return err
	}
	if r.s.opts.Disabled[d.Name] {
		r.s.opts.Log.Info("action disabled by operator", "capability", r.info.ID, "action", d.Name)
		return nil
	}
	if riskRank(d.Risk) < 0 {
		return fmt.Errorf("%w: action %s has unknown risk %q", ErrInvalid, d.Name, d.Risk)
	}
	risk := MaxRisk(d.Risk, r.s.opts.RiskFloor[d.Name])
	if risk == domain.RiskR3 {
		return fmt.Errorf("%w: action %s is R3; R3 operations are never automated", ErrInvalid, d.Name)
	}
	switch d.IdempotencyMode {
	case action.IdemNative, action.IdemQueryBeforeRetry, action.IdemNone:
	default:
		return fmt.Errorf("%w: action %s has idempotency mode %q", ErrInvalid, d.Name, d.IdempotencyMode)
	}
	if len(d.ResourceKinds) == 0 {
		return fmt.Errorf("%w: action %s declares no resource kinds", ErrInvalid, d.Name)
	}
	if err := r.claim("action", d.Name); err != nil {
		return err
	}
	if risk != d.Risk {
		e = riskExecutor{Executor: e, risk: risk}
	}
	r.s.actions = append(r.s.actions, e)
	r.info.Actions = append(r.info.Actions, d.Name)
	return nil
}

func (r *registrar) AddMetricSource(name string, src outcome.MetricSource) error {
	if err := checkName("metric source", name); err != nil {
		return err
	}
	if src == nil {
		return fmt.Errorf("%w: nil metric source %s", ErrInvalid, name)
	}
	if err := r.claim("metric", name); err != nil {
		return err
	}
	r.s.metrics[name] = src
	r.info.Metrics = append(r.info.Metrics, name)
	return nil
}
