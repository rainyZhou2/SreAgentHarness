package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
	"go-sre-harness/internal/kernel"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/tools"
)

func policyGuard(g policy.Gate) policy.Gate { return policy.Guarded(g) }

// strategySelector maps a profile's spec.strategy to a strategy plugin.
type strategySelector struct {
	builders map[string]plugin.StrategyBuilder
	cfg      StrategyConfig
	live     plugin.StrategyRuntime
}

func (s *strategySelector) resolve(name string) (plugin.StrategyBuilder, string, error) {
	if name == "" {
		name = s.cfg.Default
	}
	if alias, ok := s.cfg.Aliases[name]; ok {
		name = alias
	}
	b, ok := s.builders[name]
	if !ok {
		var names []string
		for n := range s.builders {
			names = append(names, n)
		}
		sort.Strings(names)
		return nil, name, fmt.Errorf("strategy %q is not enabled (enabled: %v)", name, names)
	}
	return b, name, nil
}

func (s *strategySelector) build(rt plugin.StrategyRuntime, spec profile.Spec) (cognitive.Strategy, error) {
	if spec.Strategy == "flow" {
		// Flow runs are executed by the kernel flow step. A reason step on a
		// flow profile (and flow child agents) uses the deployment default.
		spec.Strategy = ""
	}
	b, _, err := s.resolve(spec.Strategy)
	if err != nil {
		return nil, err
	}
	return b.Build(spec, rt)
}

func (s *strategySelector) liveFactory() profile.Factory {
	return func(spec profile.Spec) (cognitive.Strategy, error) { return s.build(s.live, spec) }
}

// multiObserver fans kernel events out to metrics and telemetry plugins.
type multiObserver []kernel.Observer

func (m multiObserver) ObserveStep(kind, outcome string, d time.Duration) {
	for _, o := range m {
		o.ObserveStep(kind, outcome, d)
	}
}

func (m multiObserver) ObserveClaim(result string) {
	for _, o := range m {
		o.ObserveClaim(result)
	}
}

func (m multiObserver) ObserveTool(source, risk, outcome string) {
	for _, o := range m {
		o.ObserveTool(source, risk, outcome)
	}
}

// optionalOne builds a single-instance kind that may be left unconfigured
// (the framework default then applies); it returns the zero value then.
func optionalOne[T any](ctx context.Context, k plugin.Kind[T], h *plugin.Host, specs plugin.Specs) (T, error) {
	var zero T
	if len(specs.Enabled()) == 0 {
		return zero, nil
	}
	return plugin.BuildOne(ctx, k, h, specs)
}

func orInt(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func orDur(v, def time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return def
}

type registeredProfile struct {
	profile.Definition
	spec profile.Spec
}

// registerProfiles loads every profile source and registers its versions.
// Versions are immutable: a changed spec under an existing version fails
// startup instead of being silently ignored.
func registerProfiles(ctx context.Context, h *plugin.Host, specs plugin.Specs) ([]registeredProfile, error) {
	sources, err := plugin.BuildAll(ctx, plugin.ProfileSource, h, specs)
	if err != nil {
		return nil, err
	}
	var out []registeredProfile
	seen := map[string]string{}
	for _, src := range sources {
		defs, err := src.Profiles(ctx)
		if err != nil {
			return nil, fmt.Errorf("profile_source: %w", err)
		}
		for _, d := range defs {
			spec, raw, err := d.Validate()
			if err != nil {
				return nil, err
			}
			if _, err := profile.Compile(fmt.Sprintf("%s@%d", d.ID, d.Version), spec); err != nil {
				return nil, fmt.Errorf("%w (%s)", err, d.Origin)
			}
			if d.Tenant == "" {
				d.Tenant = h.Settings.DefaultTenant
			}
			key := fmt.Sprintf("%s/%s@%d", d.Tenant, d.ID, d.Version)
			if prev, dup := seen[key]; dup && prev != string(raw) {
				return nil, fmt.Errorf("profile %s is defined twice with different specs (%s)", key, d.Origin)
			}
			seen[key] = string(raw)
			if spec.Tools == nil {
				h.Log.Warn("profile declares no tools allowlist; runs may use every registered tool", "profile", key)
			}
			for _, t := range spec.Tools {
				if strings.Contains(t, "*") {
					continue
				}
				if _, ok := h.Tools.Lookup(t); !ok {
					h.Log.Warn("profile references a tool no tool plugin provides", "profile", key, "tool", t)
				}
			}
			created, err := h.Store.RegisterProfile(ctx, d.Tenant, d.ID, d.Version, raw)
			if err != nil && d.Seed && errors.Is(err, domain.ErrVersionConflict) {
				h.Log.Warn("seed profile differs from the stored version; keeping the stored version", "profile", key, "origin", d.Origin)
				continue
			}
			if err != nil {
				return nil, err
			}
			if created {
				h.Log.Info("profile registered", "profile", key, "origin", d.Origin)
			}
			d.Spec = raw
			out = append(out, registeredProfile{Definition: d, spec: spec})
		}
	}
	return out, nil
}

// buildEventSinks builds the event_sink plugins and their dispatchers.
func buildEventSinks(ctx context.Context, h *plugin.Host, specs plugin.Specs, m eventsink.Counters) ([]*eventsink.Dispatcher, error) {
	hostname, _ := os.Hostname()
	owner := fmt.Sprintf("%s-%d", hostname, os.Getpid())
	seen := map[string]bool{}
	var out []*eventsink.Dispatcher
	for _, s := range specs.Enabled() {
		id := s.Instance
		if id == "" {
			id = s.Name
		}
		if seen[id] {
			return nil, fmt.Errorf("event_sink: duplicate instance %q (set \"instance\" to run a plugin twice)", id)
		}
		seen[id] = true
		sink, err := plugin.Build(ctx, plugin.EventSink, h, s)
		if err != nil {
			return nil, err
		}
		d := &eventsink.Dispatcher{ID: id, Sink: sink, Store: h.Store, Owner: owner, Log: h.Log.With("event_sink", id), Metrics: m}
		if c, ok := sink.(eventsink.Configurable); ok {
			d.Options = c.SinkOptions()
		}
		if err := d.Options.Validate(); err != nil {
			return nil, fmt.Errorf("event_sink %s: %w", id, err)
		}
		out = append(out, d)
	}
	return out, nil
}

// applyToolOverlays validates operator overlays and installs them. Overlays
// may name tenant tools that are loaded later, so an unknown name is only a
// warning; follow-up tools are resolved when they run.
func applyToolOverlays(reg *tools.Registry, overlays map[string]tools.Overlay, log *slog.Logger) error {
	if len(overlays) == 0 {
		return nil
	}
	names := make([]string, 0, len(overlays))
	for name := range overlays {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		o := overlays[name]
		if err := o.Validate(name); err != nil {
			return err
		}
		if _, ok := reg.Lookup(name); !ok {
			log.Warn("tool overlay names a tool that is not registered globally (tenant tools are resolved per run)", "tool", name)
		}
		if lc := o.Lifecycle; lc != nil {
			for _, f := range []*tools.FollowUp{lc.Verify, lc.Compensate} {
				if f == nil {
					continue
				}
				if _, ok := reg.Lookup(f.Tool); !ok {
					log.Warn("tool overlay follow-up is not registered globally", "tool", name, "follow_up", f.Tool)
				}
			}
		}
	}
	reg.SetOverlays(overlays)
	log.Info("tool overlays", "tools", names)
	return nil
}

// triggerClaimPurger deletes expired trigger deduplication claims.
type triggerClaimPurger interface {
	PurgeTriggerClaims(ctx context.Context, before time.Time, limit int) (int64, error)
}

func purgeTriggerClaims(ctx context.Context, s triggerClaimPurger, log *slog.Logger) error {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			for range 20 {
				n, err := s.PurgeTriggerClaims(ctx, time.Now(), 1000)
				if err != nil {
					log.Warn("purge trigger claims", "err", err)
					break
				}
				if n < 1000 {
					break
				}
			}
		}
	}
}
