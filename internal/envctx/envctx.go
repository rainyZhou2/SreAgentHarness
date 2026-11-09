// Package envctx gathers environment facts (topology, ownership, on-call,
// recent changes, ...) from pluggable context providers. The framework knows
// no specific CMDB, deployment system or tracing backend: providers are
// configured data sources, and their facts are rendered as untrusted data.
package envctx

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

// Query describes the run the facts are for.
type Query struct {
	TenantID string
	RunID    string
	Goal     string
	Services []string
	Incident string
	Labels   map[string]string
	// Start/End is the run's time window (zero when unknown).
	Start, End time.Time
	Now        time.Time
}

// Provider returns facts for a query. Providers must be read-only and
// respect ctx cancellation.
type Provider interface {
	Name() string
	Facts(ctx context.Context, q Query) ([]domain.ContextFact, error)
}

// Limits bound what a gatherer contributes to a prompt.
const (
	DefaultMaxFacts   = 30
	DefaultMaxSummary = 600
	DefaultTimeout    = 5 * time.Second
)

// Gatherer runs providers concurrently and normalizes their facts.
type Gatherer struct {
	Providers  []Provider
	Timeout    time.Duration // per provider (default 5s)
	MaxFacts   int           // default 30
	MaxSummary int           // runes per summary (default 600)
	Log        *slog.Logger
	// OnFacts, when set, receives the normalized facts of every gather
	// (e.g. to keep them as World history). It must not block for long.
	OnFacts func(ctx context.Context, q Query, facts []domain.ContextFact)
}

// Gather returns at most MaxFacts facts. Provider errors are logged and do
// not fail the run: environment facts are hints, not prerequisites.
func (g *Gatherer) Gather(ctx context.Context, q Query) []domain.ContextFact {
	if g == nil || len(g.Providers) == 0 {
		return nil
	}
	timeout := g.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	maxFacts := g.MaxFacts
	if maxFacts <= 0 {
		maxFacts = DefaultMaxFacts
	}
	maxSummary := g.MaxSummary
	if maxSummary <= 0 {
		maxSummary = DefaultMaxSummary
	}
	log := g.Log
	if log == nil {
		log = slog.Default()
	}
	results := make([][]domain.ContextFact, len(g.Providers))
	var wg sync.WaitGroup
	for i, p := range g.Providers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("context provider panicked", "provider", p.Name(), "panic", fmt.Sprint(r))
				}
			}()
			pctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			facts, err := p.Facts(pctx, q)
			if err != nil {
				log.Warn("context provider", "provider", p.Name(), "run_id", q.RunID, "err", err)
				return
			}
			for j := range facts {
				if facts[j].Provider == "" {
					facts[j].Provider = p.Name()
				}
			}
			results[i] = facts
		}()
	}
	wg.Wait()
	var out []domain.ContextFact
	seen := map[string]bool{}
	for _, facts := range results {
		for _, f := range facts {
			f, ok := normalize(f, maxSummary)
			if !ok {
				continue
			}
			key := f.Provider + "|" + f.Kind + "|" + f.Subject + "|" + f.Summary
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, f)
		}
	}
	// Deterministic order: newest timed facts first, then by provider/kind.
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if (a.At != nil) != (b.At != nil) {
			return a.At != nil
		}
		if a.At != nil && !a.At.Equal(*b.At) {
			return a.At.After(*b.At)
		}
		if a.Provider != b.Provider {
			return a.Provider < b.Provider
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.Subject < b.Subject
	})
	if len(out) > maxFacts {
		out = out[:maxFacts]
	}
	if g.OnFacts != nil && len(out) > 0 {
		g.OnFacts(ctx, q, out)
	}
	return out
}

func normalize(f domain.ContextFact, maxSummary int) (domain.ContextFact, bool) {
	f.Summary = strings.TrimSpace(f.Summary)
	if f.Summary == "" {
		return f, false
	}
	if poisoned, _ := memory.PoisonCheck(f.Summary); poisoned {
		// Facts that read like instructions to the model are dropped.
		return f, false
	}
	f.Summary, _ = memory.Redact(f.Summary)
	f.Summary = clip(f.Summary, maxSummary)
	f.Kind = clip(strings.TrimSpace(f.Kind), 40)
	if f.Kind == "" {
		f.Kind = "note"
	}
	f.Subject = clip(strings.TrimSpace(f.Subject), 120)
	f.Provider = clip(f.Provider, 60)
	f.Ref = clip(f.Ref, 300)
	if f.Trust == "" {
		f.Trust = "T3"
	}
	if f.At != nil {
		t := f.At.UTC()
		f.At = &t
	}
	return f, true
}

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// Services resolves the services a run is about: the pinned goal context
// first, then "service=<name>" tokens in the goal.
func Services(gc *domain.GoalContext, goal string) []string {
	var out []string
	if gc != nil {
		out = append(out, gc.Services...)
	}
	if s := memory.ServiceFromGoal(goal); s != "" {
		out = append(out, s)
	}
	return domain.NormalizeServices(out)
}
