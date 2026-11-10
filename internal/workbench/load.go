package workbench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/world"
)

// LoadOptions sizes a load run (RFC §24.3 load/perf baseline).
type LoadOptions struct {
	// Works created through the Work API, Events ingested through
	// correlation, Snapshots built over Subjects with FactsPerSubject facts.
	Works           int `json:"works"`
	Events          int `json:"events"`
	Subjects        int `json:"subjects"`
	FactsPerSubject int `json:"facts_per_subject"`
	Snapshots       int `json:"snapshots"`
	// Concurrency is the number of concurrent clients per phase.
	Concurrency int `json:"concurrency"`
	// Replicas is the number of orchestrators claiming the created Works.
	Replicas int `json:"replicas"`
}

// DefaultLoadOptions is a smoke-sized run that finishes in seconds.
func DefaultLoadOptions() LoadOptions {
	return LoadOptions{Works: 200, Events: 1000, Subjects: 50, FactsPerSubject: 8, Snapshots: 200, Concurrency: 8, Replicas: 2}
}

func (o *LoadOptions) normalize() error {
	d := DefaultLoadOptions()
	for _, p := range []struct {
		v   *int
		def int
		max int
	}{{&o.Works, d.Works, 100000}, {&o.Events, d.Events, 1000000}, {&o.Subjects, d.Subjects, 100000},
		{&o.FactsPerSubject, d.FactsPerSubject, 1000}, {&o.Snapshots, d.Snapshots, 100000},
		{&o.Concurrency, d.Concurrency, 256}, {&o.Replicas, d.Replicas, 16}} {
		if *p.v < 0 || *p.v > p.max {
			return fmt.Errorf("load option out of range: %d (max %d)", *p.v, p.max)
		}
		if *p.v == 0 {
			*p.v = p.def
		}
	}
	return nil
}

// Percentiles summarizes one latency series in milliseconds.
type Percentiles struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
	// PerSecond is the phase throughput (operations / wall time).
	PerSecond float64 `json:"per_second"`
	Errors    int     `json:"errors,omitempty"`
}

// LoadReport is the result of a load run.
type LoadReport struct {
	Store   string                 `json:"store"`
	Options LoadOptions            `json:"options"`
	Metrics map[string]Percentiles `json:"metrics"`
	// Violations lists budget breaches (empty when no budget was given).
	Violations []string `json:"violations,omitempty"`
}

// The measured series.
const (
	LoadWorkCreate  = "work_create" // Work API create (idempotent insert + event)
	LoadCorrelation = "correlation" // event ingest: dedup, correlate, create or attach
	LoadWakeLag     = "wake_lag"    // a Work becoming due -> an orchestrator starting it
	LoadSnapshot    = "snapshot"    // world snapshot of one subject and its neighbors
	loadTenant      = "load"
	loadTemplateID  = "load-confirm" // parks Works in WAITING after one pass
)

// LoadBudget caps percentiles per series, e.g. {"work_create": {"p95_ms": 50}}.
// A zero field is not checked.
type LoadBudget map[string]Percentiles

// Check returns the budget breaches of r.
func (b LoadBudget) Check(r LoadReport) []string {
	var out []string
	names := make([]string, 0, len(b))
	for n := range b {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		lim, got := b[n], r.Metrics[n]
		if _, ok := r.Metrics[n]; !ok {
			out = append(out, fmt.Sprintf("%s: not measured", n))
			continue
		}
		for _, c := range []struct {
			name      string
			got, want float64
		}{{"p50", got.P50, lim.P50}, {"p95", got.P95, lim.P95}, {"p99", got.P99, lim.P99}, {"max", got.Max, lim.Max}} {
			if c.want > 0 && c.got > c.want {
				out = append(out, fmt.Sprintf("%s %s %.1fms > budget %.1fms", n, c.name, c.got, c.want))
			}
		}
		if lim.PerSecond > 0 && got.PerSecond < lim.PerSecond {
			out = append(out, fmt.Sprintf("%s throughput %.0f/s < budget %.0f/s", n, got.PerSecond, lim.PerSecond))
		}
		if got.Errors > lim.Errors {
			out = append(out, fmt.Sprintf("%s errors %d > budget %d", n, got.Errors, lim.Errors))
		}
	}
	return out
}

// Load measures the control plane's hot paths against store on the real
// clock: Work creation, event correlation, the claim lag of due Works with
// several orchestrator replicas, and world snapshot builds.
func Load(ctx context.Context, store *cpsql.Store, storeName string, opt LoadOptions) (LoadReport, error) {
	if err := opt.normalize(); err != nil {
		return LoadReport{}, err
	}
	rep := LoadReport{Store: storeName, Options: opt, Metrics: map[string]Percentiles{}}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	tpl := work.Template{ID: loadTemplateID, Version: 1, Kind: work.KindGeneral,
		Stages: []work.Stage{{ID: "confirm", Type: work.StageConfirm}}}
	cat, err := orchestration.NewCatalog([]work.Template{tpl})
	if err != nil {
		return rep, err
	}
	run := cpkit.NewID()[24:] // keeps repeated runs on one database apart
	svc := &orchestration.Service{Store: store, Catalog: cat, Clock: time.Now, Log: quiet}

	// Orchestrators claim Works while they are being created.
	octx, stop := context.WithCancel(ctx)
	var owg sync.WaitGroup
	for i := 0; i < opt.Replicas; i++ {
		o := &orchestration.Orchestrator{Store: store, Clock: time.Now, Owner: fmt.Sprintf("load-%s-%d", run, i),
			Poll: 20 * time.Millisecond, Lease: time.Minute, Log: quiet}
		owg.Add(1)
		go func() { defer owg.Done(); _ = o.Run(octx) }()
	}
	defer func() { stop(); owg.Wait() }()

	var ids []string
	var idsMu sync.Mutex
	rep.Metrics[LoadWorkCreate] = measure(ctx, opt.Works, opt.Concurrency, func(ctx context.Context, i int) error {
		w, _, err := svc.Create(ctx, orchestration.CreateRequest{TenantID: loadTenant, ActorType: "user", ActorID: "load",
			IdempotencyKey: fmt.Sprintf("load-%s-%d", run, i), Kind: work.KindGeneral, TemplateID: loadTemplateID,
			Title: fmt.Sprintf("load %d", i), Goal: work.GoalSpec{Intent: "observe",
				SubjectRefs: []string{fmt.Sprintf("service://load-%s/svc-%d", run, i%max(opt.Subjects, 1))}}})
		if err == nil {
			idsMu.Lock()
			ids = append(ids, w.ID)
			idsMu.Unlock()
		}
		return err
	})

	// Correlation: events over Subjects, so most attach to an open Work.
	router, err := eventing.NewRouter([]eventing.Rule{{Name: "load", Match: eventing.Predicate{EventTypes: []string{"alert.firing"}},
		Key: "subject", Template: loadTemplateID, WorkKind: string(work.KindGeneral)}})
	if err != nil {
		return rep, err
	}
	ing := &orchestration.Ingestor{Store: store, Router: router, Catalog: cat, Clock: time.Now, Log: quiet}
	rep.Metrics[LoadCorrelation] = measure(ctx, opt.Events, opt.Concurrency, func(ctx context.Context, i int) error {
		_, _, err := ing.Ingest(ctx, eventing.Envelope{ID: fmt.Sprintf("load-%s-%d", run, i), TenantID: loadTenant,
			Source: "load", Type: "alert.firing", OccurredAt: time.Now(),
			SubjectRef: fmt.Sprintf("service://load-%s/alert-%d", run, i%max(opt.Subjects, 1))})
		return err
	})

	// Wake lag: created_at -> work.started, once every Work was picked up.
	lags, missing, err := wakeLags(ctx, store, ids, 30*time.Second)
	if err != nil {
		return rep, err
	}
	wl := summarize(lags, 0)
	wl.Errors = missing
	rep.Metrics[LoadWakeLag] = wl

	// Snapshots: a subject with its facts and a relation to a neighbor.
	ws := &world.Service{Store: store, Clock: time.Now}
	subject := func(i int) string { return fmt.Sprintf("service://load-%s/world-%d", run, i) }
	now := time.Now()
	for i := 0; i < opt.Subjects; i++ {
		for f := 0; f < opt.FactsPerSubject; f++ {
			if _, _, err := store.AssertFact(ctx, world.Fact{TenantID: loadTenant, SubjectRef: subject(i),
				Predicate: fmt.Sprintf("p%d", f), Value: fmt.Sprintf("v%d", f), Source: "load",
				TrustTier: world.TrustFirstParty, ValidFrom: now, ObservedAt: now}); err != nil {
				return rep, err
			}
		}
		if i > 0 {
			if _, _, err := store.AssertRelation(ctx, world.Relation{TenantID: loadTenant, FromRef: subject(i), Type: "depends_on",
				ToRef: subject(i - 1), Source: "load", ValidFrom: now, ObservedAt: now}); err != nil {
				return rep, err
			}
		}
	}
	rep.Metrics[LoadSnapshot] = measure(ctx, opt.Snapshots, opt.Concurrency, func(ctx context.Context, i int) error {
		_, err := ws.Snapshot(ctx, world.SnapshotRequest{TenantID: loadTenant, Subjects: []string{subject(i % opt.Subjects)},
			Depth: 1, MaxNodes: 50})
		return err
	})
	return rep, nil
}

// measure runs n operations over c workers and summarizes their latency.
func measure(ctx context.Context, n, c int, op func(context.Context, int) error) Percentiles {
	lat := make([]time.Duration, n)
	var next, errs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < c; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n || ctx.Err() != nil {
					return
				}
				t := time.Now()
				if err := op(ctx, i); err != nil {
					errs.Add(1)
				}
				lat[i] = time.Since(t)
			}
		}()
	}
	wg.Wait()
	p := summarize(lat, time.Since(start))
	p.Errors = int(errs.Load())
	return p
}

func summarize(lat []time.Duration, wall time.Duration) Percentiles {
	if len(lat) == 0 {
		return Percentiles{}
	}
	s := slices.Clone(lat)
	slices.Sort(s)
	ms := func(d time.Duration) float64 { return math.Round(float64(d)/1e4) / 100 }
	q := func(p float64) float64 { return ms(s[min(len(s)-1, int(math.Ceil(p*float64(len(s))))-1)]) }
	out := Percentiles{Count: len(s), P50: q(0.50), P95: q(0.95), P99: q(0.99), Max: ms(s[len(s)-1])}
	if wall > 0 {
		out.PerSecond = math.Round(float64(len(s)) / wall.Seconds())
	}
	return out
}

// wakeLags waits until every Work has a work.started event and returns the
// lag from its creation; Works not started within wait count as missing.
func wakeLags(ctx context.Context, store *cpsql.Store, ids []string, wait time.Duration) ([]time.Duration, int, error) {
	deadline := time.Now().Add(wait)
	lags := map[string]time.Duration{}
	for {
		for _, id := range ids {
			if _, ok := lags[id]; ok {
				continue
			}
			w, err := store.GetWork(ctx, loadTenant, id)
			if err != nil {
				return nil, 0, err
			}
			evs, err := store.ListWorkEvents(ctx, loadTenant, id, 0, 20)
			if err != nil {
				return nil, 0, err
			}
			for _, e := range evs {
				if e.Type == "work.started" {
					lags[id] = max(e.OccurredAt.Sub(w.CreatedAt), 0)
					break
				}
			}
		}
		if len(lags) == len(ids) || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	out := make([]time.Duration, 0, len(lags))
	for _, d := range lags {
		out = append(out, d)
	}
	if len(out) == 0 && len(ids) > 0 {
		return nil, len(ids), errors.New("no Work was started by the orchestrators")
	}
	return out, len(ids) - len(out), nil
}
