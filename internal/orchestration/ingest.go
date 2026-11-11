package orchestration

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"go-sre-harness/internal/continuous"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
)

// Catalog holds the Work templates: the built-ins plus configured ones
// (configured templates override built-ins of the same id).
type Catalog struct {
	mu   sync.RWMutex
	byID map[string]work.Template
}

// NewCatalog validates and indexes templates.
func NewCatalog(extra []work.Template) (*Catalog, error) {
	c := &Catalog{byID: map[string]work.Template{}}
	for _, t := range work.Builtin() {
		c.byID[t.ID] = t
	}
	for _, t := range extra {
		if err := t.Normalize(); err != nil {
			return nil, fmt.Errorf("work template %q: %w", t.ID, err)
		}
		c.byID[t.ID] = t
	}
	return c, nil
}

// Get returns a template by id.
func (c *Catalog) Get(id string) (work.Template, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	t, ok := c.byID[id]
	return t, ok
}

// ForKind returns the template for a Work: the named one, else the one
// named like the kind, else "general".
func (c *Catalog) ForKind(id string, kind work.Kind) (work.Template, error) {
	if id != "" {
		t, ok := c.Get(id)
		if !ok {
			return work.Template{}, fmt.Errorf("%w: unknown template %q", work.ErrInvalid, id)
		}
		return t, nil
	}
	if t, ok := c.Get(string(kind)); ok {
		return t, nil
	}
	t, _ := c.Get("general")
	return t, nil
}

// List returns all templates sorted by id.
func (c *Catalog) List() []work.Template {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]work.Template, 0, len(c.byID))
	for _, t := range c.byID {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Ingestor accepts events into the durable inbox, fires matching watches
// and routes them to Works through correlation rules.
type Ingestor struct {
	Store   *cpsql.Store
	Router  *eventing.Router
	Catalog *Catalog
	// Shadow records routing decisions without creating Works
	// (HARNESS_EVENT_SHADOW_MODE). Watches of existing Works still fire.
	Shadow      bool
	Clock       cpkit.Clock
	Log         *slog.Logger
	Metrics     *Metrics
	MaxAttempts int
	// Sources are the operator-declared event sources: which types each may
	// send and how late an event may open a Work (design §9.3). A source
	// without an entry is bound by the caller's scope only.
	Sources map[string]eventing.SourcePolicy
	// Changes receives events that are changes (deploys, config, topology,
	// manual operations) for the change ledger; nil disables it.
	Changes impact.Store
	// Neighbors returns the resources one dependency hop from a subject
	// (topology-assisted correlation, §9.5); nil disables it.
	Neighbors func(ctx context.Context, tenantID, subject string) ([]string, error)
}

func (in *Ingestor) log() *slog.Logger {
	if in.Log != nil {
		return in.Log
	}
	return slog.Default()
}

// Ingest validates and stores an event, then processes it. A redelivered
// event (same tenant, source and event id) returns the stored record with
// created=false and is not processed again.
func (in *Ingestor) Ingest(ctx context.Context, env eventing.Envelope) (eventing.Record, bool, error) {
	now := in.Clock.Now()
	if err := env.Normalize(now); err != nil {
		return eventing.Record{}, false, err
	}
	rec, created, err := in.Store.InsertEvent(ctx, env, now)
	if err != nil {
		return rec, false, err
	}
	if !created {
		in.Metrics.event("duplicate")
		return rec, false, nil
	}
	// Processing failures leave the record FAILED for the sweep; the
	// event itself is safely stored, so the caller gets 202 either way.
	if err := in.Process(ctx, rec); err != nil {
		in.log().Warn("event processing failed; will retry", "tenant", env.TenantID, "inbox_id", rec.InboxID, "err", err)
	}
	if got, err := in.Store.GetEvent(ctx, env.TenantID, rec.InboxID); err == nil {
		rec = got
	}
	return rec, true, nil
}

// Process routes one inbox record (idempotent: re-processing converges on
// the same Work and the same watch firings).
func (in *Ingestor) Process(ctx context.Context, rec eventing.Record) error {
	maxAttempts := in.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	start := time.Now()
	ctx, sp := obs.Tracer().Start(ctx, "event.correlate", trace.WithAttributes(attribute.String("event.inbox_id", rec.InboxID),
		attribute.String("event.type", rec.Envelope.Type), attribute.String("event.source", rec.Envelope.Source)))
	dec := eventing.Decision{TenantID: rec.Envelope.TenantID, InboxID: rec.InboxID, Basis: "none", Shadow: in.Shadow}
	workID, decision, state, err := in.route(ctx, rec, &dec)
	in.Metrics.correlated(metricDecision(decision), time.Since(start))
	if err == nil {
		dec.Decision, dec.WorkID, dec.At = decision, workID, in.Clock.Now()
		if dec.WorkID == "" && dec.Basis == "deterministic" {
			dec.Basis = "none"
		}
		in.Metrics.decision(dec.Basis)
		if derr := in.Store.RecordDecision(ctx, dec); derr != nil {
			in.log().Warn("record correlation decision", "inbox_id", rec.InboxID, "err", derr)
		}
	}
	if workID != "" {
		sp.SetAttributes(attribute.String("work_id", workID))
	}
	sp.SetAttributes(attribute.String("event.decision", decision))
	sp.End()
	if err != nil {
		in.Metrics.event("failed")
		if merr := in.Store.MarkEvent(ctx, rec.Envelope.TenantID, rec.InboxID, eventing.StateFailed, "", "", err.Error(), maxAttempts); merr != nil {
			return errors.Join(err, merr)
		}
		return err
	}
	in.Metrics.event(metricDecision(decision))
	return in.Store.MarkEvent(ctx, rec.Envelope.TenantID, rec.InboxID, state, workID, decision, "", maxAttempts)
}

func metricDecision(d string) string {
	switch {
	case d == cpsql.CorrCreated, d == cpsql.CorrAttached, d == cpsql.CorrThrottled, d == "watch", d == "no_rule", d == "ignored", d == "expired",
		d == "related", d == "type_not_allowed":
		return d
	case len(d) >= 6 && d[:6] == "shadow":
		return "shadow"
	}
	return "other"
}

func (in *Ingestor) route(ctx context.Context, rec eventing.Record, dec *eventing.Decision) (workID, decision, state string, err error) {
	env := rec.Envelope
	now := in.Clock.Now()
	if env.Expired(now) {
		dec.Reason = "the event expired before it was processed"
		return "", "expired", eventing.StateIgnored, nil
	}
	policy, declared := in.Sources[env.Source]
	if declared && !policy.AllowsType(env.Type) {
		// The event is kept (it arrived, and that is worth knowing) but a
		// source cannot act outside the types the operator gave it.
		dec.Reason = "source " + env.Source + " is not allowed to send " + env.Type
		return "", "type_not_allowed", eventing.StateIgnored, nil
	}
	// A change is a fact about the environment whatever happens to the
	// event's routing: record it for the timeline and for correlation.
	if in.Changes != nil {
		if c, ok := impact.FromEvent(env); ok {
			if _, _, cerr := in.Changes.RecordChange(ctx, c); cerr != nil {
				in.log().Warn("record change", "inbox_id", rec.InboxID, "err", cerr)
			}
		}
	}
	dec.Late = policy.Late(&env, now)
	firedWork, err := in.fireWatches(ctx, rec)
	if err != nil {
		return "", "", "", err
	}
	rule, ok := in.Router.Route(&env)
	if ok {
		dec.RuleName, dec.RuleVersion = rule.Name, rule.Version()
	}
	if !ok || rule.Action == "ignore" {
		if firedWork != "" {
			dec.Basis, dec.Reason = "deterministic", "a watch of the Work matched the event"
			return firedWork, "watch", eventing.StateProcessed, nil
		}
		if ok {
			dec.Reason = "rule " + rule.Name + " ignores the event"
			return "", "ignored", eventing.StateIgnored, nil
		}
		if related := in.relate(ctx, rec, dec); related != "" {
			return related, "related", eventing.StateProcessed, nil
		}
		dec.Reason = "no correlation rule matches"
		return "", "no_rule", eventing.StateIgnored, nil
	}
	group := "rule:" + rule.GroupName() + "|" + rule.GroupKey(&env)
	window := rule.Window.D()
	if window <= 0 {
		window = eventing.DefaultWindow
	}
	dec.GroupKey, dec.Basis = group, "deterministic"
	if in.Shadow {
		dec.Reason = "shadow mode: the decision is recorded, no Work is created"
		return firedWork, "shadow:" + rule.Action + ":" + rule.Name, eventing.StateProcessed, nil
	}
	req := cpsql.CorrelateRequest{TenantID: env.TenantID, GroupKey: group, Rule: rule.Name, Window: window, Now: now,
		EventID: rec.InboxID, Payload: map[string]any{"inbox_id": rec.InboxID, "type": env.Type, "source": env.Source,
			"subject_ref": env.SubjectRef, "rule": rule.Name}}
	if dec.Late {
		// A late event joins the Work that is open for its group; it never
		// opens a new one and never restarts a closed generation.
		dec.Reason = "late event: attach only"
	}
	if rule.Action != "attach" && !dec.Late {
		n, err := in.newWork(rule, rec)
		if err != nil {
			return "", "", "", err
		}
		req.Create = &n
	}
	w, decision, err := in.Store.Correlate(ctx, req)
	if err != nil {
		return "", "", "", err
	}
	if decision == cpsql.CorrNoWork {
		if related := in.relate(ctx, rec, dec); related != "" && firedWork == "" {
			return related, "related", eventing.StateProcessed, nil
		}
		dec.Reason = firstNonEmpty(dec.Reason, "the correlation group has no open Work")
		return firedWork, decision, eventing.StateIgnored, nil
	}
	dec.Candidates = append(dec.Candidates, eventing.Candidate{WorkID: w.ID, Relation: "same_group", Score: 1, Chosen: true,
		Reason: "rule " + rule.Name + " keys the event to this Work"})
	if decision == cpsql.CorrCreated {
		in.log().Info("work created from event", "tenant", env.TenantID, "work_id", w.ID, "rule", rule.Name,
			"inbox_id", rec.InboxID)
		if rule.Lookback > 0 {
			in.attachPrior(ctx, rule, group, window, rec, now)
		}
	}
	return w.ID, decision, eventing.StateProcessed, nil
}

// relate looks for open Works on a dependency of the event's subject
// (topology-assisted correlation, §9.5). A match is a lead: the event is
// noted on the Work's timeline so an investigator sees it, but it does not
// wake the Work, change its state or count as grounds for any action. The
// best candidate's id is returned ("" when there is none).
func (in *Ingestor) relate(ctx context.Context, rec eventing.Record, dec *eventing.Decision) string {
	if in.Neighbors == nil || in.Shadow {
		return ""
	}
	env := rec.Envelope
	neighbors, err := in.Neighbors(ctx, env.TenantID, env.SubjectRef)
	if err != nil || len(neighbors) == 0 {
		return ""
	}
	if len(neighbors) > 16 {
		neighbors = neighbors[:16]
	}
	best := ""
	seen := map[string]bool{}
	for _, n := range neighbors {
		works, err := in.Store.ListWorks(ctx, work.Filter{TenantID: env.TenantID, Subject: n, Kind: work.KindIncident, Limit: 3,
			Status: []work.Status{work.StatusActive, work.StatusWaiting, work.StatusVerifying, work.StatusBlocked}})
		if err != nil {
			continue
		}
		for _, w := range works {
			if seen[w.ID] || len(dec.Candidates) >= 5 {
				continue
			}
			seen[w.ID] = true
			c := eventing.Candidate{WorkID: w.ID, Relation: "dependency", Score: 0.5,
				Reason: env.SubjectRef + " is one hop from " + n + ", a subject of the Work"}
			if err := in.Store.NoteWork(ctx, env.TenantID, w.ID, "work.related_event", "system", "event:"+env.Source, map[string]any{
				"inbox_id": rec.InboxID, "type": env.Type, "subject_ref": env.SubjectRef, "via": n, "basis": "topology",
				"note": "a lead for investigation; not correlated by rule"}); err != nil {
				continue
			}
			if best == "" {
				best, c.Chosen = w.ID, true
			}
			dec.Candidates = append(dec.Candidates, c)
		}
	}
	if best != "" {
		dec.Basis, dec.Reason = "topology", "noted on Works that depend on the subject; investigation lead only"
	}
	return best
}

// attachPrior attaches events of the same correlation group that arrived
// within the rule's lookback, found no open Work then, and were dropped
// (e.g. the deploy that preceded the alert). Best effort: the Work exists
// either way, and a failure is logged, not retried.
func (in *Ingestor) attachPrior(ctx context.Context, rule eventing.Rule, group string, window time.Duration,
	rec eventing.Record, now time.Time) {
	env := rec.Envelope
	prior, err := in.Store.ListEvents(ctx, cpsql.EventFilter{TenantID: env.TenantID, Subject: env.SubjectRef,
		State: eventing.StateIgnored, Since: env.OccurredAt.Add(-rule.Lookback.D()), Until: now, Limit: 200})
	if err != nil {
		in.log().Warn("lookback: list events failed", "rule", rule.Name, "err", err)
		return
	}
	for _, p := range prior {
		if p.InboxID == rec.InboxID || p.Decision != cpsql.CorrNoWork {
			continue
		}
		pe := p.Envelope
		pr, ok := in.Router.Route(&pe)
		if !ok || "rule:"+pr.GroupName()+"|"+pr.GroupKey(&pe) != group {
			continue
		}
		w, decision, err := in.Store.Correlate(ctx, cpsql.CorrelateRequest{TenantID: env.TenantID, GroupKey: group,
			Rule: pr.Name, Window: window, Now: now, EventID: p.InboxID, Payload: map[string]any{"inbox_id": p.InboxID,
				"type": pe.Type, "source": pe.Source, "subject_ref": pe.SubjectRef, "rule": pr.Name, "lookback": true}})
		if err != nil || (decision != cpsql.CorrAttached && decision != cpsql.CorrThrottled) {
			in.log().Warn("lookback: attach failed", "inbox_id", p.InboxID, "decision", decision, "err", err)
			continue
		}
		if err := in.Store.MarkEvent(ctx, env.TenantID, p.InboxID, eventing.StateProcessed, w.ID, decision, "", 1); err != nil {
			in.log().Warn("lookback: mark event failed", "inbox_id", p.InboxID, "err", err)
		}
	}
}

func (in *Ingestor) newWork(rule eventing.Rule, rec eventing.Record) (work.NewWork, error) {
	env := rec.Envelope
	kind := work.Kind(rule.WorkKind)
	if kind == "" {
		kind = work.KindIncident
	}
	tpl, err := in.Catalog.ForKind(rule.Template, kind)
	if err != nil {
		return work.NewWork{}, err
	}
	intent := rule.Intent
	if intent == "" {
		intent = "investigate"
	}
	return work.NewWork{TenantID: env.TenantID, Kind: kind, Title: rule.RenderTitle(&env),
		Goal: work.GoalSpec{Intent: intent, SubjectRefs: []string{env.SubjectRef},
			Description: cpkit.Truncate(fmt.Sprintf("Triggered by %s from %s on %s.", env.Type, env.Source, env.SubjectRef), 500),
			Labels:      map[string]string{"rule": rule.Name}},
		Template: tpl, Priority: rule.Priority, Queue: work.Queue(rule.Queue), ActorType: "system", ActorID: "event:" + env.Source,
		IdempotencyKey: "event:" + rec.InboxID, SourceEvent: rec.InboxID,
		Subjects: []work.SubjectLink{{Kind: "subject", ID: env.SubjectRef}}, Now: in.Clock.Now()}, nil
}

// watchSkew tolerates clock skew between event sources and this process
// when deciding whether an event happened after a watch was created.
const watchSkew = 30 * time.Second

// fireWatches fires every ACTIVE event watch of the tenant that matches;
// the firing id is derived from the inbox id, so re-processing is a no-op.
func (in *Ingestor) fireWatches(ctx context.Context, rec eventing.Record) (string, error) {
	env := rec.Envelope
	ws, err := in.Store.ActiveEventWatches(ctx, env.TenantID, 1000)
	if err != nil {
		return "", err
	}
	first := ""
	for _, w := range ws {
		if w.Predicate == nil || !w.Predicate.Match(&env) || env.OccurredAt.Before(w.CreatedAt.Add(-watchSkew)) {
			continue
		}
		fired, err := in.Store.FireWatch(ctx, continuous.Firing{TenantID: env.TenantID, WatchID: w.ID, WorkID: w.WorkID,
			FiringID: "event:" + rec.InboxID, Kind: "event", FiredAt: in.Clock.Now(),
			Payload: mustJSON(map[string]any{"inbox_id": rec.InboxID, "type": env.Type, "subject_ref": env.SubjectRef})})
		if err != nil {
			return first, err
		}
		if fired {
			in.Metrics.watch("event")
			if first == "" {
				first = w.WorkID
			}
		}
	}
	return first, nil
}

// Sweep re-processes NEW/FAILED records older than age (crash recovery).
func (in *Ingestor) Sweep(ctx context.Context, age time.Duration, limit int) (int, error) {
	recs, err := in.Store.PendingEvents(ctx, in.Clock.Now().Add(-age), limit)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, r := range recs {
		if ctx.Err() != nil {
			break
		}
		if err := in.Process(ctx, r); err == nil {
			n++
		}
	}
	return n, nil
}
