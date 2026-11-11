// Package impact keeps the change ledger and relates changes to symptoms
// (design §9.4, §9.5): what was released, reconfigured, rewired or done by
// hand, on which resources and when — and which of those changes are worth
// looking at for a given symptom.
//
// The output of a correlation is a ranked list of leads. A change that
// precedes a symptom "correlates" or "temporally precedes"; nothing in this
// package ever says "caused by". Promoting a lead to a root cause takes
// independent evidence (a version comparison, a rollback that removes the
// symptom) and is the job of the hypothesis ledger, not of a time window.
package impact

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/observability"
)

// Change categories.
const (
	CategoryRelease  = "release"  // git commit, CI job, Argo CD sync, deployment revision
	CategoryConfig   = "config"   // config map, mesh, DNS, rate limit, model parameters
	CategoryTopology = "topology" // dependencies, pod/node/GPU binding, site in or out
	CategoryManual   = "manual"   // console, script, break-glass, mitigation action
)

// Change is one recorded change.
type Change struct {
	ID       string `json:"change_id"`
	TenantID string `json:"tenant_id"`
	Category string `json:"category"`
	Source   string `json:"source"`
	Actor    string `json:"actor,omitempty"`
	// StartedAt / EndedAt bound the change (a rollout has both).
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	// BeforeRef / AfterRef identify the resource state before and after
	// (revision, commit, artifact ref).
	BeforeRef string `json:"resource_before_ref,omitempty"`
	AfterRef  string `json:"resource_after_ref,omitempty"`
	// Validation is how the change was checked (ci:passed, canary:ok, none).
	Validation       string            `json:"validation,omitempty"`
	AffectedSubjects []string          `json:"affected_subjects"`
	RolloutPhase     string            `json:"rollout_phase,omitempty"`
	Summary          string            `json:"summary,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`
	RecordedAt       time.Time         `json:"recorded_at"`
}

// ErrInvalid marks a refused change.
var ErrInvalid = errors.New("invalid change")

// Normalize validates a change.
func (c *Change) Normalize(now time.Time) error {
	if c.TenantID == "" || c.ID == "" || len(c.ID) > 200 {
		return fmt.Errorf("%w: tenant and change id are required", ErrInvalid)
	}
	switch c.Category {
	case CategoryRelease, CategoryConfig, CategoryTopology, CategoryManual:
	default:
		return fmt.Errorf("%w: unknown category %q", ErrInvalid, c.Category)
	}
	if c.Source == "" || len(c.Source) > 128 {
		return fmt.Errorf("%w: source is required", ErrInvalid)
	}
	if c.StartedAt.IsZero() {
		return fmt.Errorf("%w: started_at is required", ErrInvalid)
	}
	if c.EndedAt != nil && c.EndedAt.Before(c.StartedAt) {
		return fmt.Errorf("%w: ended_at before started_at", ErrInvalid)
	}
	refs, err := cpkit.NormalizeRefs(c.AffectedSubjects)
	if err != nil || len(refs) == 0 || len(refs) > 64 {
		return fmt.Errorf("%w: 1..64 affected subjects are required", ErrInvalid)
	}
	c.AffectedSubjects = refs
	c.Summary = cpkit.Truncate(c.Summary, 500)
	if c.RecordedAt.IsZero() {
		c.RecordedAt = now
	}
	return nil
}

// Store is the change ledger port (internal/store/cpsql).
type Store interface {
	// RecordChange upserts by (tenant, change id): a rollout reports its
	// phases under one id.
	RecordChange(ctx context.Context, c Change) (Change, bool, error)
	// ChangesFor lists changes that touched any of subjects and started in
	// [from, to], newest first.
	ChangesFor(ctx context.Context, tenantID string, subjects []string, from, to time.Time, limit int) ([]Change, error)
}

// FromEvent derives a change from an event envelope, or reports false when
// the event is not a change. Deploy, sync, config and topology events of
// the event sources map onto the four categories.
func FromEvent(e eventing.Envelope) (Change, bool) {
	cat := ""
	switch t := e.Type; {
	case strings.HasPrefix(t, "config."), strings.HasPrefix(t, "change.config"):
		cat = CategoryConfig
	case strings.HasPrefix(t, "topology."), strings.HasPrefix(t, "change.topology"):
		cat = CategoryTopology
	case strings.HasPrefix(t, "manual."), strings.HasPrefix(t, "change.manual"), strings.HasPrefix(t, "action."):
		cat = CategoryManual
	case strings.HasPrefix(t, "deploy."), strings.HasPrefix(t, "release."), strings.HasPrefix(t, "gitops."),
		strings.HasPrefix(t, "change."):
		cat = CategoryRelease
	}
	if cat == "" || e.SubjectRef == "" {
		return Change{}, false
	}
	id := firstNonEmpty(e.Labels["change_id"], e.Labels["revision"], e.Labels["commit"], e.ID)
	c := Change{ID: e.Source + ":" + id, TenantID: e.TenantID, Category: cat, Source: e.Source, Actor: e.Labels["actor"],
		StartedAt: e.OccurredAt, BeforeRef: e.Labels["before"], AfterRef: firstNonEmpty(e.Labels["after"], e.Labels["revision"], e.Labels["commit"]),
		Validation: e.Labels["validation"], AffectedSubjects: []string{e.SubjectRef}, RolloutPhase: e.Labels["phase"],
		Summary: firstNonEmpty(e.Labels["summary"], e.Type), Labels: map[string]string{"event_type": e.Type}}
	return c, true
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// Symptom is what a correlation is asked about.
type Symptom struct {
	Subject   string
	StartedAt time.Time
	// Window is how far back changes are considered (default 2h); After
	// tolerates clock skew and detection delay after the onset (default 5m).
	Window time.Duration
	After  time.Duration
}

// Basis of a lead.
const (
	// BasisDeterministic: the change touched the symptom's own resource.
	BasisDeterministic = "deterministic"
	// BasisTopology: the change touched a dependency. It ranks what to
	// look at first and is never grounds for a write action.
	BasisTopology = "topology"
)

// Lead is a change worth investigating for a symptom.
type Lead struct {
	Change Change `json:"change"`
	// Score in (0,1] orders leads; it is not a probability of causation.
	Score float64 `json:"score"`
	Basis string  `json:"basis"`
	// Relation is "temporally_precedes" or "correlates" — never a cause.
	Relation string `json:"relation"`
	// Distance is the dependency distance from the symptom's subject.
	Distance int           `json:"distance"`
	Lag      time.Duration `json:"lag_ns"`
	Reasons  []string      `json:"reasons"`
	// Checks are what would raise or lower confidence in the lead.
	Checks []string `json:"distinguishing_checks"`
}

// categoryWeight ranks categories by how often they explain a symptom on
// the resource they touch; it only orders leads of equal proximity.
var categoryWeight = map[string]float64{CategoryRelease: 1, CategoryConfig: 0.9, CategoryManual: 0.8, CategoryTopology: 0.6}

// Correlate ranks changes for a symptom. neighbors maps a subject to its
// dependency distance from the symptom's subject (the subject itself is 0);
// subjects not in it are unrelated.
func Correlate(s Symptom, changes []Change, neighbors map[string]int) []Lead {
	window, after := s.Window, s.After
	if window <= 0 {
		window = 2 * time.Hour
	}
	if after <= 0 {
		after = 5 * time.Minute
	}
	var out []Lead
	for _, c := range changes {
		dist := -1
		for _, sub := range c.AffectedSubjects {
			d, ok := neighbors[sub]
			if sub == s.Subject {
				d, ok = 0, true
			}
			if ok && (dist < 0 || d < dist) {
				dist = d
			}
		}
		if dist < 0 {
			continue
		}
		lag := s.StartedAt.Sub(c.StartedAt)
		if lag > window || lag < -after {
			continue
		}
		l := Lead{Change: c, Distance: dist, Lag: lag, Basis: BasisDeterministic, Relation: "temporally_precedes"}
		// Closer in time ranks higher: half the weight every quarter window.
		recency := 1.0
		if lag > 0 {
			recency = 1 / (1 + 4*float64(lag)/float64(window))
		} else {
			// Recorded after the onset: within skew, so it only correlates.
			l.Relation = "correlates"
			recency = 0.5
		}
		proximity := 1.0
		for i := 0; i < dist; i++ {
			proximity /= 2
		}
		l.Score = recency * proximity * categoryWeight[c.Category]
		l.Reasons = append(l.Reasons, fmt.Sprintf("%s change %s before the symptom", c.Category, lag.Round(time.Second)))
		if dist == 0 {
			l.Reasons = append(l.Reasons, "it touched the affected resource")
		} else {
			l.Basis = BasisTopology
			l.Reasons = append(l.Reasons, fmt.Sprintf("it touched a dependency %d hop(s) away", dist))
		}
		if c.Validation == "" || c.Validation == "none" {
			l.Reasons = append(l.Reasons, "it has no recorded validation")
		}
		l.Checks = []string{"compare the symptom between the versions before and after the change",
			"check whether instances still on the previous version show the symptom"}
		if c.Category == CategoryRelease || c.Category == CategoryConfig {
			l.Checks = append(l.Checks, "roll the change back on one instance and observe")
		}
		out = append(out, l)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Change.ID < out[j].Change.ID
	})
	return out
}

// Ledger serves the change ledger to the Query Broker as the "ledger"
// backend, so changes are queried, bounded, audited and cited like any
// other signal.
func Ledger(store Store) observability.Factory {
	return func(src observability.Source, _ observability.Endpoint, _ observability.CredentialBroker) (observability.Provider, error) {
		return &ledger{src: src, store: store}, nil
	}
}

type ledger struct {
	src   observability.Source
	store Store
}

func (l *ledger) ID() string                   { return l.src.ID }
func (l *ledger) Signal() observability.Signal { return l.src.Kind }
func (l *ledger) Probe(context.Context) error  { return nil }

func (l *ledger) Query(ctx context.Context, q observability.Query) (observability.Result, error) {
	changes, err := l.store.ChangesFor(ctx, l.src.TenantID, []string{q.SubjectRef}, q.Start, q.End, q.Limit+1)
	if err != nil {
		return observability.Result{}, err
	}
	res := observability.Result{Status: observability.StatusOK}
	if len(changes) > q.Limit {
		changes, res.Truncated = changes[:q.Limit], true
	}
	res.Count = len(changes)
	if res.Count == 0 {
		return res, nil
	}
	res.Payload, _ = json.Marshal(changes)
	var b strings.Builder
	fmt.Fprintf(&b, "%d change(s) on %s:\n", len(changes), q.SubjectRef)
	for i, c := range changes {
		if c.StartedAt.After(res.SampledAt) {
			res.SampledAt = c.StartedAt
		}
		if i < 8 {
			fmt.Fprintf(&b, "%s %s %s by %s: %s (validation: %s)\n", c.StartedAt.Format(time.RFC3339), c.Category, c.ID,
				firstNonEmpty(c.Actor, "unknown"), c.Summary, firstNonEmpty(c.Validation, "none"))
		}
	}
	// Recency of the last change says nothing about freshness of the
	// ledger: the result is as fresh as the query.
	res.SampledAt = q.End
	res.Summary = b.String()
	return res, nil
}

// Topology returns the resources within depth dependency hops of a subject
// with their distance (the subject itself is 0).
type Topology interface {
	Neighbors(ctx context.Context, tenantID, subject string, depth int) (map[string]int, error)
}

// Leads loads the changes around a symptom from the ledger and ranks them
// over the dependency neighborhood of its subject.
func Leads(ctx context.Context, store Store, topo Topology, tenantID string, s Symptom, now time.Time) ([]Lead, error) {
	subject, err := cpkit.NormalizeRef(s.Subject)
	if err != nil {
		return nil, fmt.Errorf("%w: subject must be a resource ref", ErrInvalid)
	}
	s.Subject = subject
	if s.StartedAt.IsZero() {
		s.StartedAt = now
	}
	if s.Window <= 0 {
		s.Window = 2 * time.Hour
	}
	if s.Window > 7*24*time.Hour {
		return nil, fmt.Errorf("%w: window exceeds 7 days", ErrInvalid)
	}
	neighbors := map[string]int{subject: 0}
	if topo != nil {
		if n, err := topo.Neighbors(ctx, tenantID, subject, 2); err == nil {
			for k, v := range n {
				neighbors[k] = v
			}
		}
		// A topology that cannot be read narrows the search to the subject;
		// it does not fail the question.
	}
	subjects := make([]string, 0, len(neighbors))
	for k := range neighbors {
		subjects = append(subjects, k)
	}
	sort.Strings(subjects)
	if len(subjects) > 100 {
		subjects = subjects[:100]
	}
	changes, err := store.ChangesFor(ctx, tenantID, subjects, s.StartedAt.Add(-s.Window), s.StartedAt.Add(10*time.Minute), 200)
	if err != nil {
		return nil, err
	}
	return Correlate(s, changes, neighbors), nil
}
