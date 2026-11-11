// Package provenance judges where a fact comes from before anything
// irreversible relies on it (design §10.3, §10.4): which source is the
// authority for a predicate, whether sources disagree without one, and
// whether the collector behind a source is still reporting.
//
// It never repairs data. A conflict stays a conflict (two strings are not
// averaged), a stale fact stays visible as stale, and a collector outage
// never deletes what was known — it only stops those facts from satisfying
// the precondition of a production action. "The resource is gone" and "we
// could not look" are different answers.
package provenance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/world"
)

// CollectorHealth is the reporting state of one world collector.
type CollectorHealth struct {
	TenantID      string     `json:"tenant_id"`
	Collector     string     `json:"collector"` // "<capability>/<name>"
	LastSuccessAt *time.Time `json:"last_success_at,omitempty"`
	LastAttemptAt *time.Time `json:"last_attempt_at,omitempty"`
	LastFullSync  *time.Time `json:"last_full_sync,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	// ErrorCount counts consecutive failed collections.
	ErrorCount int64 `json:"error_count"`
	// MaxAge is how old the last success may be (three collection intervals
	// by default); beyond it the collector's facts are stale.
	MaxAge        time.Duration `json:"max_age_ns"`
	WatchCursor   string        `json:"watch_cursor,omitempty"`
	SourceVersion string        `json:"source_version,omitempty"`
}

// Collector states.
const (
	Healthy = "healthy"
	Failing = "failing" // the last attempts failed; the last success is recent
	Stale   = "stale"   // no success within MaxAge
	Never   = "never"   // never succeeded
)

// FailingAfter is the number of consecutive failures after which a
// collector is failing even though its last success is recent.
const FailingAfter = 3

// State classifies the collector at now.
func (h CollectorHealth) State(now time.Time) string {
	switch {
	case h.LastSuccessAt == nil:
		return Never
	case h.MaxAge > 0 && now.Sub(*h.LastSuccessAt) > h.MaxAge:
		return Stale
	case h.ErrorCount >= FailingAfter:
		return Failing
	}
	return Healthy
}

// Report is one collection attempt.
type Report struct {
	TenantID  string
	Collector string
	At        time.Time
	Every     time.Duration
	Err       error
	// Full marks a complete listing (as opposed to an incremental watch).
	Full          bool
	WatchCursor   string
	SourceVersion string
}

// HealthStore is the persistence port (internal/store/cpsql).
type HealthStore interface {
	RecordCollection(ctx context.Context, r Report) error
	CollectorHealth(ctx context.Context, tenantID string) ([]CollectorHealth, error)
}

// Authority names, per predicate, the sources whose word settles a
// disagreement, most authoritative first. The key "*" applies to predicates
// without an entry. A predicate with no authority among the disagreeing
// sources is a conflict.
type Authority map[string][]string

// DefaultAuthority follows the systems of record of design §10.3: the CMDB
// owns ownership and tier, the Kubernetes API owns placement and identity,
// GitOps and the deployment status own the release, telemetry is
// authoritative for nothing it merely labels.
func DefaultAuthority() Authority {
	return Authority{
		"owner": {"cmdb"}, "tier": {"cmdb"}, "team": {"cmdb"}, "criticality": {"cmdb"},
		"node": {"kubernetes"}, "generation": {"kubernetes"}, "image": {"kubernetes", "gitops"},
		"replicas": {"kubernetes"}, "ready_replicas": {"kubernetes"}, "uid": {"kubernetes"},
		"revision": {"gitops", "kubernetes"}, "release": {"gitops", "kubernetes"}, "sync_status": {"gitops"},
		"gpu_uuid": {"kubernetes", "gpu"}, "gpu_model": {"gpu"}, "serving_gpu": {"kubernetes", "inference"},
		"depends_on": {"opentelemetry", "cmdb"},
	}
}

// Decision is the outcome of resolving one predicate.
type Decision struct {
	Value  string `json:"value,omitempty"`
	Source string `json:"source,omitempty"`
	FactID string `json:"fact_id,omitempty"`
	// Conflict is true when sources disagree and no authority settles it.
	Conflict bool `json:"conflict"`
	// Sources are the disagreeing sources with their values.
	Sources map[string]string `json:"sources,omitempty"`
	Reason  string            `json:"reason"`
}

// sourceMatches reports whether a fact source is (or belongs to) an
// authority: "kubernetes" matches "kubernetes" and "kubernetes:prod-east".
func sourceMatches(authority, source string) bool {
	return source == authority || strings.HasPrefix(source, authority+":") || strings.HasPrefix(source, authority+"/")
}

// Resolve settles the value of one (subject, predicate) from its facts.
func (a Authority) Resolve(predicate string, facts []world.Fact) Decision {
	if len(facts) == 0 {
		return Decision{Reason: "no fact"}
	}
	values := map[string]string{}
	distinct := map[string]bool{}
	for _, f := range facts {
		values[f.Source] = f.Value
		distinct[f.Value] = true
	}
	// Newest first, so one source's latest value represents it.
	sorted := append([]world.Fact(nil), facts...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ObservedAt.After(sorted[j].ObservedAt) })
	if len(distinct) == 1 {
		f := sorted[0]
		return Decision{Value: f.Value, Source: f.Source, FactID: f.ID, Reason: "sources agree"}
	}
	order, ok := a[predicate]
	if !ok {
		order = a["*"]
	}
	for _, auth := range order {
		for _, f := range sorted {
			if sourceMatches(auth, f.Source) {
				return Decision{Value: f.Value, Source: f.Source, FactID: f.ID, Sources: values,
					Reason: fmt.Sprintf("%s is the authority for %s", auth, predicate)}
			}
		}
	}
	return Decision{Conflict: true, Sources: values,
		Reason: fmt.Sprintf("sources disagree on %s and none of them is its authority", predicate)}
}

// Quality implements action.FactQuality: the facts an action requires must
// come from collectors that are still reporting and must not be in an
// unsettled conflict.
type Quality struct {
	Health    HealthStore
	Authority Authority
	Clock     cpkit.Clock
}

// Problems lists what must refuse an action that relies on the required
// predicates of target in snap. A failure to read collector health is
// itself a problem: the check fails closed.
func (q Quality) Problems(ctx context.Context, tenantID, target string, required []string, snap *world.Snapshot) []string {
	var out []string
	now := q.Clock.Now()
	bySource := map[string][]CollectorHealth{}
	if q.Health != nil {
		hs, err := q.Health.CollectorHealth(ctx, tenantID)
		if err != nil {
			return []string{"collector health is unavailable; required facts cannot be trusted"}
		}
		for _, h := range hs {
			src, _, _ := strings.Cut(h.Collector, "/")
			bySource[src] = append(bySource[src], h)
		}
	}
	auth := q.Authority
	if auth == nil {
		auth = DefaultAuthority()
	}
	for _, pred := range required {
		var facts []world.Fact
		for _, f := range snap.Facts {
			if f.SubjectRef == target && f.Predicate == pred {
				facts = append(facts, f)
			}
		}
		if len(facts) == 0 {
			continue // missing facts are reported by the freshness check
		}
		d := auth.Resolve(pred, facts)
		if d.Conflict {
			out = append(out, fmt.Sprintf("conflict: %s (%s)", d.Reason, renderSources(d.Sources)))
			continue
		}
		// The collectors behind the deciding source must be reporting. A
		// source with several collectors is trusted while one of them is
		// healthy; a source no collector feeds (an operator assertion) is
		// judged by the fact's own age only.
		src, _, _ := strings.Cut(d.Source, ":")
		cs := bySource[src]
		if len(cs) == 0 {
			continue
		}
		worst, healthy := "", false
		for _, h := range cs {
			switch st := h.State(now); st {
			case Healthy:
				healthy = true
			default:
				if worst == "" || st == Stale || st == Never {
					worst = st
				}
			}
		}
		if !healthy {
			label := "stale"
			if worst == Failing {
				label = "failing"
			}
			out = append(out, fmt.Sprintf("collector %s: the %s collector behind required fact %s stopped reporting (%s); "+
				"the fact is kept but cannot justify a production action", label, src, pred, worst))
		}
	}
	return out
}

func renderSources(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+cpkit.Truncate(m[k], 60))
	}
	return strings.Join(parts, ", ")
}

// SnapshotQuality summarizes a snapshot for a reader: how many facts are
// stale, which predicates are in conflict and which collectors are not
// healthy. It is what a model sees next to the facts, so that an old or
// disputed value is never presented as plain truth.
type SnapshotQuality struct {
	Facts      int               `json:"facts"`
	Stale      int               `json:"stale"`
	Conflicts  []string          `json:"conflicts,omitempty"`
	Collectors map[string]string `json:"collectors,omitempty"`
}

// Summarize builds the quality summary of a snapshot.
func (q Quality) Summarize(ctx context.Context, tenantID string, snap *world.Snapshot) SnapshotQuality {
	out := SnapshotQuality{Facts: len(snap.Facts), Stale: len(snap.Stale), Collectors: map[string]string{}}
	auth := q.Authority
	if auth == nil {
		auth = DefaultAuthority()
	}
	groups := map[string][]world.Fact{}
	for _, f := range snap.Facts {
		k := f.SubjectRef + "|" + f.Predicate
		groups[k] = append(groups[k], f)
	}
	for k, fs := range groups {
		_, pred, _ := strings.Cut(k, "|")
		if auth.Resolve(pred, fs).Conflict {
			out.Conflicts = append(out.Conflicts, k)
		}
	}
	sort.Strings(out.Conflicts)
	if q.Health != nil {
		if hs, err := q.Health.CollectorHealth(ctx, tenantID); err == nil {
			now := q.Clock.Now()
			for _, h := range hs {
				if st := h.State(now); st != Healthy {
					out.Collectors[h.Collector] = st
				}
			}
		}
	}
	return out
}
