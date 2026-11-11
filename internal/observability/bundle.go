package observability

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/evidence"
)

// PlanItem is one query of an evidence bundle.
type PlanItem struct {
	// Role names what the item contributes ("error_rate", "slow_spans").
	Role   string `json:"role"`
	Signal Signal `json:"signal"`
	// SourceID pins a source; empty routes to the best source in scope.
	SourceID string `json:"source_id,omitempty"`
	QueryRef string `json:"query_ref,omitempty"`
	// Required items must return usable data for the bundle to be complete.
	Required bool `json:"required,omitempty"`
	// Window is how far back the item looks (default 15m).
	Window cpkit.Duration `json:"window,omitempty"`
}

// Item is the result of one plan item.
type Item struct {
	Role     string `json:"role"`
	Required bool   `json:"required,omitempty"`
	Result
	// EvidenceRef is the ledger record ("evidence:<id>") when the result
	// was usable and a Work was given.
	EvidenceRef string `json:"evidence_ref,omitempty"`
}

// Bundle statuses.
const (
	BundleComplete    = "complete"    // every item returned usable data
	BundlePartial     = "partial"     // some items failed, were empty or stale
	BundleUnavailable = "unavailable" // nothing usable came back
)

// Bundle is a set of observations about one subject, each with its source
// reference, digest, window and quality.
type Bundle struct {
	SubjectRef string    `json:"subject_ref"`
	WorkID     string    `json:"work_id,omitempty"`
	Status     string    `json:"status"` // complete|partial|unavailable
	Items      []Item    `json:"items"`
	Sources    []string  `json:"sources"`
	Problems   []string  `json:"problems,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// Healthy reports whether the bundle can support a statement that the
// subject is healthy: only a complete bundle can. A source that returned
// nothing, stale data or an error leaves the question open — an absent
// signal is never read as a good one.
func (b Bundle) Healthy() bool { return b.Status == BundleComplete }

// EvidenceRefs lists the ledger records of the bundle.
func (b Bundle) EvidenceRefs() []string {
	var out []string
	for _, it := range b.Items {
		if it.EvidenceRef != "" {
			out = append(out, it.EvidenceRef)
		}
	}
	return out
}

// EvidenceSink is where usable results are recorded (the Work ledger).
type EvidenceSink interface {
	AddEvidence(ctx context.Context, r evidence.Record) (evidence.Record, bool, error)
}

func sourceType(s Signal) string {
	switch s {
	case SignalMetrics:
		return evidence.SourceMetric
	case SignalLogs:
		return evidence.SourceLog
	case SignalTraces:
		return evidence.SourceTrace
	case SignalChange:
		return evidence.SourceChange
	}
	return evidence.SourceEvent
}

// Quality maps a result to an evidence quality.
func Quality(r Result) string {
	for _, w := range r.Warnings {
		if strings.HasPrefix(w, "injected:") {
			return evidence.QualityInjected
		}
	}
	switch r.Status {
	case StatusOK:
		if r.Truncated {
			return evidence.QualityPartial
		}
		return evidence.QualityValid
	case StatusStale:
		return evidence.QualityStale
	case StatusPartial:
		return evidence.QualityPartial
	}
	return evidence.QualityUnresolved
}

// EvidenceOf turns a result into a ledger record. Only results with a
// stored body become evidence: a refusal or an outage is a problem of the
// bundle, not an observation.
func EvidenceOf(tenantID, workID string, r Result, relation string) (evidence.Record, bool) {
	if r.DataRef == "" || (r.Status != StatusOK && r.Status != StatusStale && r.Status != StatusPartial) {
		return evidence.Record{}, false
	}
	from, to := r.Start, r.End
	kind := evidence.KindObservation
	if r.Signal == SignalChange {
		kind = evidence.KindChange
		if relation == "" {
			// A change near a symptom is a lead, not a cause.
			relation = evidence.RelCorrelates
		}
	}
	trust := "T3"
	if r.Signal == SignalChange {
		trust = "T2"
	}
	return evidence.Record{TenantID: tenantID, WorkID: workID, SourceType: sourceType(r.Signal),
		SourceRef:   fmt.Sprintf("obs://%s/%s#%s", r.SourceID, firstNonEmpty(r.QueryRef, "raw"), strings.TrimPrefix(r.QueryDigest, "sha256:")[:16]),
		ArtifactRef: r.DataRef, ContentHash: r.Digest, Summary: r.Summary, OccurredFrom: &from, OccurredTo: &to,
		CapturedAt: r.ReceivedAt, TrustTier: trust, Kind: kind, Relation: relation, Quality: Quality(r), SourceID: r.SourceID,
		IndependenceGroup: fmt.Sprintf("%s|%s|%d-%d", r.SourceID, r.QueryDigest, r.Start.Unix(), r.End.Unix()),
		Redaction:         redactionLevel(r)}, true
}

func redactionLevel(r Result) string {
	if r.Redacted > 0 {
		return "redacted"
	}
	return ""
}

// Collect runs a plan about one subject concurrently and assembles the
// bundle. One failing source does not fail the others: the bundle becomes
// partial and names what is missing. Usable results are recorded in the
// Work ledger when sink is given.
func (b *Broker) Collect(ctx context.Context, c Caller, subject string, plan []PlanItem, sink EvidenceSink) Bundle {
	b.init()
	now := b.now()
	out := Bundle{SubjectRef: subject, WorkID: c.RC.WorkID, CreatedAt: now, Items: make([]Item, len(plan))}
	var all []Source
	if c.RC.TenantID != "" {
		all, _ = b.Catalog.ListSources(ctx, c.RC.TenantID, Filter{UsableOnly: true})
	}
	var wg sync.WaitGroup
	for i, p := range plan {
		wg.Add(1)
		go func(i int, p PlanItem) {
			defer wg.Done()
			window := p.Window.D()
			if window <= 0 {
				window = 15 * time.Minute
			}
			q := Query{SourceID: p.SourceID, Signal: p.Signal, SubjectRef: subject, QueryRef: p.QueryRef, Start: now.Add(-window), End: now}
			it := Item{Role: p.Role, Required: p.Required}
			if q.SourceID == "" {
				// Route to the best source that has the query template.
				for _, s := range Route(all, p.Signal, subject, "") {
					if _, ok := s.Queries[p.QueryRef]; ok || s.Backend == "ledger" {
						q.SourceID = s.ID
						break
					}
				}
			}
			if q.SourceID == "" {
				it.Result = refuse(q, StatusUnavailable, fmt.Sprintf("source_missing: no %s source in scope answers %q", p.Signal, p.QueryRef), now)
			} else {
				it.Result = b.Query(ctx, c, q)
			}
			out.Items[i] = it
		}(i, p)
	}
	wg.Wait()
	usable, seen := 0, map[string]bool{}
	for i := range out.Items {
		it := &out.Items[i]
		if it.SourceID != "" && !seen[it.SourceID] {
			seen[it.SourceID] = true
			out.Sources = append(out.Sources, it.SourceID)
		}
		if it.Status == StatusOK {
			usable++
		} else {
			out.Problems = append(out.Problems, fmt.Sprintf("%s: %s (%s)", it.Role, it.Status, strings.Join(it.Warnings, "; ")))
		}
		if sink == nil || c.RC.WorkID == "" {
			continue
		}
		rec, ok := EvidenceOf(c.RC.TenantID, c.RC.WorkID, it.Result, "")
		if !ok {
			continue
		}
		if stored, _, err := sink.AddEvidence(ctx, rec); err == nil {
			it.EvidenceRef = "evidence:" + stored.ID
		} else {
			out.Problems = append(out.Problems, it.Role+": evidence not recorded: "+cpkit.Truncate(err.Error(), 120))
		}
	}
	sort.Strings(out.Sources)
	switch {
	case usable == len(out.Items) && usable > 0:
		out.Status = BundleComplete
	case usable == 0:
		out.Status = BundleUnavailable
	default:
		out.Status = BundlePartial
	}
	return out
}
