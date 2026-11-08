// Package world keeps the sourced, versioned and time-aware view of the
// environment that agents reason over (RFC §7): entities, facts and
// relations with two time axes —
//
//   - valid time: when the fact was true in the real world (valid_from/to);
//   - knowledge time: when the platform learned it (observed_at, and
//     closed_observed_at for the moment an interval was closed).
//
// A query "as of T, as known at K" therefore reconstructs exactly what the
// platform could have known at K about time T, so a later rollback never
// overwrites what the topology looked like during an incident. Facts are
// never updated in place: a new value closes the previous interval of the
// same (subject, predicate, source). Different sources never overwrite each
// other; conflicts stay visible.
//
// This World graph is separate from internal/graph (the capability and
// provenance graph): they differ in trust, time and permissions (ADR-002).
package world

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"time"

	"go-sre-harness/internal/cpkit"
)

// Trust tiers of a fact source (aligned with tool output trust).
const (
	TrustAuthoritative = "T1" // operator registry, CMDB of record
	TrustFirstParty    = "T2" // first-party systems (k8s API, deploy system)
	TrustTelemetry     = "T3" // metrics, logs, traces
	TrustUntrusted     = "T4" // MCP/free text/model-derived
)

// Entity is an object with a stable identity.
type Entity struct {
	TenantID   string            `json:"tenant_id"`
	Ref        string            `json:"ref"`
	Kind       string            `json:"kind"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Source     string            `json:"source"`
	Version    int64             `json:"version"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

// Fact is one sourced statement about a subject over a valid-time interval.
type Fact struct {
	ID               string     `json:"id"`
	TenantID         string     `json:"tenant_id"`
	SubjectRef       string     `json:"subject_ref"`
	Predicate        string     `json:"predicate"`
	Value            string     `json:"value"`
	Source           string     `json:"source"`
	TrustTier        string     `json:"trust_tier"`
	ValidFrom        time.Time  `json:"valid_from"`
	ValidTo          *time.Time `json:"valid_to,omitempty"`
	ObservedAt       time.Time  `json:"observed_at"`
	ClosedObservedAt *time.Time `json:"closed_observed_at,omitempty"`
	// ConfirmedAt is the last re-observation of the same value. It only
	// feeds freshness; visibility ("as known at") uses ObservedAt.
	ConfirmedAt *time.Time `json:"confirmed_at,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	EvidenceRef string     `json:"evidence_ref,omitempty"`
	Confidence  float64    `json:"confidence"`
}

// Relation is a directed, sourced, time-bounded edge.
type Relation struct {
	ID               string     `json:"id"`
	TenantID         string     `json:"tenant_id"`
	FromRef          string     `json:"from_ref"`
	Type             string     `json:"type"`
	ToRef            string     `json:"to_ref"`
	Source           string     `json:"source"`
	ValidFrom        time.Time  `json:"valid_from"`
	ValidTo          *time.Time `json:"valid_to,omitempty"`
	ObservedAt       time.Time  `json:"observed_at"`
	ClosedObservedAt *time.Time `json:"closed_observed_at,omitempty"`
}

// Snapshot is a pinned, digest-addressed view for a purpose (a run, a
// preflight). It is immutable once saved: retries read the same snapshot.
type Snapshot struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	Subjects  []string   `json:"subjects"`
	AsOf      time.Time  `json:"as_of"`
	KnownAt   time.Time  `json:"known_at"`
	CreatedAt time.Time  `json:"created_at"`
	Facts     []Fact     `json:"facts"`
	Relations []Relation `json:"relations"`
	Digest    string     `json:"digest"`
	// Missing lists "subject|predicate" pairs that were required but absent.
	Missing []string `json:"missing,omitempty"`
	// Stale lists fact ids older than the requested max age (or expired).
	Stale     []string `json:"stale,omitempty"`
	Truncated bool     `json:"truncated,omitempty"`
}

// Errors.
var (
	ErrInvalid  = errors.New("invalid world data")
	ErrNotFound = errors.New("world object not found")
)

// Limits.
const (
	MaxValueLen   = 2048
	MaxDepth      = 4
	MaxNodes      = 500
	DefaultNodes  = 100
	MaxPredicates = 64
)

var nameRe = regexp.MustCompile(`^[a-z][a-z0-9_.:-]{0,127}$`)

// Normalize validates a fact and fills defaults.
func (f *Fact) Normalize(now time.Time) error {
	ref, err := cpkit.NormalizeRef(f.SubjectRef)
	if err != nil {
		return fmt.Errorf("%w: subject: %v", ErrInvalid, err)
	}
	f.SubjectRef = ref
	if !nameRe.MatchString(f.Predicate) {
		return fmt.Errorf("%w: predicate %q", ErrInvalid, f.Predicate)
	}
	if f.Source == "" || len(f.Source) > 128 {
		return fmt.Errorf("%w: source is required", ErrInvalid)
	}
	if len(f.Value) > MaxValueLen {
		return fmt.Errorf("%w: value exceeds %d bytes", ErrInvalid, MaxValueLen)
	}
	switch f.TrustTier {
	case "":
		f.TrustTier = TrustTelemetry
	case TrustAuthoritative, TrustFirstParty, TrustTelemetry, TrustUntrusted:
	default:
		return fmt.Errorf("%w: trust tier %q", ErrInvalid, f.TrustTier)
	}
	if f.ObservedAt.IsZero() {
		f.ObservedAt = now
	}
	if f.ValidFrom.IsZero() {
		f.ValidFrom = f.ObservedAt
	}
	f.ValidFrom, f.ObservedAt = f.ValidFrom.UTC(), f.ObservedAt.UTC()
	if f.ValidTo != nil {
		v := f.ValidTo.UTC()
		if !v.After(f.ValidFrom) {
			return fmt.Errorf("%w: valid_to must be after valid_from", ErrInvalid)
		}
		f.ValidTo = &v
	}
	if f.ObservedAt.After(now.Add(10 * time.Minute)) {
		return fmt.Errorf("%w: observed_at is in the future", ErrInvalid)
	}
	if f.Confidence == 0 {
		f.Confidence = 1
	}
	if f.Confidence < 0 || f.Confidence > 1 {
		return fmt.Errorf("%w: confidence must be in [0,1]", ErrInvalid)
	}
	if f.ID == "" {
		f.ID = cpkit.NewID()
	}
	return nil
}

// Normalize validates a relation.
func (r *Relation) Normalize(now time.Time) error {
	var err error
	if r.FromRef, err = cpkit.NormalizeRef(r.FromRef); err != nil {
		return fmt.Errorf("%w: from: %v", ErrInvalid, err)
	}
	if r.ToRef, err = cpkit.NormalizeRef(r.ToRef); err != nil {
		return fmt.Errorf("%w: to: %v", ErrInvalid, err)
	}
	if !nameRe.MatchString(r.Type) {
		return fmt.Errorf("%w: relation type %q", ErrInvalid, r.Type)
	}
	if r.Source == "" || len(r.Source) > 128 {
		return fmt.Errorf("%w: source is required", ErrInvalid)
	}
	if r.ObservedAt.IsZero() {
		r.ObservedAt = now
	}
	if r.ValidFrom.IsZero() {
		r.ValidFrom = r.ObservedAt
	}
	r.ValidFrom, r.ObservedAt = r.ValidFrom.UTC(), r.ObservedAt.UTC()
	if r.ID == "" {
		r.ID = cpkit.NewID()
	}
	return nil
}

// Normalize validates an entity.
func (e *Entity) Normalize() error {
	ref, err := cpkit.NormalizeRef(e.Ref)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	e.Ref = ref
	if !nameRe.MatchString(e.Kind) {
		return fmt.Errorf("%w: kind %q", ErrInvalid, e.Kind)
	}
	if len(e.Attributes) > 64 {
		return fmt.Errorf("%w: too many attributes", ErrInvalid)
	}
	if e.Source == "" {
		return fmt.Errorf("%w: source is required", ErrInvalid)
	}
	return nil
}

// VisibleAt implements the bitemporal visibility rule.
func (f *Fact) VisibleAt(validAt, knownAt time.Time) bool {
	if f.ObservedAt.After(knownAt) || f.ValidFrom.After(validAt) {
		return false
	}
	return f.ValidTo == nil || f.ValidTo.After(validAt) || (f.ClosedObservedAt != nil && f.ClosedObservedAt.After(knownAt))
}

// VisibleAt implements the bitemporal visibility rule for relations.
func (r *Relation) VisibleAt(validAt, knownAt time.Time) bool {
	if r.ObservedAt.After(knownAt) || r.ValidFrom.After(validAt) {
		return false
	}
	return r.ValidTo == nil || r.ValidTo.After(validAt) || (r.ClosedObservedAt != nil && r.ClosedObservedAt.After(knownAt))
}

// Store is the persistence port (internal/store/cpsql).
type Store interface {
	UpsertEntity(ctx context.Context, e Entity, now time.Time) (Entity, error)
	GetEntity(ctx context.Context, tenantID, ref string) (Entity, error)
	FindEntities(ctx context.Context, tenantID, attribute, value string, limit int) ([]Entity, error)
	// AssertFact inserts f and closes the overlapping interval of the same
	// (subject, predicate, source); asserting the value already in force is
	// a no-op that returns the existing fact.
	AssertFact(ctx context.Context, f Fact) (Fact, bool, error)
	AssertRelation(ctx context.Context, r Relation) (Relation, bool, error)
	// RetractRelation closes the open interval of an edge at validTo.
	RetractRelation(ctx context.Context, tenantID, from, typ, to, source string, validTo, observedAt time.Time) error
	// FactsAt returns facts of the subjects visible at (validAt, knownAt);
	// predicates filters when non-empty.
	FactsAt(ctx context.Context, tenantID string, subjects, predicates []string, validAt, knownAt time.Time) ([]Fact, error)
	// RelationsAt returns edges touching the subjects (either direction).
	RelationsAt(ctx context.Context, tenantID string, subjects []string, validAt, knownAt time.Time) ([]Relation, error)
	SaveSnapshot(ctx context.Context, s Snapshot) error
	GetSnapshot(ctx context.Context, tenantID, id string) (Snapshot, error)
}

// SnapshotRequest asks for a bounded view.
type SnapshotRequest struct {
	TenantID string
	Subjects []string
	// AsOf is the valid time (default now); KnownAt the knowledge time
	// (default now — "everything we know today about AsOf").
	AsOf    time.Time
	KnownAt time.Time
	// MaxAge flags facts observed more than MaxAge before AsOf as stale.
	MaxAge time.Duration
	// Required predicates must exist for every requested subject.
	Required []string
	// Depth expands the neighborhood through relations (0 = subjects only).
	Depth    int
	MaxNodes int
	// Persist saves the snapshot (for pinning to runs and actions).
	Persist bool
}

// Service implements queries over a Store.
type Service struct {
	Store Store
	Clock cpkit.Clock
}

// Snapshot builds (and optionally persists) a digest-addressed view.
func (s *Service) Snapshot(ctx context.Context, req SnapshotRequest) (Snapshot, error) {
	now := s.Clock.Now()
	if req.AsOf.IsZero() {
		req.AsOf = now
	}
	if req.KnownAt.IsZero() {
		req.KnownAt = now
	}
	req.AsOf, req.KnownAt = req.AsOf.UTC(), req.KnownAt.UTC()
	if req.KnownAt.Before(req.AsOf) && req.KnownAt.Equal(now) {
		return Snapshot{}, fmt.Errorf("%w: as_of is in the future", ErrInvalid)
	}
	subjects, err := cpkit.NormalizeRefs(req.Subjects)
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if len(subjects) == 0 {
		return Snapshot{}, fmt.Errorf("%w: at least one subject is required", ErrInvalid)
	}
	if req.Depth < 0 || req.Depth > MaxDepth {
		return Snapshot{}, fmt.Errorf("%w: depth must be within [0,%d]", ErrInvalid, MaxDepth)
	}
	if len(req.Required) > MaxPredicates {
		return Snapshot{}, fmt.Errorf("%w: too many required predicates", ErrInvalid)
	}
	maxNodes := req.MaxNodes
	if maxNodes <= 0 {
		maxNodes = DefaultNodes
	}
	maxNodes = min(maxNodes, MaxNodes)
	snap := Snapshot{ID: cpkit.NewID(), TenantID: req.TenantID, Subjects: subjects, AsOf: req.AsOf,
		KnownAt: req.KnownAt, CreatedAt: now}

	// Bounded breadth-first expansion through relations.
	nodes := map[string]bool{}
	for _, sub := range subjects {
		nodes[sub] = true
	}
	frontier := subjects
	relSeen := map[string]bool{}
	for depth := 0; depth < req.Depth && len(frontier) > 0; depth++ {
		rels, err := s.Store.RelationsAt(ctx, req.TenantID, frontier, req.AsOf, req.KnownAt)
		if err != nil {
			return Snapshot{}, err
		}
		var next []string
		for _, r := range rels {
			if relSeen[r.ID] {
				continue
			}
			for _, end := range []string{r.FromRef, r.ToRef} {
				if !nodes[end] {
					if len(nodes) >= maxNodes {
						snap.Truncated = true
						continue
					}
					nodes[end] = true
					next = append(next, end)
				}
			}
			if nodes[r.FromRef] && nodes[r.ToRef] {
				relSeen[r.ID] = true
				snap.Relations = append(snap.Relations, r)
			}
		}
		frontier = next
	}
	all := make([]string, 0, len(nodes))
	for n := range nodes {
		all = append(all, n)
	}
	sort.Strings(all)
	facts, err := s.Store.FactsAt(ctx, req.TenantID, all, nil, req.AsOf, req.KnownAt)
	if err != nil {
		return Snapshot{}, err
	}
	snap.Facts = facts
	have := map[string]bool{}
	for _, f := range facts {
		have[f.SubjectRef+"|"+f.Predicate] = true
		stale := req.MaxAge > 0 && req.AsOf.Sub(f.SeenBy(req.KnownAt)) > req.MaxAge
		if f.ExpiresAt != nil && !f.ExpiresAt.After(req.AsOf) {
			stale = true
		}
		if stale {
			snap.Stale = append(snap.Stale, f.ID)
		}
	}
	for _, sub := range subjects {
		for _, p := range req.Required {
			if !have[sub+"|"+p] {
				snap.Missing = append(snap.Missing, sub+"|"+p)
			}
		}
	}
	sortSnapshot(&snap)
	snap.Digest = Digest(snap)
	if req.Persist {
		if err := s.Store.SaveSnapshot(ctx, snap); err != nil {
			return Snapshot{}, err
		}
	}
	return snap, nil
}

func sortSnapshot(s *Snapshot) {
	sort.Slice(s.Facts, func(i, j int) bool {
		a, b := s.Facts[i], s.Facts[j]
		if a.SubjectRef != b.SubjectRef {
			return a.SubjectRef < b.SubjectRef
		}
		if a.Predicate != b.Predicate {
			return a.Predicate < b.Predicate
		}
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		return a.ID < b.ID
	})
	sort.Slice(s.Relations, func(i, j int) bool {
		a, b := s.Relations[i], s.Relations[j]
		if a.FromRef != b.FromRef {
			return a.FromRef < b.FromRef
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		if a.ToRef != b.ToRef {
			return a.ToRef < b.ToRef
		}
		return a.ID < b.ID
	})
	sort.Strings(s.Missing)
	sort.Strings(s.Stale)
}

// Digest is the content address of a snapshot's view (facts, relations and
// the query), independent of its id and creation time.
func Digest(s Snapshot) string {
	type fv struct{ ID, S, P, V, Src string }
	type rv struct{ ID, F, T, To string }
	view := struct {
		Subjects []string
		AsOf     time.Time
		KnownAt  time.Time
		Facts    []fv
		Rels     []rv
		Missing  []string
	}{Subjects: s.Subjects, AsOf: s.AsOf, KnownAt: s.KnownAt, Missing: s.Missing}
	for _, f := range s.Facts {
		view.Facts = append(view.Facts, fv{f.ID, f.SubjectRef, f.Predicate, f.Value, f.Source})
	}
	for _, r := range s.Relations {
		view.Rels = append(view.Rels, rv{r.ID, r.FromRef, r.Type, r.ToRef})
	}
	return cpkit.MustHash(view)
}

// Value returns the value of a predicate for a subject in the snapshot,
// preferring the most trusted, then most recently observed source. ok is
// false when absent; conflict reports disagreeing sources.
func (s *Snapshot) Value(subject, predicate string) (f Fact, ok, conflict bool) {
	for _, x := range s.Facts {
		if x.SubjectRef != subject || x.Predicate != predicate {
			continue
		}
		if !ok {
			f, ok = x, true
			continue
		}
		if x.Value != f.Value {
			conflict = true
		}
		if x.TrustTier < f.TrustTier || (x.TrustTier == f.TrustTier && x.ObservedAt.After(f.ObservedAt)) {
			f = x
		}
	}
	return f, ok, conflict
}

// IsStale reports whether a fact id was flagged stale.
func (s *Snapshot) IsStale(id string) bool {
	i := sort.SearchStrings(s.Stale, id)
	return i < len(s.Stale) && s.Stale[i] == id
}

// Change is one difference between two snapshots.
type Change struct {
	Kind      string `json:"kind"` // fact_added | fact_removed | fact_changed | relation_added | relation_removed
	Subject   string `json:"subject"`
	Predicate string `json:"predicate,omitempty"`
	Source    string `json:"source,omitempty"`
	Before    string `json:"before,omitempty"`
	After     string `json:"after,omitempty"`
}

// Diff compares two snapshots by (subject, predicate, source) and edges.
func Diff(a, b Snapshot) []Change {
	key := func(f Fact) string { return f.SubjectRef + "|" + f.Predicate + "|" + f.Source }
	am, bm := map[string]Fact{}, map[string]Fact{}
	for _, f := range a.Facts {
		am[key(f)] = f
	}
	for _, f := range b.Facts {
		bm[key(f)] = f
	}
	var out []Change
	for k, fa := range am {
		fb, ok := bm[k]
		switch {
		case !ok:
			out = append(out, Change{Kind: "fact_removed", Subject: fa.SubjectRef, Predicate: fa.Predicate, Source: fa.Source, Before: fa.Value})
		case fb.Value != fa.Value:
			out = append(out, Change{Kind: "fact_changed", Subject: fa.SubjectRef, Predicate: fa.Predicate, Source: fa.Source, Before: fa.Value, After: fb.Value})
		}
	}
	for k, fb := range bm {
		if _, ok := am[k]; !ok {
			out = append(out, Change{Kind: "fact_added", Subject: fb.SubjectRef, Predicate: fb.Predicate, Source: fb.Source, After: fb.Value})
		}
	}
	rkey := func(r Relation) string { return r.FromRef + "|" + r.Type + "|" + r.ToRef }
	ar, br := map[string]bool{}, map[string]bool{}
	for _, r := range a.Relations {
		ar[rkey(r)] = true
	}
	for _, r := range b.Relations {
		br[rkey(r)] = true
		if !ar[rkey(r)] {
			out = append(out, Change{Kind: "relation_added", Subject: r.FromRef, Predicate: r.Type, After: r.ToRef})
		}
	}
	for _, r := range a.Relations {
		if !br[rkey(r)] {
			out = append(out, Change{Kind: "relation_removed", Subject: r.FromRef, Predicate: r.Type, Before: r.ToRef})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Subject != out[j].Subject {
			return out[i].Subject < out[j].Subject
		}
		if out[i].Predicate != out[j].Predicate {
			return out[i].Predicate < out[j].Predicate
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// CurrentFact implements outcome.StateSource: the most trusted visible
// value of a predicate now.
func (s *Service) CurrentFact(ctx context.Context, tenantID, subject, predicate string, at time.Time) (value string, observedAt time.Time, ref string, ok bool, err error) {
	facts, err := s.Store.FactsAt(ctx, tenantID, []string{subject}, []string{predicate}, at, at)
	if err != nil || len(facts) == 0 {
		return "", time.Time{}, "", false, err
	}
	snap := Snapshot{Facts: facts}
	f, _, _ := snap.Value(subject, predicate)
	return f.Value, f.SeenBy(at), "fact:" + f.ID, true, nil
}

// Resolution is the outcome of identity resolution.
type Resolution struct {
	Ref        string   `json:"ref,omitempty"`
	Candidates []string `json:"candidates,omitempty"`
}

// Unique reports whether exactly one identity was found.
func (r Resolution) Unique() bool { return r.Ref != "" }

// Resolve maps an input to an authoritative entity: an exact ref, or a
// unique entity whose "alias" attribute equals the input. Ambiguity returns
// candidates and no ref — write actions must refuse it.
func (s *Service) Resolve(ctx context.Context, tenantID, input string) (Resolution, error) {
	if ref, err := cpkit.NormalizeRef(input); err == nil {
		if _, err := s.Store.GetEntity(ctx, tenantID, ref); err == nil {
			return Resolution{Ref: ref}, nil
		} else if !errors.Is(err, ErrNotFound) {
			return Resolution{}, err
		}
	}
	ents, err := s.Store.FindEntities(ctx, tenantID, "alias", input, 10)
	if err != nil {
		return Resolution{}, err
	}
	if len(ents) == 1 {
		return Resolution{Ref: ents[0].Ref}, nil
	}
	var res Resolution
	for _, e := range ents {
		res.Candidates = append(res.Candidates, e.Ref)
	}
	return res, nil
}

// SeenBy is the latest observation of the fact known at knownAt: a
// re-confirmation recorded after knownAt was not known then.
func (f Fact) SeenBy(knownAt time.Time) time.Time {
	if f.ConfirmedAt != nil && f.ConfirmedAt.After(f.ObservedAt) && !f.ConfirmedAt.After(knownAt) {
		return *f.ConfirmedAt
	}
	return f.ObservedAt
}
