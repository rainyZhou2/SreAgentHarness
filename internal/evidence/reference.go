package evidence

import (
	"fmt"
	"strings"
	"time"
)

// ReferenceResolver resolves only records of one tenant and Work. Ambiguous
// source aliases fail closed instead of depending on database row order.
type ReferenceResolver struct {
	aliases    map[string]Record
	ambiguous  map[string]bool
	superseded map[string]bool
}

func NewReferenceResolver(scope DecisionScope, ledger []Record) *ReferenceResolver {
	return newReferenceResolverAt(scope, ledger, time.Time{})
}

func newReferenceResolverAt(scope DecisionScope, ledger []Record, at time.Time) *ReferenceResolver {
	r := &ReferenceResolver{aliases: map[string]Record{}, ambiguous: map[string]bool{}, superseded: map[string]bool{}}
	bySource := map[string][]Record{}
	byID := map[string]Record{}
	for _, e := range ledger {
		if e.TenantID != scope.TenantID || e.WorkID != scope.WorkID || e.ID == "" || (!at.IsZero() && e.CapturedAt.After(at)) {
			continue
		}
		byID[e.ID] = e
		bySource[e.SourceRef] = append(bySource[e.SourceRef], e)
		for _, alias := range []string{e.ID, "evidence:" + e.ID, e.SourceRef} {
			if alias == "" {
				continue
			}
			if old, ok := r.aliases[alias]; ok && old.ID != e.ID {
				r.ambiguous[alias] = true
			}
			r.aliases[alias] = e
		}
	}
	for _, e := range byID {
		ref, canonical := strings.CutPrefix(e.Supersedes, "evidence:")
		if ref == "" {
			continue
		}
		var predecessors []Record
		if old, ok := byID[ref]; ok {
			predecessors = []Record{old}
		} else if !canonical {
			predecessors = bySource[ref]
		}
		for _, old := range predecessors {
			if old.ID != e.ID && !old.CapturedAt.After(e.CapturedAt) {
				r.superseded[old.ID] = true
			}
		}
	}
	return r
}

func (r *ReferenceResolver) Resolve(ref, runID string) (Record, error) {
	aliases := []string{strings.TrimSpace(ref)}
	if runID != "" && !strings.Contains(ref, "://") && !strings.HasPrefix(ref, "run:") {
		aliases = append(aliases, "run:"+runID+"#"+ref)
	}
	var found Record
	for _, alias := range aliases {
		if r.ambiguous[alias] {
			return Record{}, fmt.Errorf("%w: ambiguous evidence reference", ErrInvalid)
		}
		if rec, ok := r.aliases[alias]; ok {
			if found.ID != "" && found.ID != rec.ID {
				return Record{}, fmt.Errorf("%w: ambiguous evidence reference", ErrInvalid)
			}
			found = rec
		}
	}
	if found.ID == "" {
		return Record{}, fmt.Errorf("%w: not in this Work's evidence ledger", ErrInvalid)
	}
	if r.superseded[found.ID] {
		return Record{}, fmt.Errorf("%w: evidence superseded", ErrInvalid)
	}
	return found, nil
}
