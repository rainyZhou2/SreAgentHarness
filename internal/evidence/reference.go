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
	for _, e := range ledger {
		if e.TenantID != scope.TenantID || e.WorkID != scope.WorkID || e.ID == "" {
			continue
		}
		for _, alias := range []string{e.ID, "evidence:" + e.ID, e.SourceRef} {
			if alias == "" {
				continue
			}
			if old, ok := r.aliases[alias]; ok && old.ID != e.ID {
				r.ambiguous[alias] = true
			}
			r.aliases[alias] = e
		}
		if e.Supersedes != "" && (at.IsZero() || !e.CapturedAt.After(at)) {
			r.superseded[strings.TrimPrefix(e.Supersedes, "evidence:")] = true
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
	if r.superseded[found.ID] || r.superseded[found.SourceRef] {
		return Record{}, fmt.Errorf("%w: evidence superseded", ErrInvalid)
	}
	return found, nil
}
