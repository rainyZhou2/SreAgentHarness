package improve

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/reasoning"
)

// PatternDecision is one retirement or reinstatement with its evidence.
type PatternDecision struct {
	MemoryID string           `json:"memory_id"`
	Basis    string           `json:"basis"` // cited | retrieved
	Effect   reasoning.Effect `json:"effect"`
}

// ExclusionPlan is the proposed change to reasoning.patterns_excluded.
type ExclusionPlan struct {
	Retire    []PatternDecision `json:"retire,omitempty"`
	Reinstate []PatternDecision `json:"reinstate,omitempty"`
	// Pruned are excluded ids whose memory is gone or no longer ACTIVE:
	// retrieval never returns them, so the entry only takes capacity.
	Pruned []string `json:"pruned,omitempty"`
	// Deferred retirements did not fit under the exclusion limit.
	Deferred []PatternDecision `json:"deferred,omitempty"`
	Next     []string          `json:"next"`
}

// Changes reports whether the plan retires or reinstates anything. Pruning
// alone never triggers a proposal; it rides along with real changes.
func (p ExclusionPlan) Changes() bool { return len(p.Retire) > 0 || len(p.Reinstate) > 0 }

// PlanExclusions decides which patterns to retire and which excluded ones to
// reinstate, from baseline-relative effects, and enforces the list's limit:
// stale entries are pruned first, then the strongest retirements are kept.
func PlanExclusions(ctx context.Context, st Store, tenant string, current []string, stats map[string]reasoning.PatternStat,
	cfg Config) (ExclusionPlan, error) {
	cfg = cfg.Defaults()
	plan := ExclusionPlan{}
	excluded := map[string]bool{}
	keep := []string{}
	for _, id := range current {
		if excluded[id] {
			continue
		}
		excluded[id] = true
		m, err := st.GetMemory(ctx, tenant, id)
		switch {
		case errors.Is(err, domain.ErrNotFound) || (err == nil && m.Status != "ACTIVE"):
			plan.Pruned = append(plan.Pruned, id)
			continue
		case err != nil:
			return plan, fmt.Errorf("load excluded memory %s: %w", id, err)
		}
		if s, ok := stats[id]; ok {
			if ef, basis := s.Decisive(cfg.MinUses); ef != nil && ef.Z >= cfg.ReinstateZ {
				plan.Reinstate = append(plan.Reinstate, PatternDecision{MemoryID: id, Basis: basis, Effect: *ef})
				continue
			}
		}
		keep = append(keep, id)
	}
	var retire []PatternDecision
	for _, s := range reasoning.SortedStats(stats) {
		if excluded[s.MemoryID] {
			continue
		}
		ef, basis := s.Decisive(cfg.MinUses)
		if ef == nil || ef.Z > -cfg.RetireZ || ef.Lift > -cfg.MinLift {
			continue
		}
		m, err := st.GetMemory(ctx, tenant, s.MemoryID)
		if err != nil || m.Status != "ACTIVE" || !reasoning.IsPattern(m) {
			continue
		}
		retire = append(retire, PatternDecision{MemoryID: s.MemoryID, Basis: basis, Effect: *ef})
	}
	// Strongest evidence first, so the limit keeps the clearest cases.
	sort.Slice(retire, func(i, j int) bool {
		if retire[i].Effect.Z != retire[j].Effect.Z {
			return retire[i].Effect.Z < retire[j].Effect.Z
		}
		return retire[i].MemoryID < retire[j].MemoryID
	})
	room := profile.MaxExcludedPatterns - len(keep)
	for i, r := range retire {
		if i < room {
			plan.Retire = append(plan.Retire, r)
			keep = append(keep, r.MemoryID)
		} else {
			plan.Deferred = append(plan.Deferred, r)
		}
	}
	sort.Strings(keep)
	plan.Next = keep
	return plan, nil
}

// ProposeExclusionChange lets an operator retire or reinstate patterns by
// hand. It proposes a candidate of the active version through the gated
// pipeline; another human must approve it.
func ProposeExclusionChange(ctx context.Context, st Store, evo Evolution, tenant, profileID string, add, remove []string,
	actor, note string) (eval.Candidate, error) {
	if len(add) == 0 && len(remove) == 0 {
		return eval.Candidate{}, fmt.Errorf("%w: nothing to change", eval.ErrInvalid)
	}
	base, err := st.ActiveVersion(ctx, tenant, profileID)
	if err != nil || base <= 0 {
		return eval.Candidate{}, fmt.Errorf("%w: profile %s has no active version", eval.ErrInvalid, profileID)
	}
	raw, err := st.ProfileSpec(ctx, tenant, profileID, base)
	if err != nil {
		return eval.Candidate{}, err
	}
	spec, err := profile.Parse(raw)
	if err != nil {
		return eval.Candidate{}, err
	}
	next := profile.ReasoningSpec{}
	if spec.Reasoning != nil {
		next = *spec.Reasoning
	}
	set := map[string]bool{}
	for _, id := range next.PatternsExcluded {
		set[id] = true
	}
	for _, id := range remove {
		if !set[id] {
			return eval.Candidate{}, fmt.Errorf("%w: %s is not excluded", eval.ErrInvalid, id)
		}
		delete(set, id)
	}
	for _, id := range add {
		if set[id] {
			return eval.Candidate{}, fmt.Errorf("%w: %s is already excluded", eval.ErrInvalid, id)
		}
		if _, err := st.GetMemory(ctx, tenant, id); err != nil {
			return eval.Candidate{}, fmt.Errorf("%w: memory %s: %v", eval.ErrInvalid, id, err)
		}
		set[id] = true
	}
	next.PatternsExcluded = next.PatternsExcluded[:0:0]
	for id := range set {
		next.PatternsExcluded = append(next.PatternsExcluded, id)
	}
	sort.Strings(next.PatternsExcluded)
	if err := next.Validate(); err != nil {
		return eval.Candidate{}, fmt.Errorf("%w: %v", eval.ErrInvalid, err)
	}
	patch, _ := json.Marshal(map[string]any{"reasoning": next})
	rationale, _ := json.Marshal(map[string]any{"manual": true, "exclude": add, "reinstate": remove, "note": note})
	return evo.Propose(ctx, tenant, profileID, base, patch, string(rationale), actor)
}
