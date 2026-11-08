package cognitive

import (
	"fmt"
	"strconv"
	"strings"

	"go-sre-harness/internal/domain"
)

// RefSet resolves the citations a model writes (supports, refutes,
// evidence_refs) against what the request actually showed it: observations,
// operator evidence and memories. A citation that resolves is rewritten to
// its canonical form; one that does not is kept, marked unresolved, so the
// trace records it and scoring does not count it as grounding.
//
// Canonical forms, also printed in the prompt headers:
//
//	obs:step<N>        tool result of step N (shown, or summarized by compaction)
//	obs:step<N>.<K>    result of call K of a parallel batch step N
//	kv:<key>@<version> operator evidence
//	memory:<id>@<version>
type RefSet struct {
	obs       map[int64]string         // step → tool
	parts     map[int64]map[int]string // batch step → part → tool
	byTool    map[string]string        // tool → latest ref
	handles   map[string]string        // offloaded artifact handle → ref
	compacted int64                    // steps before this were summarized
	evidence  map[string]int64         // key → pinned version
	memories  map[string]int64         // id → pinned version
}

// ObsRef is the canonical citation of a step's observation.
func ObsRef(step int64) string { return fmt.Sprintf("obs:step%d", step) }

// ObsPartRef is the citation of one call of a batch step (part 0: the
// step itself).
func ObsPartRef(step int64, part int) string {
	if part <= 0 {
		return ObsRef(step)
	}
	return fmt.Sprintf("obs:step%d.%d", step, part)
}

// NewRefSet indexes the citable items of a reasoning input.
func NewRefSet(in ReasoningInput) *RefSet {
	r := &RefSet{obs: map[int64]string{}, parts: map[int64]map[int]string{}, byTool: map[string]string{},
		handles: map[string]string{}, compacted: in.CompactedBefore, evidence: map[string]int64{}, memories: map[string]int64{}}
	for _, o := range in.Observations {
		ref := ObsPartRef(o.StepSeq, o.Part)
		if o.Part > 0 {
			if r.parts[o.StepSeq] == nil {
				r.parts[o.StepSeq] = map[int]string{}
			}
			r.parts[o.StepSeq][o.Part] = o.Tool
			if _, ok := r.obs[o.StepSeq]; !ok {
				r.obs[o.StepSeq] = ""
			}
		} else {
			r.obs[o.StepSeq] = o.Tool
		}
		r.byTool[o.Tool] = ref // observations are in step order
		if o.Artifact != nil && o.Artifact.Handle != "" {
			r.handles[o.Artifact.Handle] = ref
		}
	}
	for _, e := range in.Evidence {
		r.evidence[e.Key] = e.Version
	}
	for _, m := range in.Memories {
		r.memories[m.ID] = m.Version
	}
	return r
}

// Resolve returns the canonical form of a citation and whether it names an
// item the model was shown.
func (r *RefSet) Resolve(raw string) (string, bool) {
	ref := strings.TrimSpace(raw)
	if r == nil || ref == "" {
		return ref, false
	}
	if canon, ok := r.handles[ref]; ok {
		return canon, true
	}
	kind, rest, ok := strings.Cut(ref, ":")
	if !ok {
		return ref, false
	}
	switch strings.ToLower(kind) {
	case "obs", "observation", "step":
		return r.resolveObs(rest)
	case "kv", "evidence":
		key, ver, hasVer := cutVersion(rest)
		pinned, ok := r.evidence[key]
		if !ok || (hasVer && ver != pinned) {
			return ref, false
		}
		return fmt.Sprintf("kv:%s@%d", key, pinned), true
	case "memory", "mem":
		id, ver, hasVer := cutVersion(rest)
		pinned, ok := r.memories[id]
		if !ok || (hasVer && ver != pinned) {
			return ref, false
		}
		return fmt.Sprintf("memory:%s@%d", id, pinned), true
	}
	return ref, false
}

// resolveObs accepts step<N>, <N>, step:<N>, step<N>.<K>, <tool>,
// <tool>@step<N> and <tool>@<N>. A step cited with a tool must have called
// that tool (for a batch step: in one of its calls).
func (r *RefSet) resolveObs(s string) (string, bool) {
	s = strings.TrimPrefix(strings.TrimSpace(s), ":")
	tool := ""
	if i := strings.LastIndexByte(s, '@'); i > 0 {
		tool, s = s[:i], s[i+1:]
	}
	part := 0
	if i := strings.LastIndexByte(s, '.'); i > 0 {
		// step<N>.<K>; a dot elsewhere is part of a tool name.
		if _, isStep := stepNumber(s[:i]); isStep {
			k, err := strconv.Atoi(s[i+1:])
			if err != nil || k <= 0 {
				return "", false
			}
			part, s = k, s[:i]
		}
	}
	if n, ok := stepNumber(s); ok {
		if ps := r.parts[n]; len(ps) > 0 {
			if part > 0 {
				t, ok := ps[part]
				if !ok || (tool != "" && tool != t) {
					return "", false
				}
				return ObsPartRef(n, part), true
			}
			if tool != "" {
				for k, t := range ps {
					if t == tool {
						return ObsPartRef(n, k), true
					}
				}
				return "", false
			}
			return ObsRef(n), true
		}
		if part > 0 {
			if n < r.compacted && tool == "" {
				return ObsPartRef(n, part), true // summarized by compaction
			}
			return "", false
		}
		if t, shown := r.obs[n]; shown {
			if tool != "" && tool != t {
				return "", false
			}
			return ObsRef(n), true
		}
		if n > 0 && n < r.compacted && tool == "" {
			return ObsRef(n), true // summarized by compaction
		}
		return "", false
	}
	if tool == "" {
		tool = s
	}
	if ref, ok := r.byTool[tool]; ok {
		return ref, true
	}
	return "", false
}

func stepNumber(s string) (int64, bool) {
	s = strings.TrimPrefix(strings.TrimPrefix(strings.ToLower(s), "step"), ":")
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n > 0
}

func cutVersion(s string) (string, int64, bool) {
	if i := strings.LastIndexByte(s, '@'); i > 0 {
		if v, err := strconv.ParseInt(s[i+1:], 10, 64); err == nil {
			return s[:i], v, true
		}
	}
	return s, 0, false
}

// ResolveRefs resolves citations into thought refs. Unresolved ones keep
// their text behind domain.UnresolvedRefPrefix.
func (r *RefSet) ResolveRefs(raws []string, relation string) (refs []domain.ThoughtRef, unresolved int) {
	seen := map[string]bool{}
	for _, raw := range raws {
		ref, ok := r.Resolve(raw)
		if !ok {
			unresolved++
			ref = domain.UnresolvedRefPrefix + clipRef(raw)
		}
		if ref == domain.UnresolvedRefPrefix || seen[ref] {
			continue
		}
		seen[ref] = true
		refs = append(refs, domain.ThoughtRef{Ref: ref, Relation: relation})
	}
	return refs, unresolved
}

func clipRef(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 200 {
		s = string(r[:200])
	}
	return s
}
