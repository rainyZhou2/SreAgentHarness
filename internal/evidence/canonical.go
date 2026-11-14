package evidence

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
)

func canonicalResults(envs []ResultEnvelope) ([]ResultEnvelope, error) {
	type entry struct {
		key string
		env ResultEnvelope
	}
	entries := make([]entry, 0, len(envs))
	for _, env := range envs {
		env.Hypotheses = slices.Clone(env.Hypotheses)
		type hypothesis struct {
			key   string
			value ResultHypothesis
		}
		hs := make([]hypothesis, 0, len(env.Hypotheses))
		for _, h := range env.Hypotheses {
			h.Supports, h.Refutes = sortedReferences(h.Supports), sortedReferences(h.Refutes)
			key, err := json.Marshal(h)
			if err != nil {
				return nil, fmt.Errorf("%w: hypothesis input: %v", ErrInvalid, err)
			}
			hs = append(hs, hypothesis{string(key), h})
		}
		sort.Slice(hs, func(i, j int) bool { return hs[i].key < hs[j].key })
		for i, h := range hs {
			env.Hypotheses[i] = h.value
		}
		env.NextChecks, env.Uncertainties = sortedReferences(env.NextChecks), sortedReferences(env.Uncertainties)
		key, err := json.Marshal(env)
		if err != nil {
			return nil, fmt.Errorf("%w: result input: %v", ErrInvalid, err)
		}
		entries = append(entries, entry{string(key), env})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
	out := make([]ResultEnvelope, len(entries))
	for i, entry := range entries {
		out[i] = entry.env
	}
	return out, nil
}

func sortedReferences(refs []string) []string {
	out := slices.Clone(refs)
	sort.Strings(out)
	return slices.Compact(out)
}
