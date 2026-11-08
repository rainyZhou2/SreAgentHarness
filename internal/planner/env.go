package planner

import (
	"encoding/json"
	"fmt"
	"strings"
)

// WithClassifier attaches the n-gram classifier over DefaultExamples plus
// the examples in examplesPath ("" for none) and returns the router.
func (r *Router) WithClassifier(examplesPath string) (*Router, error) {
	examples := append([]Example(nil), DefaultExamples...)
	if p := strings.TrimSpace(examplesPath); p != "" {
		extra, err := LoadExamples(p)
		if err != nil {
			return nil, err
		}
		examples = append(examples, extra...)
	}
	r.Classifier = NewNgramClassifier(examples)
	return r, nil
}

// ParseFallbacks parses HARNESS_PLANNER_FALLBACKS, a JSON array of
// FallbackRule ("" means none).
func ParseFallbacks(v string) ([]FallbackRule, error) {
	if strings.TrimSpace(v) == "" {
		return nil, nil
	}
	var rules []FallbackRule
	if err := json.Unmarshal([]byte(v), &rules); err != nil {
		return nil, fmt.Errorf("planner fallbacks: %w", err)
	}
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return nil, err
		}
	}
	return rules, nil
}

// ParseLadder splits a comma-separated list of model aliases.
func ParseLadder(v string) []string {
	var out []string
	for _, a := range strings.Split(v, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
