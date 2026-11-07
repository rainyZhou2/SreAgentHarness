package planner

import (
	"fmt"
	"regexp"
	"strings"
)

// Router decides how an auto run proceeds when the planner could not produce
// a valid plan: the operator's fallback flow, or ReAct. It looks at signals
// in order of authority; the language patterns are configuration, not code.
//
//  1. the goal context hint (set by a trigger or the API caller);
//  2. the mode the model asked for in its last invalid plan;
//  3. the configured ReAct patterns (questions stay conversational);
//  4. a side-effecting tool the goal names (registry metadata);
//  5. the configured flow patterns;
//  6. ReAct.
type Router struct {
	React []*regexp.Regexp
	Flow  []*regexp.Regexp
}

// DefaultReactPatterns and DefaultFlowPatterns are the default values of
// the router configuration (HARNESS_PLANNER_REACT_PATTERNS and
// HARNESS_PLANNER_FLOW_PATTERNS). ReAct patterns are question openings; flow
// patterns are change verbs.
var (
	DefaultReactPatterns = []string{
		`^(why|how|what|when|where|which|who)\b`,
		`^(is|are|can|could|should|does|do)\s`,
		`^(为什么|怎么|如何|是否|能否|什么是)`,
	}
	DefaultFlowPatterns = []string{
		`\b(restart|rollback|redeploy|deploy|scale|failover|remediate|recover|drain|rotate|patch|upgrade|migrate)\b`,
		`(重启|回滚|发布|恢复|修复|扩容|缩容|摘流|降级|轮换|升级|迁移)`,
	}
)

// NewRouter compiles case-insensitive patterns. A single "none" entry
// disables a list.
func NewRouter(react, flow []string) (*Router, error) {
	r := &Router{}
	var err error
	if r.React, err = compilePatterns("react", react); err != nil {
		return nil, err
	}
	if r.Flow, err = compilePatterns("flow", flow); err != nil {
		return nil, err
	}
	return r, nil
}

// DefaultRouter uses the default pattern lists.
func DefaultRouter() *Router {
	r, err := NewRouter(DefaultReactPatterns, DefaultFlowPatterns)
	if err != nil {
		panic(err)
	}
	return r
}

// ParsePatterns splits a configuration value: "" means the defaults, "none"
// disables the list, otherwise patterns are separated by "||".
func ParsePatterns(v string, defaults []string) []string {
	v = strings.TrimSpace(v)
	switch {
	case v == "":
		return defaults
	case strings.EqualFold(v, "none"):
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, "||") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func compilePatterns(kind string, ps []string) ([]*regexp.Regexp, error) {
	if len(ps) == 1 && strings.EqualFold(strings.TrimSpace(ps[0]), "none") {
		return nil, nil
	}
	out := make([]*regexp.Regexp, 0, len(ps))
	for _, p := range ps {
		re, err := regexp.Compile("(?i)" + p)
		if err != nil {
			return nil, fmt.Errorf("planner %s pattern %q: %w", kind, p, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// Route returns "flow" or "react" and the signal that decided.
func (r *Router) Route(in Input, requested string) (string, string) {
	switch in.ModeHint {
	case "flow", "react":
		return in.ModeHint, "goal context hint"
	}
	if requested == "flow" || requested == "react" {
		return requested, "model requested " + requested
	}
	goal := strings.ToLower(strings.TrimSpace(in.Goal))
	for _, re := range r.React {
		if re.MatchString(goal) {
			return "react", "react pattern " + re.String()
		}
	}
	if name := mentionedAction(goal, in); name != "" {
		return "flow", "goal names side-effecting tool " + name
	}
	for _, re := range r.Flow {
		if re.MatchString(goal) {
			return "flow", "flow pattern " + re.String()
		}
	}
	return "react", "no signal"
}

// mentionedAction returns a visible side-effecting tool whose name (or its
// last segment, with separators as spaces) appears in the goal.
func mentionedAction(goal string, in Input) string {
	for _, d := range in.Tools {
		if !d.Risk.HasSideEffects() {
			continue
		}
		for _, form := range nameForms(d.Name) {
			if len([]rune(form)) >= 4 && strings.Contains(goal, form) {
				return d.Name
			}
		}
	}
	return ""
}

func nameForms(name string) []string {
	n := strings.ToLower(name)
	spaced := strings.NewReplacer(".", " ", "_", " ", "-", " ").Replace(n)
	forms := []string{n, spaced}
	if i := strings.LastIndex(n, "."); i >= 0 && i < len(n)-1 {
		last := n[i+1:]
		forms = append(forms, last, strings.NewReplacer("_", " ", "-", " ").Replace(last))
	}
	return forms
}
