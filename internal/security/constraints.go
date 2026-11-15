package security

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

type ConstraintRule struct {
	Name          string      `json:"name"`
	Version       string      `json:"version"`
	Operations    []Operation `json:"operations,omitempty"`
	Environments  []string    `json:"environments,omitempty"`
	Resources     []string    `json:"resources,omitempty"`
	Windows       []Window    `json:"windows,omitempty"`
	CanaryPercent *int        `json:"canary_percent,omitempty"`
}

type compiledRule struct {
	ConstraintRule
}

type CompiledConstraints struct {
	rules []compiledRule
}

func CompileConstraints(in []ConstraintRule) (*CompiledConstraints, error) {
	out := &CompiledConstraints{}
	seen := map[string]bool{}
	for _, rule := range in {
		invalidCanary := rule.CanaryPercent != nil && (*rule.CanaryPercent < 0 || *rule.CanaryPercent > 100)
		if rule.Name == "" || rule.Version == "" || seen[rule.Name+"\x00"+rule.Version] || invalidCanary {
			return nil, &Error{Code: InvalidRequest, Message: "invalid constraint rule"}
		}
		seen[rule.Name+"\x00"+rule.Version] = true
		rule.Operations = append([]Operation(nil), rule.Operations...)
		rule.Environments = append([]string(nil), rule.Environments...)
		rule.Resources = append([]string(nil), rule.Resources...)
		rule.Windows = append([]Window(nil), rule.Windows...)
		if rule.CanaryPercent != nil {
			value := *rule.CanaryPercent
			rule.CanaryPercent = &value
		}
		for i := range rule.Windows {
			if err := rule.Windows[i].Compile(); err != nil {
				return nil, &Error{Code: InvalidRequest, Message: "invalid change window", Err: err}
			}
		}
		out.rules = append(out.rules, compiledRule{ConstraintRule: rule})
	}
	return out, nil
}

func (c *CompiledConstraints) Evaluate(r Request, now time.Time) (PolicyDecision, error) {
	d := PolicyDecision{Effect: Allow, PolicyVersion: "constraints-v1"}
	for _, rule := range c.rules {
		if !rule.matches(r) {
			continue
		}
		for i := range rule.Windows {
			if !rule.Windows[i].Contains(now) {
				return PolicyDecision{Effect: Deny, PolicyVersion: rule.Version, Reasons: []Reason{{
					Code: "outside_change_window", PolicyRef: rule.Name, Message: "operation is outside its change window",
				}}}, nil
			}
		}
		if rule.CanaryPercent != nil && *rule.CanaryPercent < 100 {
			bucket := canaryBucket(rule.Version, r.TenantID, r.WorkID, r.ActionID, r.ResourceRef)
			d.Constraints.CanaryRef = rule.Name + "/" + rule.Version
			d.Constraints.CanaryBucket = &bucket
			if bucket >= uint32(*rule.CanaryPercent) {
				return PolicyDecision{Effect: Deny, PolicyVersion: rule.Version, Reasons: []Reason{{
					Code: "outside_canary", PolicyRef: rule.Name, Message: "operation is outside its canary cohort",
				}}}, nil
			}
		}
	}
	return d, nil
}

func (r compiledRule) matches(req Request) bool {
	return matchOperation(r.Operations, req.Operation) && matchString(r.Environments, req.Environment) && matchString(r.Resources, req.ResourceRef)
}

func matchOperation(values []Operation, value Operation) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func matchString(values []string, value string) bool {
	if len(values) == 0 {
		return true
	}
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func canaryBucket(parts ...string) uint32 {
	h := sha256.New()
	for _, part := range parts {
		fmt.Fprintf(h, "%d:", len(part))
		h.Write([]byte(part))
	}
	return binary.BigEndian.Uint32(h.Sum(nil)[:4]) % 100
}
