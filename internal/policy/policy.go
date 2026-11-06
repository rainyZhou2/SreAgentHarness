// Package policy is the non-bypassable gate between a model's tool intent and
// execution (§4.6, §10.2 step 6). Prompts cannot change its outcome.
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

type Verdict string

const (
	Allow           Verdict = "allow"
	RequireApproval Verdict = "require_approval"
	Deny            Verdict = "deny"
)

type Decision struct {
	Verdict           Verdict          `json:"verdict"`
	Reason            string           `json:"reason"`
	Risk              domain.RiskClass `json:"risk"`
	RequiredApprovals int              `json:"required_approvals"`
	PolicyVersion     string           `json:"policy_version"`
}

type Engine struct {
	Version     string
	Environment string // MVP: one environment per deployment; default "production"
	ApprovalTTL time.Duration
	// AllowR1WithoutApproval lets low-risk reversible changes run outside production.
	AllowR1WithoutApproval bool
}

func Default() Engine {
	return Engine{Version: "policy-v1", Environment: "production", ApprovalTTL: 30 * time.Minute}
}

// Evaluate decides from the registry descriptor; unknown tools are denied
// (default deny for unknown side effects).
func (e Engine) Evaluate(desc *tools.Descriptor, target string) Decision {
	d := Decision{PolicyVersion: e.Version}
	if desc == nil {
		d.Verdict, d.Reason = Deny, "tool is not registered"
		return d
	}
	d.Risk = desc.Risk
	if desc.TargetArg != "" && target == "" {
		d.Verdict, d.Reason = Deny, "tool target is missing"
		return d
	}
	switch desc.Risk {
	case domain.RiskR0:
		d.Verdict, d.Reason = Allow, "read-only"
	case domain.RiskR1:
		if e.AllowR1WithoutApproval && e.Environment != "production" {
			d.Verdict, d.Reason = Allow, "low-risk change outside production"
		} else {
			d.Verdict, d.Reason, d.RequiredApprovals = RequireApproval, "low-risk change requires approval", 1
		}
	case domain.RiskR2:
		d.Verdict, d.Reason, d.RequiredApprovals = RequireApproval, "production write requires approval", 1
	default:
		d.Verdict, d.Reason = Deny, "destructive actions are disabled by framework policy"
	}
	return d
}

// BuildIntent computes the ActionIntent and its hash. Everything an approval
// should bind to is inside the hash, so any change invalidates the approval.
func (e Engine) BuildIntent(desc tools.Descriptor, call domain.ToolCall, inputHash string, d Decision) domain.ActionIntent {
	target := tools.Target(desc, call.Args)
	effects, _ := json.Marshal(map[string]any{"side_effects": desc.Risk.HasSideEffects()})
	intent := domain.ActionIntent{
		Environment: e.Environment, Target: target, ToolName: desc.Name, ToolVersion: desc.Version,
		InputHash: inputHash, RiskClass: desc.Risk, PolicyVersion: d.PolicyVersion, Effects: effects,
	}
	canon, _ := json.Marshal([]string{intent.Environment, intent.Target, intent.ToolName, intent.ToolVersion,
		intent.InputHash, string(intent.RiskClass), intent.PolicyVersion, string(effects)})
	sum := sha256.Sum256(canon)
	intent.ActionHash = "sha256:" + hex.EncodeToString(sum[:])
	return intent
}
