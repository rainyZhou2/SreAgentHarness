package security

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

func Merge(decisions []PolicyDecision, now time.Time) (PolicyDecision, error) {
	if len(decisions) == 0 {
		return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "no policy decision"}
	}
	out := decisions[0]
	out.Effect = Allow
	out.RequiredApprovals = 0
	out.ExpiresAt = time.Time{}
	out.DecisionID = ""
	out.Reasons = nil
	out.RevocationStamps = nil
	out.Constraints = Constraints{}
	var versions, reasons []string
	var windows, canaries []string
	stamps := map[string]RevocationStamp{}
	for _, d := range decisions {
		if d.Effect != Allow && d.Effect != RequireApproval && d.Effect != Deny {
			return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "invalid policy effect"}
		}
		if d.Effect == RequireApproval && (d.RequiredApprovals < 1 || d.RequiredApprovals > 5) {
			return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "invalid approval requirement"}
		}
		if d.RequiredApprovals < 0 || d.RequiredApprovals > 5 {
			return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "invalid approval count"}
		}
		if d.Effect == Deny {
			out.Effect = Deny
		} else if out.Effect != Deny && d.Effect == RequireApproval {
			out.Effect = RequireApproval
		}
		if d.RequiredApprovals > out.RequiredApprovals {
			out.RequiredApprovals = d.RequiredApprovals
		}
		if !d.ExpiresAt.IsZero() && (out.ExpiresAt.IsZero() || d.ExpiresAt.Before(out.ExpiresAt)) {
			out.ExpiresAt = d.ExpiresAt
		}
		if d.Constraints.NotBefore != nil && (out.Constraints.NotBefore == nil || d.Constraints.NotBefore.After(*out.Constraints.NotBefore)) {
			value := d.Constraints.NotBefore.UTC()
			out.Constraints.NotBefore = &value
		}
		windows = append(windows, d.Constraints.WindowRef)
		canaries = append(canaries, d.Constraints.CanaryRef)
		if d.Constraints.CanaryBucket != nil {
			value := *d.Constraints.CanaryBucket
			out.Constraints.CanaryBucket = &value
		}
		versions = append(versions, d.PolicyVersion)
		for _, reason := range d.Reasons {
			reasons = append(reasons, strings.TrimSpace(reason.Code)+"\x00"+strings.TrimSpace(reason.PolicyRef)+"\x00"+strings.TrimSpace(reason.Message))
		}
		for _, stamp := range d.RevocationStamps {
			key := stamp.Scope + "\x00" + stamp.ScopeID
			if current, ok := stamps[key]; !ok || stamp.Version > current.Version {
				stamps[key] = stamp
			}
		}
		if out.RequestHash == "" {
			out.RequestHash = d.RequestHash
		} else if d.RequestHash != "" && out.RequestHash != d.RequestHash {
			return PolicyDecision{}, &Error{Code: PolicyUnavailable, Message: "conflicting decision request bindings"}
		}
	}
	if out.Effect == Deny {
		out.RequiredApprovals = 0
	}
	sort.Strings(versions)
	sort.Strings(reasons)
	out.PolicyVersion = hashParts(versions...)
	for _, encoded := range reasons {
		parts := strings.SplitN(encoded, "\x00", 3)
		out.Reasons = append(out.Reasons, Reason{Code: parts[0], PolicyRef: parts[1], Message: parts[2]})
	}
	out.Constraints.WindowRef = mergeRefs(windows)
	out.Constraints.CanaryRef = mergeRefs(canaries)
	for _, stamp := range stamps {
		out.RevocationStamps = append(out.RevocationStamps, stamp)
	}
	if out.ExpiresAt.IsZero() {
		out.ExpiresAt = now.Add(DecisionTTL)
	}
	out.DecidedAt = now.UTC()
	out.ExpiresAt = out.ExpiresAt.UTC()
	if !out.ExpiresAt.After(now) || out.ExpiresAt.After(out.DecidedAt.Add(DecisionTTL)) {
		return PolicyDecision{}, &Error{Code: DecisionExpired, Message: "invalid merged decision validity"}
	}
	return out, nil
}

func (d *PolicyDecision) Finalize(now time.Time) (string, error) {
	if d.DecidedAt.IsZero() {
		d.DecidedAt = now.UTC()
	}
	if d.ExpiresAt.IsZero() {
		d.ExpiresAt = d.DecidedAt.Add(DecisionTTL)
	}
	d.DecidedAt = d.DecidedAt.UTC()
	d.ExpiresAt = d.ExpiresAt.UTC()
	if !d.ExpiresAt.After(d.DecidedAt) || d.ExpiresAt.After(d.DecidedAt.Add(DecisionTTL)) {
		return "", &Error{Code: DecisionExpired, Message: "invalid decision validity"}
	}
	sort.Slice(d.Reasons, func(i, j int) bool { return reasonKey(d.Reasons[i]) < reasonKey(d.Reasons[j]) })
	sort.Slice(d.RevocationStamps, func(i, j int) bool { return stampKey(d.RevocationStamps[i]) < stampKey(d.RevocationStamps[j]) })
	d.DecisionID = ""
	encoded, err := json.Marshal(d)
	if err != nil {
		return "", &Error{Code: PolicyUnavailable, Err: err}
	}
	sum := sha256.Sum256(encoded)
	d.DecisionID = "sha256:" + hex.EncodeToString(sum[:])
	return d.DecisionID, nil
}

func hashParts(parts ...string) string {
	sum := sha256.Sum256([]byte(lengthPrefix(parts...)))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func lengthPrefix(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		fmt.Fprintf(&b, "%d:", len(part))
		b.WriteString(part)
	}
	return b.String()
}

func reasonKey(reason Reason) string {
	return lengthPrefix(reason.Code, reason.PolicyRef, reason.Message)
}

func stampKey(stamp RevocationStamp) string {
	return lengthPrefix(stamp.Scope, stamp.ScopeID, fmt.Sprintf("%020d", stamp.Version))
}

func mergeRefs(refs []string) string {
	seen := map[string]bool{}
	var out []string
	for _, ref := range refs {
		if ref != "" && !seen[ref] {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}
