package security

import (
	"testing"
	"time"
)

func TestMergeDecisions(t *testing.T) {
	now := time.Date(2026, 11, 15, 2, 0, 0, 0, time.UTC)
	a := PolicyDecision{Effect: Allow, RequiredApprovals: 0, DecidedAt: now, ExpiresAt: now.Add(time.Minute)}
	b := PolicyDecision{Effect: RequireApproval, RequiredApprovals: 2, DecidedAt: now, ExpiresAt: now.Add(30 * time.Second)}
	d, err := Merge([]PolicyDecision{a, b}, now)
	if err != nil || d.Effect != RequireApproval || d.RequiredApprovals != 2 || !d.ExpiresAt.Equal(b.ExpiresAt) {
		t.Fatalf("merge: %+v %v", d, err)
	}
	d, err = Merge([]PolicyDecision{d, {Effect: Deny, Reasons: []Reason{{Code: " revoked "}}}}, now)
	if err != nil || d.Effect != Deny || d.Reasons[0].Code != "revoked" {
		t.Fatalf("deny floor: %+v %v", d, err)
	}
	if _, err = Merge([]PolicyDecision{{Effect: "maybe"}}, now); err == nil {
		t.Fatal("unknown effect must fail closed")
	}
}

func TestDecisionIDStable(t *testing.T) {
	d := PolicyDecision{Effect: Allow, PolicyVersion: "v1", Reasons: []Reason{{Code: "b"}, {Code: "a"}}}
	id1, err := d.Finalize(time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	d.Reasons[0], d.Reasons[1] = d.Reasons[1], d.Reasons[0]
	id2, err := d.Finalize(time.Unix(100, 0))
	if err != nil || id1 != id2 {
		t.Fatalf("unstable ids %q %q: %v", id1, id2, err)
	}
}

func TestDecisionValidityBoundaries(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	for _, expires := range []time.Time{now, now.Add(-time.Second), now.Add(DecisionTTL + time.Second)} {
		d := PolicyDecision{Effect: Allow, DecidedAt: now, ExpiresAt: expires}
		if _, err := d.Finalize(now); err == nil {
			t.Fatalf("accepted validity ending at %s", expires)
		}
	}
}
