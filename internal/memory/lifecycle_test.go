package memory

import (
	"testing"
	"time"
)

func TestLifecyclePublicationBoundaries(t *testing.T) {
	cases := []struct {
		status, kind string
		want         bool
	}{
		{StatusCandidate, "semantic", false},
		{Verified, "semantic", true},
		{Verified, "episodic", true},
		{Verified, "procedural", false},
		{Approved, "procedural", true},
		{Contested, "semantic", false},
		{Stale, "semantic", false},
		{Revoked, "semantic", false},
		{Superseded, "semantic", false},
	}
	for _, tc := range cases {
		m := Memory{TenantID: "t", Status: tc.status, Type: tc.kind, Trust: "T2"}
		if got, _ := m.Recallable("t", Env{}, time.Now()); got != tc.want {
			t.Errorf("%s/%s: got %v, want %v", tc.status, tc.kind, got, tc.want)
		}
	}
}

func TestTransitionRejectsDirectPublication(t *testing.T) {
	if CanTransition(StatusCandidate, Approved) {
		t.Fatal("candidate must not skip machine verification")
	}
	if !CanTransition(StatusCandidate, Verified) || !CanTransition(Verified, Approved) {
		t.Fatal("verification and approval must be separate transitions")
	}
	if !CanTransition(Contested, Verified) || !CanTransition(Stale, Verified) {
		t.Fatal("suspended memories must revalidate")
	}
	for _, terminal := range []string{Revoked, Superseded} {
		if CanTransition(terminal, Verified) {
			t.Fatalf("terminal state %s must not be reopened", terminal)
		}
	}
}
