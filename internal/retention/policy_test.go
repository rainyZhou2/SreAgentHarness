package retention

import (
	"testing"
	"time"
)

func TestArtifactDeletionPolicy(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	base := RetentionInput{Now: now, RetainUntil: []time.Time{past}}
	if !CanDeleteArtifact(base) {
		t.Fatal("expired unreferenced artifact should be eligible")
	}
	withReference := base
	withReference.References = 1
	if CanDeleteArtifact(withReference) {
		t.Fatal("business reference must block deletion")
	}
	withFuture := base
	withFuture.RetainUntil = append(withFuture.RetainUntil, future)
	if CanDeleteArtifact(withFuture) {
		t.Fatal("latest valid retain_until must win")
	}
	withHold := withFuture
	withHold.Holds = []Hold{{ExpiresAt: &past}}
	if CanDeleteArtifact(withHold) {
		t.Fatal("unexpired retention policy still blocks when hold is expired")
	}
	withHold.Holds = append(withHold.Holds, Hold{})
	if CanDeleteArtifact(withHold) {
		t.Fatal("indefinite legal hold must block deletion")
	}
}

func TestArtifactDeletionPolicyRejectsInvalidInput(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	for _, in := range []RetentionInput{{}, {Now: now, References: -1}} {
		if CanDeleteArtifact(in) {
			t.Fatalf("invalid retention input must fail closed: %+v", in)
		}
	}
}

func TestArtifactDeletionPolicyHonorsEveryIndependentHold(t *testing.T) {
	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	released := now.Add(-time.Minute)
	in := RetentionInput{Now: now, Holds: []Hold{{ReleasedAt: &released}, {}}}
	if CanDeleteArtifact(in) {
		t.Fatal("releasing one hold must not release another")
	}
	in.Holds[1].ReleasedAt = &released
	if !CanDeleteArtifact(in) {
		t.Fatal("released holds must not block deletion")
	}
}
