package governance

import "testing"

func TestAttestationReportsWholeReleaseVersion(t *testing.T) {
	in := prodInput()
	in.ReleaseVersion = "1.2.3"
	if got := Evaluate(in).ReleaseVersion; got != "1.2.3" {
		t.Fatalf("release_version = %q, want 1.2.3", got)
	}
	in.ReleaseVersion = ""
	if got := Evaluate(in).ReleaseVersion; got != "dev" {
		t.Fatalf("empty release_version = %q, want dev", got)
	}
}
