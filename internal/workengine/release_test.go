package workengine

import (
	"testing"
	"time"
)

func TestGateInputPropagatesReleaseVersion(t *testing.T) {
	in := GateInput(Deployment{ReleaseVersion: "1.2.3"}, Flags{}, ExpectedSchema(), nil, nil, nil, nil, time.Time{})
	if in.ReleaseVersion != "1.2.3" {
		t.Fatalf("release_version = %q, want 1.2.3", in.ReleaseVersion)
	}
}
