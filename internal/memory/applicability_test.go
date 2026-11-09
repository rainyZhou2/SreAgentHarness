package memory

import (
	"testing"
	"time"
)

func TestApplies(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	sup := "m2"
	env := Env{Environment: "prod", Services: []string{"checkout"}, SubjectKinds: []string{"deployment"},
		Versions: map[string]string{"kubernetes": "v1.29.4", "driver": "550.54.15"}}
	cases := []struct {
		name string
		m    Memory
		env  Env
		want bool
	}{
		{"unrestricted", Memory{}, env, true},
		{"expired", Memory{ExpiresAt: &past}, env, false},
		{"superseded", Memory{SupersededBy: &sup}, env, false},
		{"not yet valid", Memory{Applicability: &Applicability{ValidFrom: &future}}, env, false},
		{"environment matches", Memory{Applicability: &Applicability{Environments: []string{"PROD"}}}, env, true},
		{"environment differs", Memory{Applicability: &Applicability{Environments: []string{"staging"}}}, env, false},
		{"service differs", Memory{Applicability: &Applicability{Services: []string{"cart"}}}, env, false},
		{"unknown env is lenient", Memory{Applicability: &Applicability{Environments: []string{"staging"}}}, Env{}, true},
		{"unknown env is strict for high risk", Memory{Applicability: &Applicability{Environments: []string{"prod"}, RiskOfReuse: "high"}}, Env{}, false},
		{"version satisfied", Memory{Applicability: &Applicability{VersionConstraints: []string{"kubernetes>=1.28", "driver~550."}}}, env, true},
		{"version too old", Memory{Applicability: &Applicability{VersionConstraints: []string{"kubernetes<1.25"}}}, env, false},
		{"numeric compare", Memory{Applicability: &Applicability{VersionConstraints: []string{"kubernetes>1.9"}}}, env, true},
		{"version unknown lenient", Memory{Applicability: &Applicability{VersionConstraints: []string{"cuda>=12"}}}, env, true},
		{"version unknown strict", Memory{Applicability: &Applicability{VersionConstraints: []string{"cuda>=12"}, RiskOfReuse: "high"}}, env, false},
	}
	for _, c := range cases {
		if got, why := c.m.Applies(c.env, now); got != c.want {
			t.Errorf("%s: got %v (%s)", c.name, got, why)
		}
	}
}

func TestApplicabilityNormalize(t *testing.T) {
	a := Applicability{Kind: "playbook", VersionConstraints: []string{"kubernetes>=1.28"}}
	if err := a.Normalize(); err != nil || a.RiskOfReuse != "medium" {
		t.Fatalf("%v %+v", err, a)
	}
	for _, bad := range []Applicability{{Kind: "secret"}, {RiskOfReuse: "none"}, {VersionConstraints: []string{"k8s >= 1"}}} {
		if bad.Normalize() == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
}

func TestOutcomeBoostAndPromotion(t *testing.T) {
	if OutcomeBoost(Verification{}) != 1 {
		t.Fatal("unverified must be neutral")
	}
	good, bad := OutcomeBoost(Verification{SuccessCount: 5}), OutcomeBoost(Verification{FailureCount: 5})
	if !(good > 1 && good <= 1.3 && bad < 1 && bad >= 0.7) {
		t.Fatalf("boosts good=%v bad=%v", good, bad)
	}
	pb := Memory{CreatedBy: "alice", Applicability: &Applicability{Kind: "playbook"}}
	if CanPromote(pb, "bob") == nil {
		t.Error("promoted without a verified success")
	}
	pb.Verification.SuccessCount = 1
	if CanPromote(pb, "alice") == nil {
		t.Error("creator approved their own playbook")
	}
	if err := CanPromote(pb, "bob"); err != nil {
		t.Errorf("valid promotion refused: %v", err)
	}
	pb.Verification.FailureCount = 2
	if CanPromote(pb, "bob") == nil {
		t.Error("failure majority promoted")
	}
	if CanPromote(Memory{CreatedBy: "alice"}, "alice") != nil {
		t.Error("plain memories keep the existing transition rules")
	}
}
