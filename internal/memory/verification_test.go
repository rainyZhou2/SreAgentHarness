package memory

import "testing"

func validVerificationInput() VerificationInput {
	return VerificationInput{TenantID: "t", MemoryID: "m", ExpectedVersion: 2,
		EvidenceRefs: []string{"evidence:1"}, DecisionRef: "decision:1", OutcomeRef: "outcome:1",
		DecisionVerdict: "CONFIRMED", OutcomeVerdict: "SATISFIED", InputHash: "sha256:abc", ActualHash: "sha256:abc"}
}

func TestVerificationRequiresCurrentSatisfiedEvidence(t *testing.T) {
	base := validVerificationInput()
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*VerificationInput){
		func(v *VerificationInput) { v.DecisionVerdict = "PROBABLE" },
		func(v *VerificationInput) { v.OutcomeVerdict = "UNSATISFIED" },
		func(v *VerificationInput) { v.ActualHash = "sha256:changed" },
		func(v *VerificationInput) { v.EvidenceRefs = nil },
	} {
		v := base
		mutate(&v)
		if err := v.Validate(); err == nil {
			t.Fatal("invalid verification evidence must fail closed")
		}
	}
}

func TestVerificationHashBindsVersionAndEvidence(t *testing.T) {
	m := Memory{ID: "m", TenantID: "t", Version: 2, ContentHash: "content", EvidenceRefs: []string{"e1"}}
	input := VerificationInputFor(m, "d1", "o1")
	if input.InputHash == "" || input.ExpectedVersion != 2 || input.DecisionRef != "d1" || input.OutcomeRef != "o1" {
		t.Fatalf("input=%+v", input)
	}
	m.Version++
	if VerificationInputFor(m, "d1", "o1").InputHash == input.InputHash {
		t.Fatal("version must change verification hash")
	}
}
