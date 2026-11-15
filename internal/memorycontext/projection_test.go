package memorycontext

import (
	"testing"
	"time"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/memory"
)

func TestRefProjectsGovernanceWithoutAuthorizationData(t *testing.T) {
	environment := "production"
	validFrom := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	m := memory.Memory{ID: "m", Version: 4, Scope: "service", ScopeID: "checkout", Type: "procedural",
		Status: memory.Approved, Title: "deploy", Content: "verify first", Trust: "T1", Sensitivity: "internal",
		Environment: &environment, DecisionRef: "decision:1", OutcomeRef: "outcome:1",
		Applicability: &memory.Applicability{Kind: "playbook", Environments: []string{"production"}, Services: []string{"checkout"},
			Teams: []string{"sre"}, SubjectKinds: []string{"service"}, VersionConstraints: []string{"kubernetes>=1.28"},
			ValidFrom: &validFrom, RiskOfReuse: "high", Reviewer: "user:private"},
		EmbeddingDescriptor: memory.EmbeddingDescriptor{Model: "text-embedding", Version: "v3", Dimensions: 1536}}
	ref := Ref(m, 0.75, "verification:1")
	if ref.ID != m.ID || ref.Version != m.Version || ref.Status != memory.Approved || ref.Sensitivity != "internal" ||
		ref.Environment == nil || *ref.Environment != environment || ref.VerificationID != "verification:1" ||
		ref.DecisionRef != "decision:1" || ref.OutcomeRef != "outcome:1" {
		t.Fatalf("governance projection: %#v", ref)
	}
	if ref.Applicability == nil || ref.Applicability.Kind != "playbook" || ref.Applicability.RiskOfReuse != "high" ||
		len(ref.Applicability.VersionConstraints) != 1 {
		t.Fatalf("applicability projection: %#v", ref.Applicability)
	}
	if ref.Embedding == nil || *ref.Embedding != (cognitive.EmbeddingRef{Model: "text-embedding", Version: "v3", Dimensions: 1536}) {
		t.Fatalf("embedding projection: %#v", ref.Embedding)
	}
}
