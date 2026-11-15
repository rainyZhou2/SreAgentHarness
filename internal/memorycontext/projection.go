package memorycontext

import (
	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/memory"
)

func Ref(m memory.Memory, score float64, verificationID string) cognitive.MemoryRef {
	ref := cognitive.MemoryRef{ID: m.ID, Version: m.Version, Kind: m.Type, Title: m.Title, Content: m.Content,
		Trust: m.Trust, Score: score, Scope: m.Scope, Status: m.Status, Sensitivity: m.Sensitivity,
		VerificationID: verificationID, DecisionRef: m.DecisionRef, OutcomeRef: m.OutcomeRef}
	if m.Environment != nil {
		environment := *m.Environment
		ref.Environment = &environment
	}
	if m.Applicability != nil {
		a := m.Applicability
		ref.Applicability = &cognitive.MemoryApplicability{Kind: a.Kind, Environments: clone(a.Environments),
			Services: clone(a.Services), Teams: clone(a.Teams), SubjectKinds: clone(a.SubjectKinds),
			VersionConstraints: clone(a.VersionConstraints), RiskOfReuse: a.RiskOfReuse}
		if a.ValidFrom != nil {
			validFrom := *a.ValidFrom
			ref.Applicability.ValidFrom = &validFrom
		}
	}
	if m.EmbeddingDescriptor.Valid() {
		d := m.EmbeddingDescriptor
		ref.Embedding = &cognitive.EmbeddingRef{Model: d.Model, Version: d.Version, Dimensions: d.Dimensions}
	}
	return ref
}

func clone(values []string) []string {
	return append([]string(nil), values...)
}
