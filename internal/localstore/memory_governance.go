package localstore

import (
	"context"

	"go-sre-harness/internal/memory"
)

func initialMemoryACL(m memory.NewMemory, id string) []memory.ACLGrant {
	grants := make([]memory.ACLGrant, 0, 2)
	seen := map[string]bool{}
	add := func(subject string) {
		if subject == "" || seen[subject] {
			return
		}
		seen[subject] = true
		grants = append(grants, memory.ACLGrant{TenantID: m.TenantID, MemoryID: id, Subject: subject})
	}
	if m.Scope == "user" {
		add("user:" + m.ScopeID)
	} else {
		add("role:viewer")
		add("role:service")
	}
	if m.CreatedBy != "" {
		add("user:" + m.CreatedBy)
	}
	return grants
}

func (s *Store) memoryReadableLocked(ctx context.Context, rec memoryRec) bool {
	access, ok := memory.AccessFrom(ctx)
	return ok && memory.AuthorizeRead(access, rec.Mem, rec.ACL)
}

func runMemoryAccess(ctx context.Context, tenantID, runID, userID string) context.Context {
	subjects := []string{"role:viewer", "service:run:" + runID}
	if access, ok := memory.AccessFrom(ctx); ok {
		subjects = append(subjects, access.Subjects...)
	}
	if userID != "" {
		subjects = append(subjects, "user:"+userID)
	}
	return memory.WithAccessContext(ctx, memory.AccessContext{TenantID: tenantID, Subjects: subjects})
}
