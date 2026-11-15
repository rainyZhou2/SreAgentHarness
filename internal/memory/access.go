package memory

import "context"

type AccessContext struct {
	TenantID string
	Subjects []string
}

type ACLGrant struct {
	TenantID string `json:"tenant_id"`
	MemoryID string `json:"memory_id"`
	Subject  string `json:"subject"`
}

func (a AccessContext) Allows(memoryID, tenantID string, grants []ACLGrant) bool {
	if a.TenantID == "" || tenantID != a.TenantID {
		return false
	}
	allowed := make(map[string]bool, len(a.Subjects)+1)
	for _, subject := range a.Subjects {
		allowed[subject] = true
	}
	if allowed["role:admin"] {
		return true
	}
	for _, grant := range grants {
		if grant.TenantID == a.TenantID && grant.MemoryID == memoryID && allowed[grant.Subject] {
			return true
		}
	}
	return false
}

func AuthorizeRead(access AccessContext, m Memory, grants []ACLGrant) bool {
	return access.Allows(m.ID, m.TenantID, grants)
}

type accessContextKey struct{}

func WithAccessContext(ctx context.Context, access AccessContext) context.Context {
	return context.WithValue(ctx, accessContextKey{}, access)
}

func AccessFrom(ctx context.Context) (AccessContext, bool) {
	access, ok := ctx.Value(accessContextKey{}).(AccessContext)
	return access, ok && access.TenantID != ""
}
