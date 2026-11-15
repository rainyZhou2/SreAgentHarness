package memory

import "context"

func (r *Retriever) withRunAccess(ctx context.Context, tenantID, runID string) context.Context {
	if _, ok := AccessFrom(ctx); ok {
		return ctx
	}
	subjects := []string{"role:viewer"}
	if runID != "" {
		subjects = append(subjects, "service:run:"+runID)
		if store, ok := r.Store.(PersonalStore); ok {
			if user, err := store.RunInitiator(ctx, tenantID, runID); err == nil && user != "" {
				subjects = append(subjects, "user:"+user)
			}
		}
	}
	return WithAccessContext(ctx, AccessContext{TenantID: tenantID, Subjects: subjects})
}
