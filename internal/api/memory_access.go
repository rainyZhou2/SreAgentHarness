package api

import (
	"net/http"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/memory"
)

func withMemoryAccess(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := auth.FromContext(r.Context())
		if !ok {
			writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
			return
		}
		subjects := []string{"user:" + principal.Subject}
		seen := map[string]bool{subjects[0]: true}
		for _, role := range principal.Roles {
			subject := "role:" + string(role)
			if !seen[subject] {
				seen[subject] = true
				subjects = append(subjects, subject)
			}
		}
		if principal.Has(auth.Viewer) && !seen["role:viewer"] {
			subjects = append(subjects, "role:viewer")
		}
		if principal.Has(auth.Admin) && !seen["role:admin"] {
			subjects = append(subjects, "role:admin")
		}
		ctx := memory.WithAccessContext(r.Context(), memory.AccessContext{TenantID: principal.Tenant, Subjects: subjects})
		next(w, r.WithContext(ctx))
	}
}
