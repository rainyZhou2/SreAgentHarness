package pg

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/memory"
)

func memorySubjects(ctx context.Context) []string {
	access, ok := memory.AccessFrom(ctx)
	if !ok {
		return []string{}
	}
	return access.Subjects
}

func memoryACLPredicate(param int) string {
	placeholder := fmt.Sprintf("$%d", param)
	return ` AND ('role:admin' = ANY(` + placeholder + `::text[]) OR EXISTS (
		SELECT 1 FROM agent_memory_acl acl WHERE acl.tenant_id = m.tenant_id AND acl.memory_id = m.id
		AND acl.permission = 'read' AND acl.subject_type || ':' || acl.subject_id = ANY(` + placeholder + `::text[]))) `
}

func insertMemoryACL(ctx context.Context, tx pgx.Tx, m memory.NewMemory, id string) error {
	grant := func(subject string) error {
		kind, value, ok := strings.Cut(subject, ":")
		if !ok || value == "" {
			return nil
		}
		_, err := tx.Exec(ctx, `INSERT INTO agent_memory_acl (tenant_id, memory_id, subject_type, subject_id, created_by)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, m.TenantID, id, kind, value, m.CreatedBy)
		return err
	}
	if m.Scope == "user" {
		if err := grant("user:" + m.ScopeID); err != nil {
			return err
		}
	} else {
		if err := grant("role:viewer"); err != nil {
			return err
		}
		if err := grant("role:service"); err != nil {
			return err
		}
	}
	if m.CreatedBy != "" {
		return grant("user:" + m.CreatedBy)
	}
	return nil
}

func (s *Store) memoryRunContext(ctx context.Context, tenantID, runID string) context.Context {
	if _, ok := memory.AccessFrom(ctx); ok || runID == "" {
		return ctx
	}
	subjects := []string{"role:viewer", "service:run:" + runID}
	if user, err := s.RunInitiator(ctx, tenantID, runID); err == nil && user != "" {
		subjects = append(subjects, "user:"+user)
	}
	return memory.WithAccessContext(ctx, memory.AccessContext{TenantID: tenantID, Subjects: subjects})
}
