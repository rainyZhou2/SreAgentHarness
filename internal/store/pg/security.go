package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/auth"
)

// ---------- security audit (§19.3) ----------

type AuditEntry struct {
	TenantID     string         `json:"tenant_id"`
	ActorType    string         `json:"actor_type"` // user | service | system
	ActorID      string         `json:"actor_id"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type"`
	ResourceID   string         `json:"resource_id,omitempty"`
	Outcome      string         `json:"outcome"` // allowed | denied | applied | rejected
	Details      map[string]any `json:"details,omitempty"`
	TraceID      string         `json:"trace_id,omitempty"`
	OccurredAt   time.Time      `json:"occurred_at"`
}

func (s *Store) Audit(ctx context.Context, e AuditEntry) error {
	var rid, trace *string
	if e.ResourceID != "" {
		rid = &e.ResourceID
	}
	if e.TraceID != "" {
		trace = &e.TraceID
	}
	if e.Details == nil {
		e.Details = map[string]any{}
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action,
		resource_type, resource_id, outcome, details, trace_id) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		newID(), e.TenantID, e.ActorType, e.ActorID, e.Action, e.ResourceType, rid, e.Outcome, jsonb(e.Details), trace)
	return err
}

func (s *Store) ListAudit(ctx context.Context, tenantID string, limit int) ([]AuditEntry, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, actor_type, actor_id, action, resource_type, COALESCE(resource_id,''),
		outcome, details, COALESCE(trace_id,''), occurred_at FROM security_audit_log WHERE tenant_id=$1
		ORDER BY occurred_at DESC LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		if err := rows.Scan(&e.TenantID, &e.ActorType, &e.ActorID, &e.Action, &e.ResourceType, &e.ResourceID,
			&e.Outcome, &e.Details, &e.TraceID, &e.OccurredAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------- role bindings ----------

var _ auth.RoleSource = (*Store)(nil)

func (s *Store) RolesFor(ctx context.Context, tenantID, principalID string) ([]auth.Role, error) {
	rows, err := s.pool.Query(ctx, `SELECT role FROM role_bindings WHERE tenant_id=$1 AND principal_id=$2`, tenantID, principalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []auth.Role
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		out = append(out, auth.Role(r))
	}
	return out, rows.Err()
}

func (s *Store) BindRole(ctx context.Context, tenantID, principalID string, role auth.Role, actor string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO role_bindings (tenant_id, principal_id, role, created_by) VALUES ($1,$2,$3,$4)
		ON CONFLICT DO NOTHING`, tenantID, principalID, string(role), actor)
	return err
}

func (s *Store) UnbindRole(ctx context.Context, tenantID, principalID string, role auth.Role) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM role_bindings WHERE tenant_id=$1 AND principal_id=$2 AND role=$3`,
		tenantID, principalID, string(role))
	return err
}

// ---------- row level security (§13.7) ----------

type tenantKey struct{}

// WithTenant scopes every query made with ctx to one tenant: the pool hook
// sets app.tenant_id on the connection, and the RLS policies hide other
// tenants' rows. Background roles (workers, reconciler) leave it unset.
// RLS only binds roles without BYPASSRLS (i.e. not superusers).
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenantID)
}

// ConfigureRLS installs the pool hooks that apply WithTenant.
func ConfigureRLS(cfg *pgxpool.Config) {
	cfg.PrepareConn = func(ctx context.Context, c *pgx.Conn) (bool, error) {
		t, _ := ctx.Value(tenantKey{}).(string)
		_, err := c.Exec(ctx, `SELECT set_config('app.tenant_id', $1, false)`, t)
		return err == nil, nil
	}
	cfg.AfterRelease = func(c *pgx.Conn) bool {
		_, err := c.Exec(context.Background(), `SELECT set_config('app.tenant_id', '', false)`)
		return err == nil
	}
}
