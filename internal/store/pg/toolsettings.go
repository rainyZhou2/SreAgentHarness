package pg

import "context"

// DisabledTools returns the tool names the tenant switched off. The set is
// applied on top of the profile allowlist; it can only hide tools, never
// expose one the profile denies.
func (s *Store) DisabledTools(ctx context.Context, tenantID string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx, `SELECT tool_name FROM tenant_tool_settings
		WHERE tenant_id=$1 AND enabled=false`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// SetToolEnabled records the tenant's switch for one tool.
func (s *Store) SetToolEnabled(ctx context.Context, tenantID, name string, enabled bool, actor string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO tenant_tool_settings (tenant_id, tool_name, enabled, updated_by)
		VALUES ($1,$2,$3,$4)
		ON CONFLICT (tenant_id, tool_name) DO UPDATE
		SET enabled=EXCLUDED.enabled, updated_by=EXCLUDED.updated_by, updated_at=now()`,
		tenantID, name, enabled, actor)
	return err
}
