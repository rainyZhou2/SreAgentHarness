package pg

import (
	"context"

	"go-sre-harness/internal/domain"
)

// ListProfiles returns every registered profile version for the mode picker.
func (s *Store) ListProfiles(ctx context.Context, tenantID string) ([]domain.ProfileInfo, error) {
	rows, err := s.pool.Query(ctx, `SELECT profile_id, version, status, COALESCE(spec->>'strategy', '')
		FROM agent_profiles WHERE tenant_id=$1 ORDER BY profile_id, version DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.ProfileInfo
	for rows.Next() {
		var p domain.ProfileInfo
		if err := rows.Scan(&p.ID, &p.Version, &p.Status, &p.Strategy); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
