package pg

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/extensions"
)

var _ extensions.Store = (*Store)(nil)

const extCols = `tenant_id, kind, name, spec, enabled, version, updated_by, updated_at`

func scanExt(row pgx.Row) (extensions.Record, error) {
	var r extensions.Record
	var kind string
	err := row.Scan(&r.TenantID, &kind, &r.Name, &r.Spec, &r.Enabled, &r.Version, &r.UpdatedBy, &r.UpdatedAt)
	r.Kind = extensions.Kind(kind)
	return r, err
}

// ListExtensions lists a tenant's records ("" = every tenant, for the
// runtime sync of background processes).
func (s *Store) ListExtensions(ctx context.Context, tenantID string) ([]extensions.Record, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+extCols+` FROM tenant_extensions
		WHERE $1 = '' OR tenant_id = $1 ORDER BY tenant_id, kind, name`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []extensions.Record
	for rows.Next() {
		r, err := scanExt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UpsertExtension creates or replaces a record. expectedVersion nil means
// "create or overwrite"; 0 means "must not exist"; n means "must be at n".
func (s *Store) UpsertExtension(ctx context.Context, r extensions.Record, expectedVersion *int64) (extensions.Record, error) {
	var row pgx.Row
	switch {
	case expectedVersion != nil && *expectedVersion == 0:
		row = s.pool.QueryRow(ctx, `INSERT INTO tenant_extensions (tenant_id, kind, name, spec, enabled, updated_by)
			VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING `+extCols,
			r.TenantID, string(r.Kind), r.Name, r.Spec, r.Enabled, r.UpdatedBy)
	case expectedVersion != nil:
		row = s.pool.QueryRow(ctx, `UPDATE tenant_extensions SET spec=$4, enabled=$5, updated_by=$6,
				version=version+1, updated_at=now()
			WHERE tenant_id=$1 AND kind=$2 AND name=$3 AND version=$7 RETURNING `+extCols,
			r.TenantID, string(r.Kind), r.Name, r.Spec, r.Enabled, r.UpdatedBy, *expectedVersion)
	default:
		row = s.pool.QueryRow(ctx, `INSERT INTO tenant_extensions (tenant_id, kind, name, spec, enabled, updated_by)
			VALUES ($1,$2,$3,$4,$5,$6)
			ON CONFLICT (tenant_id, kind, name) DO UPDATE SET spec=EXCLUDED.spec, enabled=EXCLUDED.enabled,
				updated_by=EXCLUDED.updated_by, version=tenant_extensions.version+1, updated_at=now()
			RETURNING `+extCols, r.TenantID, string(r.Kind), r.Name, r.Spec, r.Enabled, r.UpdatedBy)
	}
	out, err := scanExt(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, extensions.ErrConflict
	}
	return out, err
}

func (s *Store) DeleteExtension(ctx context.Context, tenantID string, kind extensions.Kind, name string) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM tenant_extensions WHERE tenant_id=$1 AND kind=$2 AND name=$3`,
		tenantID, string(kind), name)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return extensions.ErrNotFound
	}
	return nil
}
