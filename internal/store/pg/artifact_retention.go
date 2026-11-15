package pg

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/retention"
)

const streamSegmentReference = "model_stream_segment"

func validateArtifactReference(ref retention.ArtifactReference) error {
	if ref.TenantID == "" || ref.ReferrerType == "" || ref.ReferrerID == "" || ref.ReferrerVersion < 1 ||
		!retention.ValidArtifactRef(ref.TenantID, ref.ArtifactRef) {
		return &domain.RejectedError{Reason: "invalid_artifact_reference"}
	}
	return nil
}

func ensureArtifactObject(ctx context.Context, tx pgx.Tx, tenantID, ref string) error {
	if !retention.ValidArtifactRef(tenantID, ref) {
		return &domain.RejectedError{Reason: "invalid_artifact_reference"}
	}
	_, err := tx.Exec(ctx, `INSERT INTO artifact_objects (tenant_id,artifact_ref) VALUES ($1,$2) ON CONFLICT DO NOTHING`, tenantID, ref)
	return err
}

func referenceArtifact(ctx context.Context, tx pgx.Tx, ref retention.ArtifactReference) error {
	if err := validateArtifactReference(ref); err != nil {
		return err
	}
	if err := ensureArtifactObject(ctx, tx, ref.TenantID, ref.ArtifactRef); err != nil {
		return err
	}
	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM artifact_objects WHERE tenant_id=$1 AND artifact_ref=$2 FOR UPDATE`,
		ref.TenantID, ref.ArtifactRef).Scan(&status); err != nil {
		return err
	}
	if status != "ACTIVE" {
		return &domain.RejectedError{Reason: "artifact_not_referenceable"}
	}
	_, err := tx.Exec(ctx, `INSERT INTO artifact_references (tenant_id,artifact_ref,referrer_type,referrer_id,referrer_version)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, ref.TenantID, ref.ArtifactRef, ref.ReferrerType,
		ref.ReferrerID, ref.ReferrerVersion)
	return err
}

func (s *Store) ReferenceArtifact(ctx context.Context, ref retention.ArtifactReference) error {
	return s.inTx(ctx, func(tx pgx.Tx) error { return referenceArtifact(ctx, tx, ref) })
}

func (s *Store) ReleaseArtifactReference(ctx context.Context, ref retention.ArtifactReference) error {
	if err := validateArtifactReference(ref); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM artifact_references WHERE tenant_id=$1 AND artifact_ref=$2
		AND referrer_type=$3 AND referrer_id=$4 AND referrer_version=$5`, ref.TenantID, ref.ArtifactRef,
		ref.ReferrerType, ref.ReferrerID, ref.ReferrerVersion)
	return err
}

func (s *Store) PutArtifactHold(ctx context.Context, hold retention.ArtifactHold) error {
	if hold.TenantID == "" || hold.HoldID == "" || hold.Reason == "" || hold.Actor == "" ||
		!retention.ValidArtifactRef(hold.TenantID, hold.ArtifactRef) {
		return &domain.RejectedError{Reason: "invalid_artifact_hold"}
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := ensureArtifactObject(ctx, tx, hold.TenantID, hold.ArtifactRef); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO artifact_holds (tenant_id,artifact_ref,hold_id,reason,created_by,expires_at)
			VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (tenant_id,artifact_ref,hold_id) DO UPDATE
			SET reason=EXCLUDED.reason,expires_at=EXCLUDED.expires_at,released_by=NULL,released_at=NULL`,
			hold.TenantID, hold.ArtifactRef, hold.HoldID, hold.Reason, hold.Actor, hold.ExpiresAt)
		return err
	})
}

func (s *Store) ReleaseArtifactHold(ctx context.Context, tenantID, ref, holdID, actor string, at time.Time) error {
	if tenantID == "" || holdID == "" || actor == "" || at.IsZero() || !retention.ValidArtifactRef(tenantID, ref) {
		return &domain.RejectedError{Reason: "invalid_artifact_hold_release"}
	}
	tag, err := s.pool.Exec(ctx, `UPDATE artifact_holds SET released_by=$4,released_at=$5
		WHERE tenant_id=$1 AND artifact_ref=$2 AND hold_id=$3 AND released_at IS NULL`, tenantID, ref, holdID, actor, at)
	if err == nil && tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return err
}

func (s *Store) PutArtifactRetention(ctx context.Context, tenantID, ref, policyID string, until time.Time) error {
	if tenantID == "" || policyID == "" || until.IsZero() || !retention.ValidArtifactRef(tenantID, ref) {
		return &domain.RejectedError{Reason: "invalid_artifact_retention_policy"}
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := ensureArtifactObject(ctx, tx, tenantID, ref); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO artifact_retention_policies (tenant_id,artifact_ref,policy_id,retain_until)
			VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,artifact_ref,policy_id) DO UPDATE SET retain_until=EXCLUDED.retain_until`,
			tenantID, ref, policyID, until)
		return err
	})
}

func artifactRetentionInput(ctx context.Context, q pgx.Tx, tenantID, ref string, now time.Time) (retention.RetentionInput, error) {
	in := retention.RetentionInput{Now: now, RetainUntil: []time.Time{}, Holds: []retention.Hold{}}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM artifact_references WHERE tenant_id=$1 AND artifact_ref=$2
		AND referrer_type<>$3`, tenantID, ref, streamSegmentReference).Scan(&in.References); err != nil {
		return in, err
	}
	rows, err := q.Query(ctx, `SELECT retain_until FROM artifact_retention_policies WHERE tenant_id=$1 AND artifact_ref=$2
		UNION SELECT expires_at FROM model_stream_segments WHERE tenant_id=$1 AND segment_ref=$2`, tenantID, ref)
	if err != nil {
		return in, err
	}
	defer rows.Close()
	for rows.Next() {
		var until time.Time
		if err := rows.Scan(&until); err != nil {
			return in, err
		}
		in.RetainUntil = append(in.RetainUntil, until)
	}
	if err := rows.Err(); err != nil {
		return in, err
	}
	holdRows, err := q.Query(ctx, `SELECT expires_at,released_at FROM artifact_holds WHERE tenant_id=$1 AND artifact_ref=$2`, tenantID, ref)
	if err != nil {
		return in, err
	}
	defer holdRows.Close()
	for holdRows.Next() {
		var hold retention.Hold
		if err := holdRows.Scan(&hold.ExpiresAt, &hold.ReleasedAt); err != nil {
			return in, err
		}
		in.Holds = append(in.Holds, hold)
	}
	return in, holdRows.Err()
}

func (s *Store) ArtifactRetentionInput(ctx context.Context, tenantID, ref string, now time.Time) (retention.RetentionInput, error) {
	if now.IsZero() || !retention.ValidArtifactRef(tenantID, ref) {
		return retention.RetentionInput{}, &domain.RejectedError{Reason: "invalid_artifact_retention_query"}
	}
	var in retention.RetentionInput
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		in, err = artifactRetentionInput(ctx, tx, tenantID, ref, now)
		return err
	})
	return in, err
}

func (s *Store) ClaimArtifacts(ctx context.Context, now time.Time, limit int, lease time.Duration) ([]retention.ArtifactClaim, error) {
	if now.IsZero() || limit < 1 || lease <= 0 {
		return nil, &domain.RejectedError{Reason: "invalid_artifact_claim"}
	}
	claims := []retention.ArtifactClaim{}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT tenant_id,artifact_ref FROM artifact_objects o
			WHERE status<>'DELETED' AND (next_attempt_at IS NULL OR next_attempt_at<=$1)
			AND (lease_expires_at IS NULL OR lease_expires_at<=$1)
			AND (status='DELETING' OR EXISTS (SELECT 1 FROM artifact_retention_policies p
				WHERE p.tenant_id=o.tenant_id AND p.artifact_ref=o.artifact_ref AND p.retain_until<=$1)
			OR EXISTS (SELECT 1 FROM model_stream_segments seg WHERE seg.tenant_id=o.tenant_id
				AND seg.segment_ref=o.artifact_ref AND seg.expires_at<=$1))
			ORDER BY updated_at LIMIT $2 FOR UPDATE SKIP LOCKED`, now, limit)
		if err != nil {
			return err
		}
		type candidate struct{ tenantID, ref string }
		candidates := []candidate{}
		for rows.Next() {
			var c candidate
			if err := rows.Scan(&c.tenantID, &c.ref); err != nil {
				rows.Close()
				return err
			}
			candidates = append(candidates, c)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for _, c := range candidates {
			in, err := artifactRetentionInput(ctx, tx, c.tenantID, c.ref, now)
			if err != nil {
				return err
			}
			if !retention.CanDeleteArtifact(in) {
				if _, err := tx.Exec(ctx, `UPDATE artifact_objects SET status='ACTIVE',lease_expires_at=NULL,
					next_attempt_at=NULL WHERE tenant_id=$1 AND artifact_ref=$2 AND status='DELETING'`, c.tenantID, c.ref); err != nil {
					return err
				}
				continue
			}
			var token int64
			if err := tx.QueryRow(ctx, `UPDATE artifact_objects SET status='DELETING',lease_token=lease_token+1,
				lease_expires_at=$3,next_attempt_at=NULL,last_error='',updated_at=now()
				WHERE tenant_id=$1 AND artifact_ref=$2 RETURNING lease_token`, c.tenantID, c.ref, now.Add(lease)).Scan(&token); err != nil {
				return err
			}
			claims = append(claims, retention.ArtifactClaim{TenantID: c.tenantID, Ref: c.ref, LeaseToken: token})
		}
		return nil
	})
	return claims, err
}

func (s *Store) ConfirmArtifactDeleted(ctx context.Context, claim retention.ArtifactClaim) error {
	if claim.TenantID == "" || claim.LeaseToken < 1 || !retention.ValidArtifactRef(claim.TenantID, claim.Ref) {
		return &domain.RejectedError{Reason: "invalid_artifact_confirmation"}
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var token int64
		err := tx.QueryRow(ctx, `SELECT lease_token FROM artifact_objects WHERE tenant_id=$1 AND artifact_ref=$2
			AND status='DELETING' FOR UPDATE`, claim.TenantID, claim.Ref).Scan(&token)
		if err != nil {
			return err
		}
		if token != claim.LeaseToken {
			return domain.ErrVersionConflict
		}
		if _, err := tx.Exec(ctx, `DELETE FROM model_stream_segments WHERE tenant_id=$1 AND segment_ref=$2`,
			claim.TenantID, claim.Ref); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM artifact_references WHERE tenant_id=$1 AND artifact_ref=$2`,
			claim.TenantID, claim.Ref); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE artifact_objects SET status='DELETED',lease_expires_at=NULL,next_attempt_at=NULL,
			last_error='',updated_at=now() WHERE tenant_id=$1 AND artifact_ref=$2 AND lease_token=$3 AND status='DELETING'`,
			claim.TenantID, claim.Ref, claim.LeaseToken)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrVersionConflict
		}
		return nil
	})
}

func (s *Store) RetryArtifact(ctx context.Context, claim retention.ArtifactClaim, next time.Time, message string) error {
	if claim.TenantID == "" || claim.LeaseToken < 1 || next.IsZero() || !retention.ValidArtifactRef(claim.TenantID, claim.Ref) {
		return &domain.RejectedError{Reason: "invalid_artifact_retry"}
	}
	if len(message) > 2048 {
		message = message[:2048]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE artifact_objects SET next_attempt_at=$3,lease_expires_at=NULL,last_error=$4,updated_at=now()
		WHERE tenant_id=$1 AND artifact_ref=$2 AND lease_token=$5 AND status='DELETING'`,
		claim.TenantID, claim.Ref, next, message, claim.LeaseToken)
	if err == nil && tag.RowsAffected() != 1 {
		return domain.ErrVersionConflict
	}
	return err
}
