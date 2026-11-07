package pg

import (
	"context"
	"time"

	"go-sre-harness/internal/domain"
)

// ClaimTrigger takes or joins the deduplication claim for key. A live claim
// (not yet expired) is returned with Fresh=false; otherwise a new claim is
// created atomically. extend pushes the expiry of a live claim forward.
func (s *Store) ClaimTrigger(ctx context.Context, tenantID, key string, window time.Duration, extend bool) (domain.TriggerClaim, error) {
	claim := newID()
	var out domain.TriggerClaim
	var runID *string
	err := s.pool.QueryRow(ctx, `INSERT INTO trigger_dedup (tenant_id, dedup_key, claim_id, expires_at)
		VALUES ($1, $2, $3, now() + ($4 * interval '1 second'))
		ON CONFLICT (tenant_id, dedup_key) DO UPDATE SET
		  claim_id   = CASE WHEN trigger_dedup.expires_at <= now() THEN EXCLUDED.claim_id ELSE trigger_dedup.claim_id END,
		  run_id     = CASE WHEN trigger_dedup.expires_at <= now() THEN NULL ELSE trigger_dedup.run_id END,
		  hits       = CASE WHEN trigger_dedup.expires_at <= now() THEN 1 ELSE trigger_dedup.hits + 1 END,
		  first_seen = CASE WHEN trigger_dedup.expires_at <= now() THEN now() ELSE trigger_dedup.first_seen END,
		  last_seen  = now(),
		  expires_at = CASE WHEN trigger_dedup.expires_at <= now() OR $5 THEN EXCLUDED.expires_at
		                    ELSE trigger_dedup.expires_at END
		RETURNING claim_id::text, run_id::text, hits, expires_at`,
		tenantID, key, claim, window.Seconds(), extend).Scan(&out.ID, &runID, &out.Hits, &out.ExpiresAt)
	if err != nil {
		return out, err
	}
	out.Fresh = out.ID == claim
	if runID != nil {
		out.RunID = *runID
	}
	return out, nil
}

// BindTrigger records the run that handles a claim.
func (s *Store) BindTrigger(ctx context.Context, tenantID, key, claimID, runID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE trigger_dedup SET run_id=$4
		WHERE tenant_id=$1 AND dedup_key=$2 AND claim_id=$3`, tenantID, key, claimID, runID)
	return err
}

// ReleaseTrigger drops a claim (its run ended), so the next event for the
// key starts a new run.
func (s *Store) ReleaseTrigger(ctx context.Context, tenantID, key, claimID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM trigger_dedup WHERE tenant_id=$1 AND dedup_key=$2 AND claim_id=$3`,
		tenantID, key, claimID)
	return err
}

// PurgeTriggerClaims deletes claims that expired before the given time.
func (s *Store) PurgeTriggerClaims(ctx context.Context, before time.Time, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM trigger_dedup WHERE ctid IN (
		SELECT ctid FROM trigger_dedup WHERE expires_at < $1 LIMIT $2)`, before, limit)
	return tag.RowsAffected(), err
}

// CountActions counts planned action intents of a tool (on one target, or
// every target when target is empty) since the given time. Rate-based
// policy rules read it.
func (s *Store) CountActions(ctx context.Context, tenantID, tool, target string, since time.Time) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT count(*) FROM action_intents
		WHERE tenant_id=$1 AND tool_name=$2 AND ($3 = '' OR target=$3) AND created_at >= $4`,
		tenantID, tool, target, since).Scan(&n)
	return n, err
}
