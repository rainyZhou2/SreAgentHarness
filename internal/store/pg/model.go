package pg

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/stream"
)

// Model ledger (§34). Store implements gateway.Ledger.

var _ gw.Ledger = (*Store)(nil)

func leaseOf(sc gw.Scope) domain.Lease {
	return domain.Lease{TenantID: sc.TenantID, RunID: sc.RunID, WorkerID: sc.WorkerID, FenceToken: sc.FenceToken}
}

// withModelEvent locks the run, verifies the fence, runs fn and appends a run
// event. When the fence is lost, requireFence decides between failing
// (creation) and still recording the ledger row without an event (finish).
func (s *Store) withModelEvent(ctx context.Context, sc gw.Scope, requireFence bool, typ string, payload any,
	fn func(tx pgx.Tx) error) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.lockLeased(ctx, tx, leaseOf(sc))
		if errors.Is(err, domain.ErrLeaseLost) || errors.Is(err, domain.ErrNotFound) {
			if requireFence {
				return &gw.Error{Code: gw.ErrFenceLost, Msg: "step lease no longer held"}
			}
			return fn(tx)
		}
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		step := nullable(sc.StepID)
		att := nullable(sc.StepAttemptID)
		if err := r.event(ctx, typ, "worker", sc.WorkerID, step, att, payload); err != nil {
			return err
		}
		return r.save(ctx)
	})
}

// CreateModelRequest inserts the request only while the caller's fence is
// current (INSERT … SELECT … WHERE fence matches): a stale worker cannot
// create billable requests on behalf of a step it no longer owns.
func (s *Store) CreateModelRequest(ctx context.Context, sc gw.Scope, rec gw.RequestRecord) error {
	return s.withModelEvent(ctx, sc, true, "model.requested", map[string]any{
		"model_request_id": rec.ID, "model_alias": rec.ModelAlias, "route_policy": rec.RoutePolicyID,
		"route_policy_version": rec.RoutePolicyVersion, "request_hash": rec.RequestHash, "manifest_ref": rec.ManifestRef,
	}, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO model_requests (id, tenant_id, run_id, step_id, step_attempt_id, fence_token,
			context_snapshot_id, model_alias, route_policy_id, route_policy_version, request_ref, request_hash, manifest_ref,
			status, max_output_tokens)
			SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'QUEUED',$14
			FROM agent_runs WHERE id=$3 AND tenant_id=$2 AND fence_token=$6 AND lease_owner=$15 AND status='RUNNING'`,
			rec.ID, sc.TenantID, sc.RunID, sc.StepID, sc.StepAttemptID, sc.FenceToken, sc.ContextSnapshotID,
			rec.ModelAlias, rec.RoutePolicyID, rec.RoutePolicyVersion, rec.RequestRef, rec.RequestHash, rec.ManifestRef,
			rec.MaxOutputTokens, sc.WorkerID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return &gw.Error{Code: gw.ErrFenceLost, Msg: "fence token no longer current"}
		}
		return nil
	})
}

func (s *Store) StartModelAttempt(ctx context.Context, sc gw.Scope, rec gw.AttemptRecord) error {
	return s.withModelEvent(ctx, sc, true, "model.attempt_started", map[string]any{
		"model_request_id": rec.ModelRequestID, "attempt_id": rec.ID, "attempt_no": rec.AttemptNo,
		"endpoint": rec.EndpointID, "model": rec.ModelID,
	}, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO model_request_attempts (id, tenant_id, model_request_id, attempt_no,
			provider_id, endpoint_id, resolved_model_id, resolved_model_version, route_reason, status)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'SENT')`, rec.ID, sc.TenantID, rec.ModelRequestID, rec.AttemptNo,
			rec.ProviderID, rec.EndpointID, rec.ModelID, rec.ModelVersion, []byte(nonEmptyJSON(rec.RouteReason))); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE model_requests SET status='INFLIGHT' WHERE id=$1 AND status='QUEUED'`, rec.ModelRequestID)
		return err
	})
}

func nonEmptyJSON(b []byte) []byte {
	if len(b) == 0 {
		return []byte(`{}`)
	}
	return b
}

// AppendUsage appends a usage revision; revisions are never updated in place.
func (s *Store) AppendUsage(ctx context.Context, tenantID string, rec gw.UsageRecord) error {
	var in, out, cached, write *int64
	if rec.Usage != nil {
		in, out, cached, write = &rec.Usage.InputTokens, &rec.Usage.OutputTokens, &rec.Usage.CachedInputTokens, &rec.Usage.CacheWriteTokens
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO model_usage_ledger (id, tenant_id, model_attempt_id, usage_state, revision,
		supersedes_id, input_tokens, output_tokens, cached_input_tokens, cache_write_tokens, pricing_version,
		estimated_cost_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (model_attempt_id, revision) DO NOTHING`,
		rec.ID, tenantID, rec.AttemptID, rec.State, rec.Revision, nullable(rec.SupersedesID), in, out, cached, write,
		rec.PricingVersion, rec.CostMicros)
	return err
}

func (s *Store) FinishModelAttempt(ctx context.Context, sc gw.Scope, rec gw.AttemptResult) error {
	_, err := s.pool.Exec(ctx, `UPDATE model_request_attempts SET status=$2, provider_request_id=$3, response_ref=$4,
		finish_reason=$5, error_code=$6, ttft_ms=$7, ended_at=now() WHERE id=$1 AND tenant_id=$8`,
		rec.ID, rec.Status, nullable(rec.ProviderRequestID), nullable(rec.ResponseRef), nullable(rec.FinishReason),
		nullable(rec.ErrorCode), rec.TTFT.Milliseconds(), sc.TenantID)
	return err
}

func (s *Store) FinishModelRequest(ctx context.Context, sc gw.Scope, rec gw.RequestResult) error {
	typ := "model.completed"
	if rec.Status != "SUCCEEDED" {
		typ = "model.failed"
	}
	return s.withModelEvent(ctx, sc, false, typ, map[string]any{
		"model_request_id": rec.ID, "status": rec.Status, "error_code": rec.ErrorCode, "endpoint": rec.Endpoint,
		"model": rec.Model, "usage": rec.Usage, "cost_micros": rec.CostMicros,
	}, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE model_requests SET status=$2, response_ref=$3, error_code=$4, finished_at=now()
			WHERE id=$1 AND tenant_id=$5`, rec.ID, rec.Status, nullable(rec.ResponseRef), nullable(rec.ErrorCode), sc.TenantID)
		return err
	})
}

// SpentMicros sums the effective (latest revision) cost since t.
func (s *Store) SpentMicros(ctx context.Context, tenantID string, since time.Time) (int64, error) {
	var total int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(COALESCE(reconciled_cost_micros, estimated_cost_micros, 0)),0)::BIGINT
		FROM model_usage_effective u JOIN model_request_attempts a ON a.id = u.model_attempt_id
		WHERE u.tenant_id=$1 AND a.started_at >= $2`, tenantID, since).Scan(&total)
	return total, err
}

// ---------- cost / usage views (P1-A, §36.3) ----------

type ModelRequestView struct {
	ID          string             `json:"id"`
	StepID      string             `json:"step_id"`
	ModelAlias  string             `json:"model_alias"`
	Status      string             `json:"status"`
	ErrorCode   *string            `json:"error_code,omitempty"`
	ManifestRef string             `json:"manifest_ref"`
	RequestHash string             `json:"request_hash"`
	ResponseRef *string            `json:"response_ref,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	FinishedAt  *time.Time         `json:"finished_at,omitempty"`
	Attempts    []ModelAttemptView `json:"attempts"`
}

type ModelAttemptView struct {
	ID           string  `json:"id"`
	AttemptNo    int     `json:"attempt_no"`
	EndpointID   string  `json:"endpoint_id"`
	Model        string  `json:"model"`
	Status       string  `json:"status"`
	FinishReason *string `json:"finish_reason,omitempty"`
	ErrorCode    *string `json:"error_code,omitempty"`
	TTFTms       *int64  `json:"ttft_ms,omitempty"`
	UsageState   *string `json:"usage_state,omitempty"`
	InputTokens  *int64  `json:"input_tokens,omitempty"`
	OutputTokens *int64  `json:"output_tokens,omitempty"`
	CachedTokens *int64  `json:"cached_input_tokens,omitempty"`
	CostMicros   *int64  `json:"cost_micros,omitempty"`
}

func (s *Store) ListModelRequests(ctx context.Context, tenantID, runID string) ([]ModelRequestView, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, step_id, model_alias, status, error_code, manifest_ref, request_hash,
		response_ref, created_at, finished_at FROM model_requests WHERE tenant_id=$1 AND run_id=$2 ORDER BY created_at`,
		tenantID, runID)
	if err != nil {
		return nil, err
	}
	reqs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (ModelRequestView, error) {
		var v ModelRequestView
		err := row.Scan(&v.ID, &v.StepID, &v.ModelAlias, &v.Status, &v.ErrorCode, &v.ManifestRef, &v.RequestHash,
			&v.ResponseRef, &v.CreatedAt, &v.FinishedAt)
		v.Attempts = []ModelAttemptView{}
		return v, err
	})
	if err != nil {
		return nil, err
	}
	idx := map[string]int{}
	for i, r := range reqs {
		idx[r.ID] = i
	}
	rows, err = s.pool.Query(ctx, `SELECT a.model_request_id, a.id, a.attempt_no, a.endpoint_id, a.resolved_model_id,
		a.status, a.finish_reason, a.error_code, a.ttft_ms, u.usage_state, u.input_tokens, u.output_tokens,
		u.cached_input_tokens, COALESCE(u.reconciled_cost_micros, u.estimated_cost_micros)
		FROM model_request_attempts a JOIN model_requests r ON r.id = a.model_request_id
		LEFT JOIN model_usage_effective u ON u.model_attempt_id = a.id
		WHERE r.tenant_id=$1 AND r.run_id=$2 ORDER BY a.model_request_id, a.attempt_no`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var reqID string
		var a ModelAttemptView
		if err := rows.Scan(&reqID, &a.ID, &a.AttemptNo, &a.EndpointID, &a.Model, &a.Status, &a.FinishReason, &a.ErrorCode,
			&a.TTFTms, &a.UsageState, &a.InputTokens, &a.OutputTokens, &a.CachedTokens, &a.CostMicros); err != nil {
			return nil, err
		}
		if i, ok := idx[reqID]; ok {
			reqs[i].Attempts = append(reqs[i].Attempts, a)
		}
	}
	return reqs, rows.Err()
}

type CostSummary struct {
	Requests          int64            `json:"model_requests"`
	Attempts          int64            `json:"attempts"`
	InputTokens       int64            `json:"input_tokens"`
	OutputTokens      int64            `json:"output_tokens"`
	CachedInputTokens int64            `json:"cached_input_tokens"`
	CostMicros        int64            `json:"cost_micros"`
	CostUSD           float64          `json:"cost_usd"`
	UnknownUsage      int64            `json:"attempts_with_unknown_usage"`
	ByModel           map[string]int64 `json:"cost_micros_by_model"`
}

// RunCost aggregates the effective usage of every attempt of a run.
func (s *Store) RunCost(ctx context.Context, tenantID, runID string) (CostSummary, error) {
	return s.costSummary(ctx, `r.tenant_id=$1 AND r.run_id=$2::uuid`, tenantID, runID)
}

// TenantCost aggregates usage since t (the /v1/usage endpoint).
func (s *Store) TenantCost(ctx context.Context, tenantID string, since time.Time) (CostSummary, error) {
	return s.costSummary(ctx, `r.tenant_id=$1 AND a.started_at >= $2::timestamptz`, tenantID, since)
}

func (s *Store) costSummary(ctx context.Context, where string, args ...any) (CostSummary, error) {
	c := CostSummary{ByModel: map[string]int64{}}
	rows, err := s.pool.Query(ctx, `SELECT a.resolved_model_id, COUNT(DISTINCT r.id), COUNT(a.id),
		COALESCE(SUM(u.input_tokens),0)::BIGINT, COALESCE(SUM(u.output_tokens),0)::BIGINT,
		COALESCE(SUM(u.cached_input_tokens),0)::BIGINT,
		COALESCE(SUM(COALESCE(u.reconciled_cost_micros, u.estimated_cost_micros)),0)::BIGINT,
		COUNT(*) FILTER (WHERE u.usage_state='UNKNOWN')
		FROM model_requests r JOIN model_request_attempts a ON a.model_request_id = r.id
		LEFT JOIN model_usage_effective u ON u.model_attempt_id = a.id
		WHERE `+where+` GROUP BY a.resolved_model_id`, args...)
	if err != nil {
		return c, err
	}
	defer rows.Close()
	for rows.Next() {
		var model string
		var reqs, atts, in, out, cached, cost, unknown int64
		if err := rows.Scan(&model, &reqs, &atts, &in, &out, &cached, &cost, &unknown); err != nil {
			return c, err
		}
		c.Requests += reqs
		c.Attempts += atts
		c.InputTokens += in
		c.OutputTokens += out
		c.CachedInputTokens += cached
		c.CostMicros += cost
		c.UnknownUsage += unknown
		c.ByModel[model] += cost
	}
	c.CostUSD = float64(c.CostMicros) / 1e6
	return c, rows.Err()
}

// InsertStreamSegment records a bounded delta segment stored in object storage (§35.5).
func (s *Store) InsertStreamSegment(ctx context.Context, seg stream.Segment) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO model_stream_segments (tenant_id, run_id, model_request_id, segment_no,
		first_delta_seq, last_delta_seq, segment_ref, segment_hash, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (model_request_id, segment_no) DO NOTHING`, seg.TenantID, seg.RunID, seg.ModelRequestID, seg.No,
		seg.FirstSeq, seg.LastSeq, seg.Ref, seg.Hash, seg.ExpiresAt)
	return err
}

type SegmentView struct {
	ModelRequestID string    `json:"model_request_id"`
	No             int       `json:"segment_no"`
	FirstSeq       int64     `json:"first_delta_seq"`
	LastSeq        int64     `json:"last_delta_seq"`
	Ref            string    `json:"segment_ref"`
	ExpiresAt      time.Time `json:"expires_at"`
}

func (s *Store) ListStreamSegments(ctx context.Context, tenantID, runID string) ([]SegmentView, error) {
	rows, err := s.pool.Query(ctx, `SELECT model_request_id, segment_no, first_delta_seq, last_delta_seq, segment_ref, expires_at
		FROM model_stream_segments WHERE tenant_id=$1 AND run_id=$2 ORDER BY created_at, segment_no`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (SegmentView, error) {
		var v SegmentView
		err := row.Scan(&v.ModelRequestID, &v.No, &v.FirstSeq, &v.LastSeq, &v.Ref, &v.ExpiresAt)
		return v, err
	})
}

// RunUsage is the run's model spend for the kernel's run budget.
func (s *Store) RunUsage(ctx context.Context, tenantID, runID string) (domain.RunUsage, error) {
	c, err := s.RunCost(ctx, tenantID, runID)
	if err != nil {
		return domain.RunUsage{}, err
	}
	return domain.RunUsage{Requests: c.Requests, InputTokens: c.InputTokens, OutputTokens: c.OutputTokens,
		CostMicros: c.CostMicros}, nil
}
