package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/reasoning"
)

var _ eval.Store = (*Store)(nil)

// ---------- profiles & channels ----------

func (s *Store) ProfileSpec(ctx context.Context, tenantID, profileID string, version int64) (json.RawMessage, error) {
	var spec []byte
	err := s.pool.QueryRow(ctx, `SELECT spec FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`,
		tenantID, profileID, version).Scan(&spec)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	return spec, err
}

// CreateProfileVersion appends an immutable version (max+1).
func (s *Store) CreateProfileVersion(ctx context.Context, tenantID, profileID string, spec json.RawMessage, status string) (int64, error) {
	var v int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, tenantID+"|profile|"+profileID); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version),0)+1 FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2`,
			tenantID, profileID).Scan(&v); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO agent_profiles (tenant_id, profile_id, version, spec, spec_hash, status)
			VALUES ($1,$2,$3,$4,$5,$6)`, tenantID, profileID, v, jsonb(spec), hashStrings(string(spec)), status)
		return err
	})
	return v, err
}

func (s *Store) SetProfileStatus(ctx context.Context, tenantID, profileID string, version int64, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE agent_profiles SET status=$4 WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`,
		tenantID, profileID, version, status)
	return err
}

func (s *Store) SetActiveVersion(ctx context.Context, tenantID, profileID string, version int64) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO profile_channels (tenant_id, profile_id, active_version) VALUES ($1,$2,$3)
		ON CONFLICT (tenant_id, profile_id) DO UPDATE SET active_version=EXCLUDED.active_version, updated_at=now()`,
		tenantID, profileID, version)
	return err
}

// ActiveVersion is the channel version, else the newest ACTIVE version.
func (s *Store) ActiveVersion(ctx context.Context, tenantID, profileID string) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(
			(SELECT active_version FROM profile_channels WHERE tenant_id=$1 AND profile_id=$2),
			(SELECT MAX(version) FROM agent_profiles WHERE tenant_id=$1 AND profile_id=$2 AND status='ACTIVE'), 0)`,
		tenantID, profileID).Scan(&v)
	if err == nil && v == 0 {
		return 0, domain.ErrNotFound
	}
	return v, err
}

type ProfileVersion struct {
	ProfileID string          `json:"profile_id"`
	Version   int64           `json:"version"`
	Status    string          `json:"status"`
	SpecHash  string          `json:"spec_hash"`
	Spec      json.RawMessage `json:"spec"`
	CreatedAt time.Time       `json:"created_at"`
}

func (s *Store) ListProfileVersions(ctx context.Context, tenantID, profileID string) ([]ProfileVersion, error) {
	rows, err := s.pool.Query(ctx, `SELECT profile_id, version, status, spec_hash, spec, created_at FROM agent_profiles
		WHERE tenant_id=$1 AND profile_id=$2 ORDER BY version`, tenantID, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ProfileVersion
	for rows.Next() {
		var p ProfileVersion
		if err := rows.Scan(&p.ProfileID, &p.Version, &p.Status, &p.SpecHash, &p.Spec, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// EvalRunCost adapts the run cost projection for plan datasets without making
// the eval package depend on this store.
func (s *Store) EvalRunCost(ctx context.Context, tenantID, runID string) (eval.CostSummary, error) {
	c, err := s.RunCost(ctx, tenantID, runID)
	if err != nil {
		return eval.CostSummary{}, err
	}
	return eval.CostSummary{ModelRequests: int(c.Requests), InputTokens: c.InputTokens,
		OutputTokens: c.OutputTokens, CachedInputTokens: c.CachedInputTokens,
		CostMicros: c.CostMicros, CostUSD: c.CostUSD}, nil
}

// ---------- datasets ----------

const datasetCols = `id, name, version, status, manifest_hash, created_by, created_at, frozen_at`

func scanDataset(row pgx.Row) (eval.Dataset, error) {
	var d eval.Dataset
	err := row.Scan(&d.ID, &d.Name, &d.Version, &d.Status, &d.ManifestHash, &d.CreatedBy, &d.CreatedAt, &d.FrozenAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, domain.ErrNotFound
	}
	return d, err
}

func (s *Store) CreateDataset(ctx context.Context, tenantID, name, actor string) (eval.Dataset, error) {
	return scanDataset(s.pool.QueryRow(ctx, `INSERT INTO evaluation_datasets (id, tenant_id, name, version, status, created_by)
		VALUES ($1,$2,$3,(SELECT COALESCE(MAX(version),0)+1 FROM evaluation_datasets WHERE tenant_id=$2 AND name=$3),'DRAFT',$4)
		RETURNING `+datasetCols, newID(), tenantID, name, actor))
}

func (s *Store) GetDataset(ctx context.Context, tenantID, id string) (eval.Dataset, error) {
	return scanDataset(s.pool.QueryRow(ctx, `SELECT `+datasetCols+` FROM evaluation_datasets WHERE tenant_id=$1 AND id=$2::uuid`,
		tenantID, id))
}

func (s *Store) ListDatasets(ctx context.Context, tenantID string) ([]eval.Dataset, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+datasetCols+` FROM evaluation_datasets WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eval.Dataset
	for rows.Next() {
		d, err := scanDataset(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// AddCase adds a case to a DRAFT dataset; idempotent on case_key.
func (s *Store) AddCase(ctx context.Context, tenantID, datasetID string, c eval.Case) (eval.Case, bool, error) {
	ds, err := s.GetDataset(ctx, tenantID, datasetID)
	if err != nil {
		return c, false, err
	}
	if ds.Status != "DRAFT" {
		return c, false, fmt.Errorf("%w: dataset is %s; cases can only be added to DRAFT datasets", eval.ErrInvalid, ds.Status)
	}
	if c.Split == "" {
		c.Split = eval.SplitFor(c.Key)
	}
	if c.RiskLabels == nil {
		c.RiskLabels = []string{}
	}
	if c.AllowedTools == nil {
		c.AllowedTools = []string{}
	}
	id := newID()
	tag, err := s.pool.Exec(ctx, `INSERT INTO evaluation_cases (id, tenant_id, dataset_id, case_key, split, service_id, environment,
		goal, recording_ref, ground_truth, allowed_tools, risk_labels, occurred_at, source_run_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT (dataset_id, case_key) DO NOTHING`,
		id, tenantID, datasetID, c.Key, c.Split, c.ServiceID, c.Environment, c.Goal, c.RecordingRef, jsonb(c.GroundTruth),
		jsonb(c.AllowedTools), jsonb(c.RiskLabels), c.OccurredAt, c.SourceRunID)
	if err != nil {
		return c, false, err
	}
	created := tag.RowsAffected() == 1
	cases, err := s.listCases(ctx, `tenant_id=$1 AND dataset_id=$2::uuid AND case_key=$3`, tenantID, datasetID, c.Key)
	if err != nil || len(cases) == 0 {
		return c, created, err
	}
	return cases[0], created, nil
}

// FreezeDataset makes a dataset immutable and records its manifest hash.
func (s *Store) FreezeDataset(ctx context.Context, tenantID, datasetID string) (eval.Dataset, error) {
	cases, err := s.ListCases(ctx, tenantID, datasetID, "")
	if err != nil {
		return eval.Dataset{}, err
	}
	if len(cases) == 0 {
		return eval.Dataset{}, fmt.Errorf("%w: dataset has no cases", eval.ErrInvalid)
	}
	parts := make([]string, 0, len(cases))
	for _, c := range cases {
		gt, _ := json.Marshal(c.GroundTruth)
		parts = append(parts, c.Key+"|"+c.Split+"|"+c.RecordingRef+"|"+string(gt))
	}
	sort.Strings(parts)
	d, err := scanDataset(s.pool.QueryRow(ctx, `UPDATE evaluation_datasets SET status='FROZEN', frozen_at=now(), manifest_hash=$3
		WHERE tenant_id=$1 AND id=$2::uuid AND status='DRAFT' RETURNING `+datasetCols, tenantID, datasetID,
		hashStrings(strings.Join(parts, "\n"))))
	if errors.Is(err, domain.ErrNotFound) {
		return d, fmt.Errorf("%w: dataset is not DRAFT", eval.ErrInvalid)
	}
	return d, err
}

func (s *Store) ListCases(ctx context.Context, tenantID, datasetID, split string) ([]eval.Case, error) {
	if split == "" {
		return s.listCases(ctx, `tenant_id=$1 AND dataset_id=$2::uuid`, tenantID, datasetID)
	}
	return s.listCases(ctx, `tenant_id=$1 AND dataset_id=$2::uuid AND split=$3`, tenantID, datasetID, split)
}

func (s *Store) listCases(ctx context.Context, where string, args ...any) ([]eval.Case, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, case_key, split, service_id, environment, goal, recording_ref, ground_truth,
		allowed_tools, risk_labels, occurred_at, source_run_id FROM evaluation_cases WHERE `+where+` ORDER BY case_key`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eval.Case
	for rows.Next() {
		var c eval.Case
		var gt, at, rl []byte
		if err := rows.Scan(&c.ID, &c.Key, &c.Split, &c.ServiceID, &c.Environment, &c.Goal, &c.RecordingRef, &gt, &at, &rl,
			&c.OccurredAt, &c.SourceRunID); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(gt, &c.GroundTruth)
		_ = json.Unmarshal(at, &c.AllowedTools)
		_ = json.Unmarshal(rl, &c.RiskLabels)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---------- evaluation runs ----------

func (s *Store) StartEvalRun(ctx context.Context, tenantID, datasetID, profileID string, version int64, split, actor string) (string, error) {
	id := newID()
	_, err := s.pool.Exec(ctx, `INSERT INTO evaluation_runs (id, tenant_id, dataset_id, profile_id, profile_version, split,
		status, created_by) VALUES ($1,$2,$3,$4,$5,$6,'RUNNING',$7)`, id, tenantID, datasetID, profileID, version, split, actor)
	return id, err
}

func (s *Store) FinishEvalRun(ctx context.Context, tenantID, id, status string, metrics any, reportRef string) error {
	var m []byte
	if metrics != nil {
		m = jsonb(metrics)
	}
	var ref *string
	if reportRef != "" {
		ref = &reportRef
	}
	_, err := s.pool.Exec(ctx, `UPDATE evaluation_runs SET status=$3, metrics=$4, report_ref=$5, finished_at=now()
		WHERE tenant_id=$1 AND id=$2::uuid AND status='RUNNING'`, tenantID, id, status, m, ref)
	return err
}

func (s *Store) GetEvalRun(ctx context.Context, tenantID, id string) (eval.EvalRun, error) {
	var r eval.EvalRun
	var m []byte
	err := s.pool.QueryRow(ctx, `SELECT id, dataset_id, profile_id, profile_version, split, status, metrics, report_ref, created_at
		FROM evaluation_runs WHERE tenant_id=$1 AND id=$2::uuid`, tenantID, id).Scan(&r.ID, &r.DatasetID, &r.ProfileID,
		&r.ProfileVersion, &r.Split, &r.Status, &m, &r.ReportRef, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrNotFound
	}
	r.Metrics = m
	return r, err
}

// ---------- evolution candidates ----------

const candidateCols = `id, profile_id, base_version, candidate_version, change_summary, base_evaluation_id::text,
	evaluation_id::text, gate_result, status, proposed_by, approved_by, created_at`

func scanCandidate(row pgx.Row) (eval.Candidate, error) {
	var c eval.Candidate
	var cs, gr []byte
	err := row.Scan(&c.ID, &c.ProfileID, &c.BaseVersion, &c.CandidateVersion, &cs, &c.BaseEvaluationID, &c.EvaluationID,
		&gr, &c.Status, &c.ProposedBy, &c.ApprovedBy, &c.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, domain.ErrNotFound
	}
	c.ChangeSummary, c.GateResult = cs, gr
	return c, err
}

func (s *Store) CreateCandidate(ctx context.Context, tenantID string, c eval.Candidate, specRef string) (eval.Candidate, error) {
	return scanCandidate(s.pool.QueryRow(ctx, `INSERT INTO evolution_candidates (id, tenant_id, profile_id, base_version,
		candidate_version, candidate_spec_ref, change_summary, status, proposed_by) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING `+candidateCols, newID(), tenantID, c.ProfileID, c.BaseVersion, c.CandidateVersion, specRef,
		jsonb(c.ChangeSummary), c.Status, c.ProposedBy))
}

func (s *Store) GetCandidate(ctx context.Context, tenantID, id string) (eval.Candidate, error) {
	return scanCandidate(s.pool.QueryRow(ctx, `SELECT `+candidateCols+` FROM evolution_candidates WHERE tenant_id=$1 AND id=$2::uuid`,
		tenantID, id))
}

func (s *Store) ListCandidates(ctx context.Context, tenantID, profileID string) ([]eval.Candidate, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+candidateCols+` FROM evolution_candidates WHERE tenant_id=$1
		AND ($2='' OR profile_id=$2) ORDER BY created_at DESC`, tenantID, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eval.Candidate
	for rows.Next() {
		c, err := scanCandidate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

var candidateSettable = map[string]bool{"status": true, "base_evaluation_id": true, "evaluation_id": true,
	"gate_result": true, "approved_by": true}

// UpdateCandidate is a compare-and-set on status.
func (s *Store) UpdateCandidate(ctx context.Context, tenantID, id, fromStatus string, set map[string]any) error {
	keys := make([]string, 0, len(set))
	for k := range set {
		if !candidateSettable[k] {
			return fmt.Errorf("candidate column %q not settable", k)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	args := []any{tenantID, id, fromStatus}
	var sets []string
	for _, k := range keys {
		v := set[k]
		if raw, ok := v.([]byte); ok {
			v = json.RawMessage(raw)
		}
		args = append(args, v)
		cast := ""
		if k == "base_evaluation_id" || k == "evaluation_id" {
			cast = "::uuid"
		}
		sets = append(sets, fmt.Sprintf("%s=$%d%s", k, len(args), cast))
	}
	tag, err := s.pool.Exec(ctx, `UPDATE evolution_candidates SET `+strings.Join(sets, ", ")+`
		WHERE tenant_id=$1 AND id=$2::uuid AND status=$3`, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrVersionConflict
	}
	return nil
}

// ---------- rollouts ----------

const rolloutCols = `id, profile_id, from_version, to_version, candidate_id::text, status, traffic_percent::float8,
	guardrails, observations, approved_by, started_at, ended_at`

func scanRollout(row pgx.Row) (eval.Rollout, error) {
	var r eval.Rollout
	var g, o []byte
	err := row.Scan(&r.ID, &r.ProfileID, &r.FromVersion, &r.ToVersion, &r.CandidateID, &r.Status, &r.TrafficPercent,
		&g, &o, &r.ApprovedBy, &r.StartedAt, &r.EndedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, domain.ErrNotFound
	}
	_ = json.Unmarshal(g, &r.Guardrails)
	r.Observations = o
	return r, err
}

func (s *Store) CreateRollout(ctx context.Context, tenantID string, r eval.Rollout) (eval.Rollout, error) {
	out, err := scanRollout(s.pool.QueryRow(ctx, `INSERT INTO evolution_rollouts (id, tenant_id, profile_id, from_version,
		to_version, candidate_id, status, traffic_percent, guardrails, approved_by, started_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()) RETURNING `+rolloutCols, newID(), tenantID, r.ProfileID, r.FromVersion,
		r.ToVersion, r.CandidateID, r.Status, r.TrafficPercent, jsonb(r.Guardrails), r.ApprovedBy))
	if isUniqueViolation(err) {
		return out, fmt.Errorf("%w: profile already has an active rollout", domain.ErrVersionConflict)
	}
	return out, err
}

func (s *Store) GetRollout(ctx context.Context, tenantID, id string) (eval.Rollout, error) {
	return scanRollout(s.pool.QueryRow(ctx, `SELECT `+rolloutCols+` FROM evolution_rollouts WHERE tenant_id=$1 AND id=$2::uuid`,
		tenantID, id))
}

func (s *Store) ActiveRollout(ctx context.Context, tenantID, profileID string) (*eval.Rollout, error) {
	r, err := scanRollout(s.pool.QueryRow(ctx, `SELECT `+rolloutCols+` FROM evolution_rollouts WHERE tenant_id=$1
		AND profile_id=$2 AND status IN ('PENDING_APPROVAL','CANARY')`, tenantID, profileID))
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

func (s *Store) ListRollouts(ctx context.Context, tenantID, profileID string) ([]eval.Rollout, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+rolloutCols+` FROM evolution_rollouts WHERE tenant_id=$1
		AND ($2='' OR profile_id=$2) ORDER BY created_at DESC`, tenantID, profileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eval.Rollout
	for rows.Next() {
		r, err := scanRollout(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) ListCanaryRollouts(ctx context.Context) ([]struct{ TenantID, ID string }, error) {
	rows, err := s.pool.Query(ctx, `SELECT tenant_id, id FROM evolution_rollouts WHERE status='CANARY'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []struct{ TenantID, ID string }
	for rows.Next() {
		var x struct{ TenantID, ID string }
		if err := rows.Scan(&x.TenantID, &x.ID); err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Store) EndRollout(ctx context.Context, tenantID, id string, e eval.RolloutEnd) error {
	obs := map[string]any{"by": e.Actor}
	if e.Observations != nil {
		obs["details"] = e.Observations
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		var status, profileID string
		var from, to int64
		err := tx.QueryRow(ctx, `SELECT status, profile_id, from_version, to_version FROM evolution_rollouts
			WHERE tenant_id=$1 AND id=$2::uuid FOR UPDATE`, tenantID, id).Scan(&status, &profileID, &from, &to)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrNotFound
		}
		if err != nil {
			return err
		}
		allowed := false
		for _, f := range e.From {
			allowed = allowed || f == status
		}
		if !allowed {
			return domain.ErrVersionConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE evolution_rollouts SET status=$3, observations=$4, ended_at=now()
			WHERE tenant_id=$1 AND id=$2::uuid`, tenantID, id, e.To, jsonb(obs)); err != nil {
			return err
		}
		active := int64(0)
		switch {
		case e.Activate:
			active = to
		case e.RestoreIfPromoted && status == "PROMOTED":
			active = from
		}
		if active > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO profile_channels (tenant_id, profile_id, active_version) VALUES ($1,$2,$3)
				ON CONFLICT (tenant_id, profile_id) DO UPDATE SET active_version=EXCLUDED.active_version, updated_at=now()`,
				tenantID, profileID, active); err != nil {
				return err
			}
		}
		if e.ToVersionStatus != "" {
			if _, err := tx.Exec(ctx, `UPDATE agent_profiles SET status=$4 WHERE tenant_id=$1 AND profile_id=$2 AND version=$3`,
				tenantID, profileID, to, e.ToVersionStatus); err != nil {
				return err
			}
		}
		return nil
	})
}

// RolloutStats aggregates finished runs since the canary started: the canary
// arm (rollout_id = id) or the control arm (same profile, no rollout).
func (s *Store) RolloutStats(ctx context.Context, tenantID, rolloutID string, control bool, profileID string, since time.Time) (eval.RolloutStats, error) {
	where := `r.rollout_id = $2::uuid AND $4::text IS NOT NULL`
	if control {
		where = `r.rollout_id IS NULL AND r.profile_id = $4 AND $2::text <> ''`
	}
	var st eval.RolloutStats
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE r.status IN ('COMPLETED','FAILED','CANCELLED')),
			COUNT(*) FILTER (WHERE r.status = 'FAILED'),
			COALESCE((SELECT SUM(COALESCE(u.reconciled_cost_micros, u.estimated_cost_micros))
				FROM model_requests mr JOIN model_request_attempts a ON a.model_request_id = mr.id
				JOIN model_usage_effective u ON u.model_attempt_id = a.id
				WHERE mr.tenant_id = $1 AND mr.run_id IN (SELECT r2.id FROM agent_runs r2 WHERE r2.tenant_id=$1
					AND r2.created_at >= $3 AND `+strings.ReplaceAll(where, "r.", "r2.")+`)), 0)::BIGINT
		FROM agent_runs r WHERE r.tenant_id = $1 AND r.created_at >= $3 AND `+where,
		tenantID, rolloutID, since, profileID).Scan(&st.Finished, &st.Failed, &st.CostMicros)
	if err != nil {
		return st, err
	}
	if st.Finished > 0 {
		st.FailRate = float64(st.Failed) / float64(st.Finished)
		st.CostPerRun = float64(st.CostMicros) / 1e6 / float64(st.Finished)
	}
	rows, err := s.pool.Query(ctx, `SELECT s.metrics FROM reasoning_scores s
		JOIN agent_runs r ON r.tenant_id = s.tenant_id AND r.id = s.run_id
		WHERE r.tenant_id = $1 AND r.created_at >= $3 AND `+where+`
		ORDER BY s.scored_at DESC LIMIT 5000`, tenantID, rolloutID, since, profileID)
	if err != nil {
		return st, err
	}
	raws, err := pgx.CollectRows(rows, pgx.RowTo[[]byte])
	if err != nil {
		return st, err
	}
	st.Reasoning = summarizeMetrics(raws)
	return st, nil
}

func summarizeMetrics(raws [][]byte) *reasoning.Summary {
	if len(raws) == 0 {
		return nil
	}
	ms := make([]reasoning.Metrics, 0, len(raws))
	for _, raw := range raws {
		var m reasoning.Metrics
		if json.Unmarshal(raw, &m) == nil {
			ms = append(ms, m)
		}
	}
	sum := reasoning.Summarize(ms)
	return &sum
}
