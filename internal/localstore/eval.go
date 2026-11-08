package localstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eval"
	"go-sre-harness/internal/reasoning"
)

// Evaluation and evolution state. The local runtime keeps the same lifecycle
// and compare-and-set semantics as the server so plan replay and profile
// evaluation behave identically offline.

type evalDatasetRec struct {
	TenantID string       `json:"tenant_id"`
	Dataset  eval.Dataset `json:"dataset"`
}

type evalCaseRec struct {
	TenantID  string    `json:"tenant_id"`
	DatasetID string    `json:"dataset_id"`
	Case      eval.Case `json:"case"`
}

type evalRunRec struct {
	TenantID string       `json:"tenant_id"`
	Run      eval.EvalRun `json:"run"`
}

type evalCandidateRec struct {
	TenantID  string         `json:"tenant_id"`
	Candidate eval.Candidate `json:"candidate"`
}

type evalRolloutRec struct {
	TenantID string       `json:"tenant_id"`
	Rollout  eval.Rollout `json:"rollout"`
}

func hashBytes(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func (s *Store) saveDataset(rec evalDatasetRec) {
	s.datasets[rec.Dataset.ID] = rec
	s.put("evaldataset", rec.Dataset.ID, rec.TenantID, 0, rec)
}

func (s *Store) saveEvalCase(rec evalCaseRec) {
	s.evalCases[rec.Case.ID] = rec
	s.put("evalcase", rec.Case.ID, rec.DatasetID, 0, rec)
}

func (s *Store) saveEvalRun(rec evalRunRec) {
	s.evalRuns[rec.Run.ID] = rec
	s.put("evalrun", rec.Run.ID, rec.TenantID, 0, rec)
}

func (s *Store) saveCandidate(rec evalCandidateRec) {
	s.candidates[rec.Candidate.ID] = rec
	s.put("evalcandidate", rec.Candidate.ID, rec.TenantID, 0, rec)
}

func (s *Store) saveRollout(rec evalRolloutRec) {
	s.rollouts[rec.Rollout.ID] = rec
	s.put("evalrollout", rec.Rollout.ID, rec.TenantID, 0, rec)
}

func (s *Store) CreateDataset(ctx context.Context, tenantID, name, actor string) (eval.Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var version int64
	for _, rec := range s.datasets {
		if rec.TenantID == tenantID && rec.Dataset.Name == name && rec.Dataset.Version > version {
			version = rec.Dataset.Version
		}
	}
	d := eval.Dataset{ID: newID(), Name: name, Version: version + 1, Status: "DRAFT", CreatedBy: actor, CreatedAt: now()}
	s.saveDataset(evalDatasetRec{TenantID: tenantID, Dataset: d})
	return d, nil
}

func (s *Store) GetDataset(ctx context.Context, tenantID, id string) (eval.Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.datasets[id]
	if !ok || rec.TenantID != tenantID {
		return eval.Dataset{}, domain.ErrNotFound
	}
	return rec.Dataset, nil
}

func (s *Store) ListDatasets(ctx context.Context, tenantID string) ([]eval.Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []eval.Dataset
	for _, rec := range s.datasets {
		if rec.TenantID == tenantID {
			out = append(out, rec.Dataset)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) AddCase(ctx context.Context, tenantID, datasetID string, c eval.Case) (eval.Case, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds, ok := s.datasets[datasetID]
	if !ok || ds.TenantID != tenantID {
		return c, false, domain.ErrNotFound
	}
	if ds.Dataset.Status != "DRAFT" {
		return c, false, fmt.Errorf("%w: dataset is %s; cases can only be added to DRAFT datasets",
			eval.ErrInvalid, ds.Dataset.Status)
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
	for _, rec := range s.evalCases {
		if rec.TenantID == tenantID && rec.DatasetID == datasetID && rec.Case.Key == c.Key {
			return rec.Case, false, nil
		}
	}
	c.ID = newID()
	if c.OccurredAt.IsZero() {
		c.OccurredAt = now()
	}
	s.saveEvalCase(evalCaseRec{TenantID: tenantID, DatasetID: datasetID, Case: c})
	return c, true, nil
}

func (s *Store) FreezeDataset(ctx context.Context, tenantID, datasetID string) (eval.Dataset, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.datasets[datasetID]
	if !ok || rec.TenantID != tenantID {
		return eval.Dataset{}, domain.ErrNotFound
	}
	if rec.Dataset.Status != "DRAFT" {
		return eval.Dataset{}, fmt.Errorf("%w: dataset is not DRAFT", eval.ErrInvalid)
	}
	var parts []string
	for _, c := range s.evalCases {
		if c.TenantID != tenantID || c.DatasetID != datasetID {
			continue
		}
		gt, _ := json.Marshal(c.Case.GroundTruth)
		parts = append(parts, c.Case.Key+"|"+c.Case.Split+"|"+c.Case.RecordingRef+"|"+string(gt))
	}
	if len(parts) == 0 {
		return eval.Dataset{}, fmt.Errorf("%w: dataset has no cases", eval.ErrInvalid)
	}
	sort.Strings(parts)
	h := hashBytes([]byte(strings.Join(parts, "\n")))
	t := now()
	rec.Dataset.Status, rec.Dataset.ManifestHash, rec.Dataset.FrozenAt = "FROZEN", &h, &t
	s.saveDataset(rec)
	return rec.Dataset, nil
}

func (s *Store) ListCases(ctx context.Context, tenantID, datasetID, split string) ([]eval.Case, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []eval.Case
	for _, rec := range s.evalCases {
		if rec.TenantID != tenantID || rec.DatasetID != datasetID {
			continue
		}
		if split != "" && rec.Case.Split != split {
			continue
		}
		out = append(out, rec.Case)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (s *Store) StartEvalRun(ctx context.Context, tenantID, datasetID, profileID string, version int64, split, actor string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := eval.EvalRun{ID: newID(), DatasetID: datasetID, ProfileID: profileID, ProfileVersion: version,
		Split: split, Status: "RUNNING", CreatedAt: now()}
	s.saveEvalRun(evalRunRec{TenantID: tenantID, Run: r})
	return r.ID, nil
}

func (s *Store) FinishEvalRun(ctx context.Context, tenantID, id, status string, metrics any, reportRef string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.evalRuns[id]
	if !ok || rec.TenantID != tenantID || rec.Run.Status != "RUNNING" {
		return domain.ErrNotFound
	}
	if metrics != nil {
		rec.Run.Metrics, _ = json.Marshal(metrics)
	}
	if reportRef != "" {
		rec.Run.ReportRef = &reportRef
	}
	rec.Run.Status = status
	s.saveEvalRun(rec)
	return nil
}

func (s *Store) GetEvalRun(ctx context.Context, tenantID, id string) (eval.EvalRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.evalRuns[id]
	if !ok || rec.TenantID != tenantID {
		return eval.EvalRun{}, domain.ErrNotFound
	}
	return rec.Run, nil
}

func (s *Store) EvalRunCost(ctx context.Context, tenantID, runID string) (eval.CostSummary, error) {
	c, err := s.RunCost(ctx, tenantID, runID)
	if err != nil {
		return eval.CostSummary{}, err
	}
	return eval.CostSummary{ModelRequests: int(c.Requests), InputTokens: c.InputTokens,
		OutputTokens: c.OutputTokens, CachedInputTokens: c.CachedInputTokens,
		CostMicros: c.CostMicros, CostUSD: c.CostUSD}, nil
}

func (s *Store) CreateCandidate(ctx context.Context, tenantID string, c eval.Candidate, specRef string) (eval.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c.ID = newID()
	if c.Status == "" {
		c.Status = "PROPOSED"
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = now()
	}
	s.saveCandidate(evalCandidateRec{TenantID: tenantID, Candidate: c})
	return c, nil
}

func (s *Store) GetCandidate(ctx context.Context, tenantID, id string) (eval.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.candidates[id]
	if !ok || rec.TenantID != tenantID {
		return eval.Candidate{}, domain.ErrNotFound
	}
	return rec.Candidate, nil
}

func (s *Store) ListCandidates(ctx context.Context, tenantID, profileID string) ([]eval.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []eval.Candidate
	for _, rec := range s.candidates {
		if rec.TenantID != tenantID {
			continue
		}
		if profileID != "" && rec.Candidate.ProfileID != profileID {
			continue
		}
		out = append(out, rec.Candidate)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) UpdateCandidate(ctx context.Context, tenantID, id, fromStatus string, set map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.candidates[id]
	if !ok || rec.TenantID != tenantID || rec.Candidate.Status != fromStatus {
		return domain.ErrVersionConflict
	}
	for k, v := range set {
		switch k {
		case "status":
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("candidate status must be a string")
			}
			rec.Candidate.Status = s
		case "base_evaluation_id":
			rec.Candidate.BaseEvaluationID = asStringPtr(v)
		case "evaluation_id":
			rec.Candidate.EvaluationID = asStringPtr(v)
		case "gate_result":
			rec.Candidate.GateResult = asRaw(v)
		case "approved_by":
			rec.Candidate.ApprovedBy = asStringPtr(v)
		default:
			return fmt.Errorf("candidate column %q not settable", k)
		}
	}
	s.saveCandidate(rec)
	return nil
}

func asStringPtr(v any) *string {
	switch x := v.(type) {
	case string:
		return &x
	case *string:
		return x
	case nil:
		return nil
	}
	return nil
}

func asRaw(v any) json.RawMessage {
	switch x := v.(type) {
	case json.RawMessage:
		return x
	case []byte:
		return json.RawMessage(x)
	case string:
		return json.RawMessage(x)
	}
	b, _ := json.Marshal(v)
	return b
}

func (s *Store) CreateRollout(ctx context.Context, tenantID string, r eval.Rollout) (eval.Rollout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.rollouts {
		if rec.TenantID == tenantID && rec.Rollout.ProfileID == r.ProfileID &&
			(rec.Rollout.Status == "PENDING_APPROVAL" || rec.Rollout.Status == "CANARY") {
			return eval.Rollout{}, fmt.Errorf("%w: profile already has an active rollout", domain.ErrVersionConflict)
		}
	}
	r.ID = newID()
	t := now()
	r.StartedAt = &t
	s.saveRollout(evalRolloutRec{TenantID: tenantID, Rollout: r})
	return r, nil
}

func (s *Store) GetRollout(ctx context.Context, tenantID, id string) (eval.Rollout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.rollouts[id]
	if !ok || rec.TenantID != tenantID {
		return eval.Rollout{}, domain.ErrNotFound
	}
	return rec.Rollout, nil
}

func (s *Store) ActiveRollout(ctx context.Context, tenantID, profileID string) (*eval.Rollout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.rollouts {
		if rec.TenantID == tenantID && rec.Rollout.ProfileID == profileID &&
			(rec.Rollout.Status == "PENDING_APPROVAL" || rec.Rollout.Status == "CANARY") {
			out := rec.Rollout
			return &out, nil
		}
	}
	return nil, nil
}

func (s *Store) ListRollouts(ctx context.Context, tenantID, profileID string) ([]eval.Rollout, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []eval.Rollout
	for _, rec := range s.rollouts {
		if rec.TenantID != tenantID {
			continue
		}
		if profileID != "" && rec.Rollout.ProfileID != profileID {
			continue
		}
		out = append(out, rec.Rollout)
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].StartedAt, out[j].StartedAt
		if ai == nil || aj == nil {
			return false
		}
		return ai.After(*aj)
	})
	return out, nil
}

func (s *Store) ListCanaryRollouts(ctx context.Context) ([]struct{ TenantID, ID string }, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []struct{ TenantID, ID string }
	for _, rec := range s.rollouts {
		if rec.Rollout.Status == "CANARY" {
			out = append(out, struct{ TenantID, ID string }{rec.TenantID, rec.Rollout.ID})
		}
	}
	return out, nil
}

func (s *Store) EndRollout(ctx context.Context, tenantID, id string, e eval.RolloutEnd) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.rollouts[id]
	if !ok || rec.TenantID != tenantID {
		return domain.ErrNotFound
	}
	allowed := false
	for _, f := range e.From {
		allowed = allowed || f == rec.Rollout.Status
	}
	if !allowed {
		return domain.ErrVersionConflict
	}
	obs := map[string]any{"by": e.Actor}
	if e.Observations != nil {
		obs["details"] = e.Observations
	}
	rec.Rollout.Observations, _ = json.Marshal(obs)
	prevStatus := rec.Rollout.Status
	t := now()
	rec.Rollout.Status, rec.Rollout.EndedAt = e.To, &t
	s.saveRollout(rec)
	active := int64(0)
	switch {
	case e.Activate:
		active = rec.Rollout.ToVersion
	case e.RestoreIfPromoted && prevStatus == "PROMOTED":
		active = rec.Rollout.FromVersion
	}
	if active > 0 {
		for key, p := range s.profiles {
			if p.TenantID != tenantID || p.ID != rec.Rollout.ProfileID {
				continue
			}
			p.Default = p.Version == active
			s.profiles[key] = p
			s.put("profile", key, tenantID, p.Version, p)
		}
	}
	if e.ToVersionStatus != "" {
		key := profileKey(tenantID, rec.Rollout.ProfileID, rec.Rollout.ToVersion)
		if p, ok := s.profiles[key]; ok {
			p.Status = e.ToVersionStatus
			s.profiles[key] = p
			s.put("profile", key, tenantID, p.Version, p)
		}
	}
	return nil
}

func (s *Store) RolloutStats(ctx context.Context, tenantID, rolloutID string, control bool, profileID string, since time.Time) (eval.RolloutStats, error) {
	s.mu.Lock()
	runs := make([]domain.Run, 0, len(s.runs))
	for _, r := range s.runs {
		if r.TenantID == tenantID && !r.CreatedAt.Before(since) {
			runs = append(runs, r)
		}
	}
	s.mu.Unlock()
	var st eval.RolloutStats
	var scored []reasoning.Metrics
	for _, r := range runs {
		match := false
		if control {
			match = r.RolloutID == nil && r.ProfileID == profileID
		} else {
			match = r.RolloutID != nil && *r.RolloutID == rolloutID
		}
		if !match {
			continue
		}
		switch r.Status {
		case domain.RunCompleted, domain.RunFailed, domain.RunCancelled:
			st.Finished++
			if r.Status == domain.RunFailed {
				st.Failed++
			}
			c, err := s.RunCost(ctx, tenantID, r.ID)
			if err == nil {
				st.CostMicros += c.CostMicros
			}
		}
		if sc, err := s.ReasoningScore(ctx, tenantID, r.ID); err == nil {
			scored = append(scored, sc.Metrics)
		}
	}
	if st.Finished > 0 {
		st.FailRate = float64(st.Failed) / float64(st.Finished)
		st.CostPerRun = float64(st.CostMicros) / 1e6 / float64(st.Finished)
	}
	if len(scored) > 0 {
		sum := reasoning.Summarize(scored)
		st.Reasoning = &sum
	}
	return st, nil
}
