package localstore

import (
	"context"
	"sort"
	"time"

	"go-sre-harness/internal/domain"
	gw "go-sre-harness/internal/gateway"
	"go-sre-harness/internal/store/pg"
)

type modelAttemptRec struct {
	ID           string     `json:"id"`
	AttemptNo    int        `json:"attempt_no"`
	EndpointID   string     `json:"endpoint_id"`
	Model        string     `json:"model"`
	Status       string     `json:"status"`
	FinishReason string     `json:"finish_reason,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	ResponseRef  string     `json:"response_ref,omitempty"`
	TTFTms       int64      `json:"ttft_ms,omitempty"`
	InputTokens  int64      `json:"input_tokens,omitempty"`
	OutputTokens int64      `json:"output_tokens,omitempty"`
	CachedTokens int64      `json:"cached_input_tokens,omitempty"`
	CostMicros   int64      `json:"cost_micros,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
}

type modelReqRec struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id"`
	RunID       string            `json:"run_id"`
	StepID      string            `json:"step_id"`
	ModelAlias  string            `json:"model_alias"`
	Status      string            `json:"status"`
	ErrorCode   string            `json:"error_code,omitempty"`
	ManifestRef string            `json:"manifest_ref,omitempty"`
	RequestHash string            `json:"request_hash,omitempty"`
	ResponseRef string            `json:"response_ref,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	FinishedAt  *time.Time        `json:"finished_at,omitempty"`
	Attempts    []modelAttemptRec `json:"attempts,omitempty"`
}

func (s *Store) saveModelReq(v modelReqRec) {
	s.modelReqs[v.ID] = v
	s.put("modelreq", v.ID, v.RunID, 0, v)
}

func (s *Store) CreateModelRequest(ctx context.Context, sc gw.Scope, rec gw.RequestRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveModelReq(modelReqRec{ID: rec.ID, TenantID: sc.TenantID, RunID: sc.RunID, StepID: sc.StepID,
		ModelAlias: rec.ModelAlias, Status: "QUEUED", ManifestRef: rec.ManifestRef,
		RequestHash: rec.RequestHash, CreatedAt: now()})
	return nil
}

func (s *Store) StartModelAttempt(ctx context.Context, sc gw.Scope, rec gw.AttemptRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.modelReqs[rec.ModelRequestID]
	if !ok {
		return nil
	}
	r.Attempts = append(r.Attempts, modelAttemptRec{ID: rec.ID, AttemptNo: rec.AttemptNo,
		EndpointID: rec.EndpointID, Model: rec.ModelID, Status: "SENT", StartedAt: now()})
	r.Status = "INFLIGHT"
	s.saveModelReq(r)
	return nil
}

func (s *Store) AppendUsage(ctx context.Context, tenantID string, rec gw.UsageRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, r := range s.modelReqs {
		for i := range r.Attempts {
			if r.Attempts[i].ID != rec.AttemptID {
				continue
			}
			if rec.Usage != nil {
				r.Attempts[i].InputTokens = rec.Usage.InputTokens
				r.Attempts[i].OutputTokens = rec.Usage.OutputTokens
				r.Attempts[i].CachedTokens = rec.Usage.CachedInputTokens
			}
			r.Attempts[i].CostMicros = rec.CostMicros
			s.saveModelReq(r)
			_ = id
			return nil
		}
	}
	return nil
}

func (s *Store) FinishModelAttempt(ctx context.Context, sc gw.Scope, rec gw.AttemptResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.modelReqs[rec.ModelRequestID]
	if !ok {
		return nil
	}
	for i := range r.Attempts {
		if r.Attempts[i].ID != rec.ID {
			continue
		}
		a := &r.Attempts[i]
		a.Status, a.ResponseRef, a.FinishReason, a.ErrorCode = rec.Status, rec.ResponseRef, rec.FinishReason, rec.ErrorCode
		a.TTFTms = rec.TTFT.Milliseconds()
		a.InputTokens, a.OutputTokens, a.CachedTokens = rec.Usage.InputTokens, rec.Usage.OutputTokens, rec.Usage.CachedInputTokens
		a.CostMicros = rec.CostMicros
		end := now()
		a.EndedAt = &end
	}
	s.saveModelReq(r)
	return nil
}

func (s *Store) FinishModelRequest(ctx context.Context, sc gw.Scope, rec gw.RequestResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.modelReqs[rec.ID]
	if !ok {
		return nil
	}
	r.Status, r.ResponseRef, r.ErrorCode = rec.Status, rec.ResponseRef, rec.ErrorCode
	end := now()
	r.FinishedAt = &end
	s.saveModelReq(r)
	return nil
}

func (s *Store) SpentMicros(ctx context.Context, tenantID string, since time.Time) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, r := range s.modelReqs {
		if r.TenantID != tenantID || r.CreatedAt.Before(since) {
			continue
		}
		for _, a := range r.Attempts {
			total += a.CostMicros
		}
	}
	return total, nil
}

func (s *Store) ListModelRequests(ctx context.Context, tenantID, runID string) ([]pg.ModelRequestView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []pg.ModelRequestView
	for _, r := range s.modelReqs {
		if r.TenantID != tenantID || r.RunID != runID {
			continue
		}
		v := pg.ModelRequestView{ID: r.ID, StepID: r.StepID, ModelAlias: r.ModelAlias, Status: r.Status,
			ManifestRef: r.ManifestRef, RequestHash: r.RequestHash, CreatedAt: r.CreatedAt, FinishedAt: r.FinishedAt}
		if r.ResponseRef != "" {
			v.ResponseRef = &r.ResponseRef
		}
		if r.ErrorCode != "" {
			v.ErrorCode = &r.ErrorCode
		}
		for _, a := range r.Attempts {
			av := pg.ModelAttemptView{ID: a.ID, AttemptNo: a.AttemptNo, EndpointID: a.EndpointID, Model: a.Model,
				Status: a.Status}
			if a.FinishReason != "" {
				av.FinishReason = &a.FinishReason
			}
			v.Attempts = append(v.Attempts, av)
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) costLocked(tenantID, runID string, since time.Time) pg.CostSummary {
	out := pg.CostSummary{ByModel: map[string]int64{}}
	for _, r := range s.modelReqs {
		if r.TenantID != tenantID || (runID != "" && r.RunID != runID) || r.CreatedAt.Before(since) {
			continue
		}
		out.Requests++
		for _, a := range r.Attempts {
			out.Attempts++
			out.InputTokens += a.InputTokens
			out.OutputTokens += a.OutputTokens
			out.CachedInputTokens += a.CachedTokens
			out.CostMicros += a.CostMicros
			out.ByModel[a.Model] += a.CostMicros
		}
	}
	out.CostUSD = float64(out.CostMicros) / 1e6
	return out
}

func (s *Store) RunCost(ctx context.Context, tenantID, runID string) (pg.CostSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.costLocked(tenantID, runID, time.Time{}), nil
}

func (s *Store) TenantCost(ctx context.Context, tenantID string, since time.Time) (pg.CostSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.costLocked(tenantID, "", since), nil
}

var _ gw.Ledger = (*Store)(nil)

// RunUsage is the run's model spend for the kernel's run budget.
func (s *Store) RunUsage(ctx context.Context, tenantID, runID string) (domain.RunUsage, error) {
	c, err := s.RunCost(ctx, tenantID, runID)
	if err != nil {
		return domain.RunUsage{}, err
	}
	return domain.RunUsage{Requests: c.Requests, InputTokens: c.InputTokens, OutputTokens: c.OutputTokens,
		CostMicros: c.CostMicros}, nil
}
