package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/projection"
)

// getRunProjection returns the ordered, read-only blocks a desktop client renders.
func (s *Server) getRunProjection(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	run, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	etag := fmt.Sprintf(`"%d-%d"`, run.Version, run.LastEventSeq)
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	steps, err := s.Store.ListSteps(r.Context(), id.tenant, run.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	thoughts, _, traceErr := s.Store.RunTrace(r.Context(), id.tenant, run.ID)
	if traceErr != nil && s.Log != nil {
		s.Log.Warn("run trace unavailable for projection", "run", run.ID, "err", traceErr)
	}
	approvals, err := s.Store.ListApprovals(r.Context(), id.tenant, run.ID)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	cost := projection.Cost{}
	if summary, err := s.Store.RunCost(r.Context(), id.tenant, run.ID); err == nil {
		cost = projection.Cost{
			ModelRequests: summary.Requests,
			InputTokens:   summary.InputTokens,
			OutputTokens:  summary.OutputTokens,
			CostUSD:       summary.CostUSD,
		}
	}

	outputs := make(map[string]json.RawMessage, len(steps))
	for _, step := range steps {
		if step.OutputRef == nil {
			continue
		}
		if raw := s.artifactJSON(r.Context(), id.tenant, *step.OutputRef); raw != nil {
			outputs[step.ID] = raw
		}
	}
	var result *domain.Decision
	if run.ResultRef != nil {
		if raw := s.artifactJSON(r.Context(), id.tenant, *run.ResultRef); raw != nil {
			var decision domain.Decision
			if json.Unmarshal(raw, &decision) == nil && decision.Kind != "" {
				result = &decision
			}
		}
	}

	principal, _ := auth.FromContext(r.Context())
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("ETag", etag)
	writeJSON(w, http.StatusOK, projection.Build(projection.Input{
		Run:        run,
		Steps:      steps,
		Thoughts:   thoughts,
		StepOutput: outputs,
		Approvals:  approvals,
		Cost:       cost,
		Result:     result,
		CanApprove: principal.Has(auth.Approver),
	}))
}
