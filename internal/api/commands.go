package api

import (
	"encoding/json"
	"net/http"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/tools"
)

// prepareCommand derives server-side values (artifacts, intents) for commands
// that need them. The client can never supply domain.CommandInternal.
func (s *Server) prepareCommand(r *http.Request, id identity, req *domain.CommandRequest) (int, string) {
	switch req.Type {
	case domain.CmdOverrideResult:
		var p struct {
			StepID string          `json:"step_id"`
			Output json.RawMessage `json:"output"`
		}
		if json.Unmarshal(req.Payload, &p) != nil || len(p.Output) == 0 || !json.Valid(p.Output) {
			return http.StatusBadRequest, "payload.output (JSON) and payload.step_id are required"
		}
		obs := domain.Observation{Outcome: "ok", Content: p.Output, TrustTag: "T1",
			Note: "output overridden by " + id.actor + ": " + req.Reason}
		data, _ := json.Marshal(obs)
		ref, hash, err := s.Artifacts.Put(r.Context(), id.tenant, "observation", data)
		if err != nil {
			return http.StatusServiceUnavailable, "artifact store unavailable"
		}
		req.Internal = &domain.CommandInternal{OutputRef: ref, OutputHash: hash}

	case domain.CmdReplacePendingInput:
		if s.Tools == nil || s.Policy == nil {
			return http.StatusNotImplemented, "replace_pending_input requires the tool registry"
		}
		var p struct {
			StepID string          `json:"step_id"`
			Args   json.RawMessage `json:"args"`
		}
		if json.Unmarshal(req.Payload, &p) != nil || len(p.Args) == 0 || !json.Valid(p.Args) {
			return http.StatusBadRequest, "payload.args (JSON object) and payload.step_id are required"
		}
		steps, err := s.Store.ListSteps(r.Context(), id.tenant, r.PathValue("run_id"))
		if err != nil {
			return http.StatusNotFound, "run not found"
		}
		var step *domain.Step
		for i := range steps {
			if steps[i].ID == p.StepID {
				step = &steps[i]
			}
		}
		if step == nil || step.Kind != domain.StepTool {
			return http.StatusUnprocessableEntity, "step is not a tool step of this run"
		}
		var old domain.ToolCall
		data, err := s.Artifacts.Get(r.Context(), id.tenant, step.InputRef)
		if err != nil || json.Unmarshal(data, &old) != nil {
			return http.StatusServiceUnavailable, "cannot read the current tool input"
		}
		call := domain.ToolCall{Name: old.Name, Args: p.Args}
		cj, _ := json.Marshal(call)
		ref, hash, err := s.Artifacts.Put(r.Context(), id.tenant, "tool-input", cj)
		if err != nil {
			return http.StatusServiceUnavailable, "artifact store unavailable"
		}
		in := &domain.CommandInternal{InputRef: ref, InputHash: hash}
		rules := profile.Unrestricted()
		if s.Rules != nil {
			run, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id"))
			if err != nil {
				return http.StatusNotFound, "run not found"
			}
			if rules, err = s.Rules(r.Context(), id.tenant, run.ProfileID, run.ProfileVersion); err != nil {
				return http.StatusServiceUnavailable, "cannot load the run profile"
			}
		}
		var reg *tools.Descriptor
		if t, ok := s.Tools.LookupFor(id.tenant, call.Name); ok {
			d := t.Descriptor()
			reg = &d
		}
		// Same decision path as the kernel: profile allowlist, risk
		// overrides and approval rules apply to replaced inputs too.
		pd, desc := rules.DecideCall(r.Context(), s.Policy, profile.Call{TenantID: id.tenant, RunID: r.PathValue("run_id"),
			Name: call.Name, Args: call.Args}, reg)
		in.PolicyDecision, _ = json.Marshal(pd)
		switch pd.Verdict {
		case policy.Deny:
			in.Denied = pd.Reason
		default:
			intent := s.Policy.BuildIntent(*desc, call, hash, pd)
			in.Intent, in.RequiredApprovals = &intent, pd.RequiredApprovals
		}
		req.Internal = in
	}
	return 0, ""
}

func (s *Server) listCheckpoints(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	cps, err := s.Store.ListCheckpoints(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": cps})
}

func (s *Server) forkRun(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}
	var body struct {
		CheckpointID   string `json:"checkpoint_id"`
		ProfileVersion int64  `json:"profile_version"`
	}
	_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body)
	run, created, err := s.Store.ForkRun(r.Context(), pg.ForkRunRequest{TenantID: id.tenant, SourceRunID: r.PathValue("run_id"),
		CheckpointID: body.CheckpointID, IdempotencyKey: key, ActorID: id.actor, ProfileVersion: body.ProfileVersion})
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": run.ID, "created": created, "status": run.DisplayStatus(),
		"forked_from_checkpoint_id": run.ForkedFrom})
}

func (s *Server) stepRevisions(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	revs, err := s.Store.StepOutputRevisions(r.Context(), id.tenant, r.PathValue("step_id"))
	if err != nil {
		s.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"revisions": revs})
}
