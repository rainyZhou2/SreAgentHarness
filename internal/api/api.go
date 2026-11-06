// Package api exposes the REST + SSE control plane (§17).
//
// Every request is authenticated (static token, OIDC JWT, or — development
// only — X-Tenant-ID/X-Actor-ID headers) and authorized by route role (see
// authz.go). The principal's tenant scopes all queries (RLS, §13.7).
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/profile"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/stream"
	"go-sre-harness/internal/tools"
)

type Store interface {
	CreateRun(ctx context.Context, req domain.CreateRunRequest) (domain.Run, bool, error)
	GetRun(ctx context.Context, tenantID, runID string) (domain.Run, error)
	ListRuns(ctx context.Context, tenantID string, status domain.RunStatus, limit int) ([]domain.Run, error)
	ListSteps(ctx context.Context, tenantID, runID string) ([]domain.Step, error)
	ListEvents(ctx context.Context, tenantID, runID string, afterSeq int64, limit int) ([]domain.Event, error)
	ListApprovals(ctx context.Context, tenantID, runID string) ([]domain.ApprovalRequest, error)
	ApplyCommand(ctx context.Context, tenantID, runID string, req domain.CommandRequest) (domain.Command, error)
	GetCommand(ctx context.Context, tenantID, runID, commandID string) (domain.Command, error)

	// Conversation (§29).
	CreateSession(ctx context.Context, req domain.CreateSessionRequest) (domain.Session, bool, error)
	ListSessions(ctx context.Context, tenantID string, f pg.SessionFilter) ([]pg.SessionSummary, error)
	UpdateSession(ctx context.Context, tenantID, id string, title, status *string) (domain.Session, error)
	ListPendingApprovals(ctx context.Context, tenantID string, limit int) ([]pg.PendingApproval, error)
	ResumeSession(ctx context.Context, tenantID, sessionID string) (domain.SessionView, error)
	SubmitTurn(ctx context.Context, req domain.SubmitTurnRequest, policy domain.BranchPolicy, maxPending int) (domain.SubmittedTurn, error)
	ListTurns(ctx context.Context, tenantID, sessionID, branchID string) ([]domain.Turn, error)
	GetTurn(ctx context.Context, tenantID, turnID string) (domain.Turn, error)
	ForkBranch(ctx context.Context, req domain.ForkBranchRequest) (domain.Branch, error)
	BranchMessages(ctx context.Context, tenantID, branchID string, upTo int64) ([]domain.Message, error)

	// Model usage and cost (§34, §36).
	ListModelRequests(ctx context.Context, tenantID, runID string) ([]pg.ModelRequestView, error)
	RunCost(ctx context.Context, tenantID, runID string) (pg.CostSummary, error)
	TenantCost(ctx context.Context, tenantID string, since time.Time) (pg.CostSummary, error)
	ListStreamSegments(ctx context.Context, tenantID, runID string) ([]pg.SegmentView, error)

	// Checkpoints and fork (§29.4).
	ListCheckpoints(ctx context.Context, tenantID, runID string) ([]pg.Checkpoint, error)
	ForkRun(ctx context.Context, req pg.ForkRunRequest) (domain.Run, bool, error)
	StepOutputRevisions(ctx context.Context, tenantID, stepID string) ([]pg.OutputRevision, error)
}

type Server struct {
	Store          Store
	Artifacts      artifact.Store
	DefaultProfile string
	DefaultVersion int64
	PollInterval   time.Duration // SSE DB polling; events are always read from the DB cursor
	Log            *slog.Logger

	// Conversation policy (§29.2).
	BranchPolicy   domain.BranchPolicy
	MaxPendingTurn int
	// Deltas is the ephemeral token stream (optional).
	Deltas stream.Bus
	// Rules returns the enforced rules of a profile version; commands that
	// plan tool steps (replace_pending_input) decide through them. Nil means
	// unrestricted.
	Rules func(ctx context.Context, tenantID, profileID string, version int64) (profile.Rules, error)
	// ResolveProfile picks the profile version for a new run (channel/canary, §32).
	ResolveProfile func(ctx context.Context, tenantID, profileID, routingKey string) (int64, *string)

	// Tools and Policy let the API compute intents server-side for
	// replace_pending_input (the client never supplies an intent).
	Tools  *tools.Registry
	Policy policy.Gate

	// Auth authenticates requests; nil means development header mode.
	Auth *auth.Chain
	// Audit records security-relevant decisions (optional).
	Audit Auditor
	// Metrics, when set, is exposed on GET /metrics.
	Metrics *obs.Metrics
	// Admin enables /v1/admin routes (role bindings, audit log).
	Admin AdminStore

	// Plugins, when set, backs GET /v1/admin/plugins.
	Plugins func() any

	extensions []Extension
	extRoles   map[string]auth.Role
}

// Extension is an API route plugin. Every non-GET route it registers must be
// declared in RouteRoles, otherwise the authorization middleware denies it.
// GET routes default to viewer unless declared.
type Extension interface {
	Register(mux *http.ServeMux)
	RouteRoles() map[string]auth.Role
}

// AdminStore backs the /v1/admin routes.
type AdminStore interface {
	BindRole(ctx context.Context, tenantID, principalID string, role auth.Role, actor string) error
	UnbindRole(ctx context.Context, tenantID, principalID string, role auth.Role) error
	ListAudit(ctx context.Context, tenantID string, limit int) ([]pg.AuditEntry, error)
}

// Extend adds an API extension (memory, evaluation, plugins, ...).
func (s *Server) Extend(ext Extension) { s.extensions = append(s.extensions, ext) }

func (s *Server) Handler() http.Handler {
	if s.PollInterval == 0 {
		s.PollInterval = 500 * time.Millisecond
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /v1/runs", s.createRun)
	mux.HandleFunc("GET /v1/runs", s.listRuns)
	mux.HandleFunc("GET /v1/runs/{run_id}", s.getRun)
	mux.HandleFunc("GET /v1/runs/{run_id}/steps", s.listSteps)
	mux.HandleFunc("GET /v1/runs/{run_id}/approvals", s.listApprovals)
	mux.HandleFunc("GET /v1/runs/{run_id}/events", s.listEvents)
	mux.HandleFunc("GET /v1/runs/{run_id}/events/stream", s.streamEvents)
	mux.HandleFunc("POST /v1/runs/{run_id}/commands", s.postCommand)
	mux.HandleFunc("GET /v1/runs/{run_id}/commands/{command_id}", s.getCommand)
	mux.HandleFunc("GET /v1/runs/{run_id}/model-requests", s.listModelRequests)
	mux.HandleFunc("GET /v1/runs/{run_id}/checkpoints", s.listCheckpoints)
	mux.HandleFunc("POST /v1/runs/{run_id}/fork", s.forkRun)
	mux.HandleFunc("GET /v1/runs/{run_id}/steps/{step_id}/revisions", s.stepRevisions)
	mux.HandleFunc("GET /v1/runs/{run_id}/cost", s.runCost)
	mux.HandleFunc("GET /v1/runs/{run_id}/stream-segments", s.listSegments)
	mux.HandleFunc("GET /v1/usage", s.tenantUsage)
	mux.HandleFunc("POST /v1/sessions", s.createSession)
	mux.HandleFunc("GET /v1/sessions", s.listSessions)
	mux.HandleFunc("GET /v1/sessions/{session_id}", s.getSession)
	mux.HandleFunc("PATCH /v1/sessions/{session_id}", s.updateSession)
	mux.HandleFunc("GET /v1/approvals", s.listPendingApprovals)
	mux.HandleFunc("POST /v1/sessions/{session_id}/turns", s.submitTurn)
	mux.HandleFunc("GET /v1/sessions/{session_id}/turns", s.listTurns)
	mux.HandleFunc("GET /v1/sessions/{session_id}/turns/{turn_id}", s.getTurn)
	mux.HandleFunc("POST /v1/sessions/{session_id}/branches", s.forkBranch)
	mux.HandleFunc("GET /v1/sessions/{session_id}/branches/{branch_id}/messages", s.branchMessages)
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Reg.Handler())
	}
	if s.Admin != nil {
		mux.HandleFunc("POST /v1/admin/role-bindings", s.bindRole)
		mux.HandleFunc("DELETE /v1/admin/role-bindings", s.unbindRole)
		mux.HandleFunc("GET /v1/admin/audit", s.listAudit)
	}
	mux.HandleFunc("GET /v1/whoami", s.whoami)
	if s.Plugins != nil {
		mux.HandleFunc("GET /v1/admin/plugins", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, s.Plugins()) })
	}
	s.extRoles = map[string]auth.Role{}
	for _, ext := range s.extensions {
		for pattern, role := range ext.RouteRoles() {
			if _, core := routeRoles[pattern]; core || publicRoutes[pattern] {
				continue // extensions cannot relax core routes
			}
			s.extRoles[pattern] = role
		}
		ext.Register(mux)
	}
	return s.authMiddleware(mux)
}

type identity struct{ tenant, actor string }

// ident returns the authenticated principal set by the auth middleware.
func ident(w http.ResponseWriter, r *http.Request) (identity, bool) {
	p, ok := auth.FromContext(r.Context())
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
		return identity{}, false
	}
	return identity{tenant: p.Tenant, actor: p.Subject}, true
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": msg})
}

func (s *Server) storeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "run or resource not found")
	default:
		s.Log.Error("api", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

type createRunBody struct {
	Goal           string  `json:"goal"`
	ProfileID      string  `json:"profile_id"`
	ProfileVersion int64   `json:"profile_version"`
	IncidentID     *string `json:"incident_id"`
}

func (s *Server) createRun(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}
	var body createRunBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&body); err != nil || strings.TrimSpace(body.Goal) == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "goal is required")
		return
	}
	var rollout *string
	if body.ProfileID == "" {
		body.ProfileID, body.ProfileVersion = s.DefaultProfile, s.DefaultVersion
	}
	if body.ProfileVersion == 0 && s.ResolveProfile != nil {
		body.ProfileVersion, rollout = s.ResolveProfile(r.Context(), id.tenant, body.ProfileID, key)
	}
	if body.ProfileVersion == 0 {
		body.ProfileVersion = s.DefaultVersion
	}
	data, _ := json.Marshal(map[string]string{"goal": body.Goal})
	ref, hash, err := s.Artifacts.Put(r.Context(), id.tenant, "goal", data)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "artifact_store_unavailable", "retry later")
		return
	}
	run, created, err := s.Store.CreateRun(r.Context(), domain.CreateRunRequest{
		TenantID: id.tenant, IncidentID: body.IncidentID, ProfileID: body.ProfileID, ProfileVersion: body.ProfileVersion,
		GoalRef: ref, GoalHash: hash, IdempotencyKey: key, InitiatorType: "user", InitiatorID: id.actor, RolloutID: rollout,
	})
	if err != nil {
		if strings.Contains(err.Error(), "foreign key") {
			writeErr(w, http.StatusUnprocessableEntity, "unknown_profile_or_incident", err.Error())
			return
		}
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"run_id": run.ID, "status": run.DisplayStatus(), "created": created,
		"version": run.Version, "stream_url": fmt.Sprintf("/v1/runs/%s/events/stream", run.ID)})
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	run, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	out := map[string]any{"run": run, "display_status": run.DisplayStatus(), "goal": s.goalText(r.Context(), id.tenant, run)}
	if run.ResultRef != nil {
		if data, err := s.Artifacts.Get(r.Context(), id.tenant, *run.ResultRef); err == nil {
			out["result"] = json.RawMessage(data)
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// listRuns returns the tenant's most recent runs, newest first.
func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeErr(w, http.StatusBadRequest, "invalid_limit", "limit must be 1..200")
			return
		}
		limit = n
	}
	status := domain.RunStatus(r.URL.Query().Get("status"))
	runs, err := s.Store.ListRuns(r.Context(), id.tenant, status, limit)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	type item struct {
		domain.Run
		DisplayStatus string `json:"display_status"`
		Goal          string `json:"goal"`
	}
	out := make([]item, 0, len(runs))
	for _, run := range runs {
		out = append(out, item{Run: run, DisplayStatus: run.DisplayStatus(), Goal: s.goalText(r.Context(), id.tenant, run)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

// maxInlineArtifact bounds artifacts inlined into step listings.
const maxInlineArtifact = 256 << 10

// artifactJSON returns a JSON artifact for inlining, or nil when it is
// missing, too large or not JSON.
func (s *Server) artifactJSON(ctx context.Context, tenant, ref string) json.RawMessage {
	if ref == "" || s.Artifacts == nil {
		return nil
	}
	data, err := s.Artifacts.Get(ctx, tenant, ref)
	if err != nil || len(data) > maxInlineArtifact || !json.Valid(data) {
		return nil
	}
	return data
}

// goalText reads the goal artifact ("" when unavailable).
func (s *Server) goalText(ctx context.Context, tenant string, run domain.Run) string {
	if run.GoalRef == "" || s.Artifacts == nil {
		return ""
	}
	data, err := s.Artifacts.Get(ctx, tenant, run.GoalRef)
	if err != nil {
		return ""
	}
	var g struct {
		Goal string `json:"goal"`
	}
	if json.Unmarshal(data, &g) != nil {
		return ""
	}
	return g.Goal
}

func (s *Server) listSteps(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if _, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id")); err != nil {
		s.storeErr(w, err)
		return
	}
	steps, err := s.Store.ListSteps(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	if r.URL.Query().Get("expand") != "io" {
		writeJSON(w, http.StatusOK, map[string]any{"steps": steps})
		return
	}
	// expand=io resolves each step's input and output artifacts so clients
	// can show tool arguments, observations and decisions inline.
	type stepIO struct {
		domain.Step
		Input  json.RawMessage `json:"input,omitempty"`
		Output json.RawMessage `json:"output,omitempty"`
	}
	out := make([]stepIO, 0, len(steps))
	for _, st := range steps {
		x := stepIO{Step: st, Input: s.artifactJSON(r.Context(), id.tenant, st.InputRef)}
		if st.OutputRef != nil {
			x.Output = s.artifactJSON(r.Context(), id.tenant, *st.OutputRef)
		}
		out = append(out, x)
	}
	writeJSON(w, http.StatusOK, map[string]any{"steps": out})
}

func (s *Server) listApprovals(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	list, err := s.Store.ListApprovals(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approvals": list})
}

func afterSeq(r *http.Request) int64 {
	v := r.URL.Query().Get("after_seq")
	if v == "" {
		v = r.Header.Get("Last-Event-ID")
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return max(n, 0)
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if _, err := s.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id")); err != nil {
		s.storeErr(w, err)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	events, err := s.Store.ListEvents(r.Context(), id.tenant, r.PathValue("run_id"), afterSeq(r), limit)
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": events})
}

// streamEvents serves SSE from the durable event cursor (§17.3): reconnecting
// with Last-Event-ID resumes without gaps; a client disconnect never affects
// the run. The stream ends after the terminal event of a finished run.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	runID := r.PathValue("run_id")
	if _, err := s.Store.GetRun(r.Context(), id.tenant, runID); err != nil {
		s.storeErr(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "no_streaming", "streaming unsupported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	cursor := afterSeq(r)
	ctx := r.Context()
	lastWrite := time.Now()
	var deltas <-chan stream.Delta
	if s.Deltas != nil && r.URL.Query().Get("include_deltas") == "true" {
		ch, cancel := s.Deltas.Subscribe(id.tenant, runID)
		defer cancel()
		deltas = ch
	}
	for {
		events, err := s.Store.ListEvents(ctx, id.tenant, runID, cursor, 500)
		if err != nil {
			return
		}
		for _, e := range events {
			data, _ := json.Marshal(e)
			fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, data)
			cursor = e.Seq
		}
		if len(events) > 0 {
			flusher.Flush()
			lastWrite = time.Now()
			continue
		}
		run, err := s.Store.GetRun(ctx, id.tenant, runID)
		if err != nil || (run.Status.Terminal() && run.LastEventSeq <= cursor) {
			return
		}
		if time.Since(lastWrite) > 15*time.Second {
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
			lastWrite = time.Now()
		}
		timer := time.NewTimer(s.PollInterval)
	wait:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case d, ok := <-deltas:
				if !ok {
					deltas = nil
					continue
				}
				// Deltas carry no event id: they are not replayable and a
				// reconnect resumes from the durable event cursor (§35.4).
				data, _ := json.Marshal(d)
				fmt.Fprintf(w, "event: model.delta\ndata: %s\n\n", data)
				flusher.Flush()
				lastWrite = time.Now()
			case <-timer.C:
				break wait
			}
		}
	}
}

func (s *Server) postCommand(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header is required")
		return
	}
	var req domain.CommandRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&req); err != nil || req.Type == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "type is required")
		return
	}
	req.IdempotencyKey, req.ActorID = key, id.actor
	if !s.requireCommandRole(w, r, id, req.Type) {
		return
	}
	if code, msg := s.prepareCommand(r, id, &req); code != 0 {
		writeErr(w, code, "invalid_command", msg)
		return
	}
	cmd, err := s.Store.ApplyCommand(r.Context(), id.tenant, r.PathValue("run_id"), req)
	if _, sensitive := commandRoles[req.Type]; sensitive || req.Type == domain.CmdCancel {
		outcome, details := "applied", map[string]any{"command_id": cmd.ID, "payload_bytes": len(req.Payload)}
		var rj *domain.RejectedError
		if errors.As(err, &rj) {
			outcome, details["reason"] = "rejected", rj.Reason
		} else if err != nil {
			outcome, details["error"] = "rejected", err.Error()
		}
		s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor,
			Action: "command." + string(req.Type), ResourceType: "run", ResourceID: r.PathValue("run_id"),
			Outcome: outcome, Details: details, TraceID: obs.TraceID(r.Context())})
	}
	var rej *domain.RejectedError
	switch {
	case err == nil:
		// 202: the command is durable and applied to desired state; the run's
		// actual transition is reported by events (run.paused, ...).
		writeJSON(w, http.StatusAccepted, cmd)
	case errors.Is(err, domain.ErrVersionConflict):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "version_conflict", "command": cmd})
	case errors.As(err, &rej):
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": rej.Reason, "command": cmd})
	default:
		s.storeErr(w, err)
	}
}

func (s *Server) getCommand(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	cmd, err := s.Store.GetCommand(r.Context(), id.tenant, r.PathValue("run_id"), r.PathValue("command_id"))
	if err != nil {
		s.storeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cmd)
}
