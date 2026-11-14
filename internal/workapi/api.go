// Package workapi exposes the v2 control plane over HTTP as an api.Server
// extension:
//
//	POST /v1/works                         create a Work (operator; Idempotency-Key)
//	GET  /v1/works                         list Works (viewer; ?status=&kind=&queue=&subject=)
//	GET  /v1/works/{id}                    read a Work (ETag = version)
//	GET  /v1/works/{id}/events             timeline page (after_seq, limit)
//	GET  /v1/works/{id}/events/stream      timeline SSE (Last-Event-ID / after_seq)
//	GET  /v1/works/{id}/runs|evidence|hypotheses|watches|outcomes|actions|evaluation
//	GET  /v1/works/{id}/assignments        agent assignments per stage with results
//	GET  /v1/works/{id}/goals              goal of every generation
//	POST /v1/works/{id}/commands           pause|resume|cancel|replan|resolve|wake|set_queue (operator),
//	                                       confirm|reopen (approver); Idempotency-Key, If-Match
//	GET  /v1/work-templates                template catalog
//	POST /v1/works/{id}/evidence           attach evidence (operator; untrusted tier unless admin)
//	POST /v1/works/{id}/playbook           propose a Playbook candidate from a verified Work (operator)
//	POST /v1/events                        ingest an event envelope (service or operator, within scope)
//	GET  /v1/events/{id}                   inbox record
//	GET  /v1/actions/{id}/effects          kernel tool effects on the action's target
//	POST /v1/sources/{source}/events       source-native webhook via a domain capability (service)
//	GET  /v1/events/shadow-report          v1 trigger vs v2 routing agreement (?since=&until=)
//	GET  /v1/world/entities                ?ref= or ?attr=&value=
//	GET  /v1/world/facts                   ?subject=&predicate=&as_of=&known_at=
//	POST /v1/world/facts                   assert a fact (operator; untrusted tier unless admin)
//	POST /v1/world/snapshots/query         bitemporal snapshot (viewer)
//	GET  /v1/world/snapshots/{id}
//	GET  /v1/actions/{id}
//	POST /v1/actions/{id}/approve          approve/reject bound to intent_hash (approver)
//	POST /v1/actions/{id}/reconcile        read-only reconcile of an unknown effect (operator)
//	POST /v1/actions/{id}/resolve          human resolution of an unknown effect (approver)
//	POST /v1/actions/{id}/break-glass      emergency release with justification (break_glass role)
//
// Every query is scoped to the authenticated tenant (and PostgreSQL RLS
// through the request's tenant setting). Principals with a Scope are
// limited to their event sources/types and resources; approvers to their
// risk ceiling. Approvals, break-glass releases and scope denials are
// written to the security audit log.
package workapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/evidence"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/outcome"
	"go-sre-harness/internal/playbook"
	"go-sre-harness/internal/postmortem"
	"go-sre-harness/internal/protocol"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/store/pg"
	"go-sre-harness/internal/work"
	"go-sre-harness/internal/workeval"
	"go-sre-harness/internal/world"
)

// Normalizer turns a source-native webhook body into envelopes.
type Normalizer func(ctx context.Context, tenantID string, h http.Header, body []byte, now time.Time) ([]eventing.Envelope, error)

// MaxBody bounds request bodies.
const MaxBody = 1 << 20

// API is the HTTP extension.
type API struct {
	Store    *cpsql.Store
	Service  *orchestration.Service
	Ingestor *orchestration.Ingestor // nil: event ingestion disabled
	Actions  *action.Runtime         // nil: action routes return 503
	// Audit receives security-relevant decisions (scope denials,
	// approvals, break-glass); nil disables the audit trail.
	Audit Auditor
	// Sources are webhook normalizers by source name (domain capabilities).
	Sources map[string]Normalizer
	// Denials counts guard refusals (harness_guard_denials_total).
	Denials interface{ GuardDenied(guard, reason string) }
	World   *world.Service
	Clock   cpkit.Clock
	Log     *slog.Logger
	// StreamPoll is the SSE store poll interval (default 1s); StreamMax the
	// maximum stream duration before the client reconnects (default 30m).
	StreamPoll time.Duration
	StreamMax  time.Duration
	// Eval scores Works on the Improvement Loop dimensions (nil: 503).
	Eval *workeval.Evaluator
	// Playbooks proposes Playbook candidates (nil: route returns 503).
	Playbooks interface {
		Propose(ctx context.Context, req playbook.ProposeRequest) (memory.Memory, error)
	}
	// Postmortems builds ledger reports (nil: routes return 503).
	Postmortems *postmortem.Builder
	// Broker is the observability Query Broker (nil: routes return 503).
	Broker *observability.Broker
	// Attestation returns the deployment's launch attestation.
	Attestation func() governance.Attestation
}

// Auditor is the security audit sink (the kernel's audit log).
type Auditor interface {
	Audit(ctx context.Context, e pg.AuditEntry) error
}

// audit writes one audit entry; it never fails the request and does not
// depend on the request's cancellation.
func (a *API) audit(ctx context.Context, p auth.Principal, act, resType, resID, outcome string, details map[string]any) {
	if a.Audit == nil {
		return
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	actor := "user"
	if p.Has(auth.Service) && !p.Has(auth.Operator) {
		actor = "service"
	}
	if err := a.Audit.Audit(actx, pg.AuditEntry{TenantID: p.Tenant, ActorType: actor, ActorID: p.Subject, Action: act,
		ResourceType: resType, ResourceID: resID, Outcome: outcome, Details: details,
		OccurredAt: a.Clock.Now()}); err != nil {
		a.log().Warn("audit write failed", "action", act, "err", err)
	}
}

// outOfScope refuses (403) and audits a request outside the caller's scope.
func (a *API) outOfScope(w http.ResponseWriter, r *http.Request, p auth.Principal, act, resType, resID, why string) {
	if a.Denials != nil {
		a.Denials.GuardDenied("scope", act)
	}
	a.audit(r.Context(), p, act, resType, resID, "denied", map[string]any{"reason": why})
	writeErr(w, http.StatusForbidden, "forbidden", why)
}

func (a *API) log() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}

// RouteRoles declares the minimum role per route.
func (a *API) RouteRoles() map[string]auth.Role {
	roles := a.v2Roles()
	for route, role := range a.v3Roles() {
		roles[route] = role
	}
	return roles
}

func (a *API) v2Roles() map[string]auth.Role {
	return map[string]auth.Role{
		"POST /v1/works":                    auth.Operator,
		"GET /v1/works":                     auth.Viewer,
		"GET /v1/works/{id}":                auth.Viewer,
		"GET /v1/works/{id}/events":         auth.Viewer,
		"GET /v1/works/{id}/events/stream":  auth.Viewer,
		"GET /v1/works/{id}/runs":           auth.Viewer,
		"GET /v1/works/{id}/evidence":       auth.Viewer,
		"POST /v1/works/{id}/evidence":      auth.Operator,
		"POST /v1/works/{id}/playbook":      auth.Operator,
		"GET /v1/works/{id}/postmortem":     auth.Viewer,
		"POST /v1/works/{id}/postmortem":    auth.Operator,
		"GET /v1/works/{id}/hypotheses":     auth.Viewer,
		"GET /v1/works/{id}/decisions":      auth.Viewer,
		"GET /v1/works/{id}/watches":        auth.Viewer,
		"GET /v1/works/{id}/outcomes":       auth.Viewer,
		"GET /v1/works/{id}/evaluation":     auth.Viewer,
		"GET /v1/works/{id}/actions":        auth.Viewer,
		"POST /v1/works/{id}/commands":      auth.Operator,
		"GET /v1/work-templates":            auth.Viewer,
		"POST /v1/events":                   auth.Service,
		"GET /v1/events/{id}":               auth.Viewer,
		"GET /v1/world/entities":            auth.Viewer,
		"GET /v1/world/facts":               auth.Viewer,
		"POST /v1/world/facts":              auth.Operator,
		"POST /v1/world/snapshots/query":    auth.Viewer,
		"GET /v1/world/snapshots/{id}":      auth.Viewer,
		"GET /v1/actions/{id}":              auth.Viewer,
		"POST /v1/actions/{id}/approve":     auth.Approver,
		"POST /v1/actions/{id}/reconcile":   auth.Operator,
		"POST /v1/actions/{id}/resolve":     auth.Approver,
		"POST /v1/actions/{id}/break-glass": auth.BreakGlass,
		"GET /v1/events/shadow-report":      auth.Viewer,
		"POST /v1/sources/{source}/events":  auth.Service,
		"GET /v1/works/{id}/assignments":    auth.Viewer,
		"GET /v1/works/{id}/goals":          auth.Viewer,
		"GET /v1/actions/{id}/effects":      auth.Viewer,
	}
}

// Register mounts the routes.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/works", a.createWork)
	mux.HandleFunc("GET /v1/works", a.listWorks)
	mux.HandleFunc("GET /v1/works/{id}", a.getWork)
	mux.HandleFunc("GET /v1/works/{id}/events", a.workEvents)
	mux.HandleFunc("GET /v1/works/{id}/events/stream", a.streamEvents)
	mux.HandleFunc("GET /v1/works/{id}/runs", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListRunLinks(ctx, t, id)
	}))
	mux.HandleFunc("GET /v1/works/{id}/evidence", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListEvidence(ctx, t, id, 500)
	}))
	mux.HandleFunc("GET /v1/works/{id}/hypotheses", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListHypotheses(ctx, t, id)
	}))
	mux.HandleFunc("GET /v1/works/{id}/decisions", a.sub(func(ctx context.Context, t, id string) (any, error) {
		w, err := a.Store.GetWork(ctx, t, id)
		if err != nil {
			return nil, err
		}
		hs, err := a.Store.ListHypotheses(ctx, t, id)
		if err != nil {
			return nil, err
		}
		decisions := []evidence.DecisionVerdict{}
		scope := evidence.DecisionScope{TenantID: t, WorkID: id, Generation: w.Generation}
		for _, h := range hs {
			if h.Decision.CurrentFor(scope, h.Key) {
				decisions = append(decisions, *h.Decision)
			}
		}
		return decisions, nil
	}))
	mux.HandleFunc("GET /v1/works/{id}/watches", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListWatches(ctx, t, id, false)
	}))
	mux.HandleFunc("GET /v1/works/{id}/evaluation", a.sub(func(ctx context.Context, t, id string) (any, error) {
		if a.Eval == nil {
			return nil, fmt.Errorf("%w: work evaluation is not configured", work.ErrInvalid)
		}
		return a.Eval.Score(ctx, t, id)
	}))
	mux.HandleFunc("GET /v1/works/{id}/outcomes", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListOutcomes(ctx, t, id, 100)
	}))
	mux.HandleFunc("GET /v1/works/{id}/actions", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListActions(ctx, t, id)
	}))
	mux.HandleFunc("GET /v1/works/{id}/assignments", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.assignments(ctx, t, id)
	}))
	mux.HandleFunc("GET /v1/works/{id}/goals", a.sub(func(ctx context.Context, t, id string) (any, error) {
		return a.Store.ListGoals(ctx, t, id)
	}))
	mux.HandleFunc("POST /v1/works/{id}/commands", a.command)
	mux.HandleFunc("GET /v1/work-templates", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := principal(w, r); ok {
			writeJSON(w, http.StatusOK, map[string]any{"templates": a.Service.Catalog.List()})
		}
	})
	mux.HandleFunc("POST /v1/events", a.ingest)
	mux.HandleFunc("GET /v1/events/{id}", a.getEvent)
	mux.HandleFunc("GET /v1/world/entities", a.entities)
	mux.HandleFunc("GET /v1/world/facts", a.facts)
	mux.HandleFunc("POST /v1/world/facts", a.assertFact)
	mux.HandleFunc("POST /v1/world/snapshots/query", a.snapshot)
	mux.HandleFunc("GET /v1/world/snapshots/{id}", a.getSnapshot)
	mux.HandleFunc("GET /v1/actions/{id}", a.getAction)
	mux.HandleFunc("GET /v1/actions/{id}/effects", a.actionEffects)
	mux.HandleFunc("POST /v1/actions/{id}/approve", a.approve)
	mux.HandleFunc("POST /v1/actions/{id}/reconcile", a.reconcile)
	mux.HandleFunc("POST /v1/actions/{id}/resolve", a.resolve)
	mux.HandleFunc("POST /v1/actions/{id}/break-glass", a.breakGlass)
	mux.HandleFunc("POST /v1/works/{id}/evidence", a.addEvidence)
	mux.HandleFunc("POST /v1/works/{id}/playbook", a.proposePlaybook)
	mux.HandleFunc("GET /v1/works/{id}/postmortem", a.getPostmortem)
	mux.HandleFunc("POST /v1/works/{id}/postmortem", a.createPostmortem)
	mux.HandleFunc("GET /v1/events/shadow-report", a.shadowReport)
	mux.HandleFunc("POST /v1/sources/{source}/events", a.sourceEvents)
	a.registerV3(mux)
}

// ---- helpers ----

func principal(w http.ResponseWriter, r *http.Request) (auth.Principal, bool) {
	p, ok := auth.FromContext(r.Context())
	if !ok {
		writeErr(w, http.StatusUnauthorized, "unauthenticated", "authentication required")
	}
	return p, ok
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": msg})
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body too large")
		return false
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return false
	}
	return true
}

// fail maps domain errors to HTTP statuses.
func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	var te *work.TransitionError
	switch {
	case errors.Is(err, work.ErrNotFound), errors.Is(err, action.ErrNotFound), errors.Is(err, world.ErrNotFound),
		errors.Is(err, cpsql.ErrEvidenceNotFound):
		writeErr(w, http.StatusNotFound, "not_found", "not found")
	case errors.Is(err, work.ErrVersionConflict), errors.Is(err, action.ErrVersionConflict),
		errors.Is(err, evidence.ErrVersionConflict):
		if r.Header.Get("If-Match") != "" {
			writeErr(w, http.StatusPreconditionFailed, "version_mismatch", "the resource changed; reload and retry")
			return
		}
		writeErr(w, http.StatusConflict, "version_conflict", "concurrent update; retry")
	case errors.Is(err, work.ErrIdempotencyConflict):
		writeErr(w, http.StatusUnprocessableEntity, "idempotency_conflict", "the idempotency key was used with a different request")
	case errors.As(err, &te), errors.Is(err, work.ErrInvalidTransition):
		writeErr(w, http.StatusConflict, "invalid_transition", err.Error())
	case errors.Is(err, work.ErrLimit):
		writeErr(w, http.StatusConflict, "limit_reached", err.Error())
	case errors.Is(err, work.ErrOwnerConflict):
		writeErr(w, http.StatusConflict, "owner_conflict", err.Error())
	case errors.Is(err, action.ErrInvalidState), errors.Is(err, action.ErrTargetBusy), errors.Is(err, action.ErrPendingEffect):
		writeErr(w, http.StatusConflict, "invalid_action_state", err.Error())
	case errors.Is(err, action.ErrApprovalMismatch):
		writeErr(w, http.StatusUnprocessableEntity, "approval_mismatch", err.Error())
	case errors.Is(err, playbook.ErrNotVerified):
		writeErr(w, http.StatusConflict, "outcome_not_verified", err.Error())
	case errors.Is(err, work.ErrInvalid), errors.Is(err, playbook.ErrInvalid), errors.Is(err, eventing.ErrInvalid), errors.Is(err, world.ErrInvalid),
		errors.Is(err, outcome.ErrInvalidSpec), errors.Is(err, evidence.ErrInvalid), errors.Is(err, action.ErrNotApplied):
		writeErr(w, http.StatusBadRequest, "invalid_request", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeErr(w, http.StatusServiceUnavailable, "timeout", "request timed out")
	default:
		a.log().Error("work api", "method", r.Method, "path", r.URL.Path, "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	k := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if k == "" || len(k) > 200 {
		writeErr(w, http.StatusBadRequest, "idempotency_key_required", "Idempotency-Key header (max 200 chars) is required")
		return "", false
	}
	return k, true
}

// ifMatch parses an If-Match version ("7" or W/"7"); 0 when absent.
func ifMatch(w http.ResponseWriter, r *http.Request) (int64, bool) {
	v := strings.TrimSpace(r.Header.Get("If-Match"))
	if v == "" {
		return 0, true
	}
	v = strings.Trim(strings.TrimPrefix(v, "W/"), `"`)
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		writeErr(w, http.StatusBadRequest, "invalid_if_match", "If-Match must be a Work version")
		return 0, false
	}
	return n, true
}

func etag(w http.ResponseWriter, version int64) { w.Header().Set("ETag", fmt.Sprintf(`"%d"`, version)) }

func queryInt(r *http.Request, name string, def, max int) int {
	n, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil || n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func queryTime(w http.ResponseWriter, r *http.Request, name string, def time.Time) (time.Time, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_time", name+" must be RFC 3339")
		return t, false
	}
	return t.UTC(), true
}

// sub serves a list below a Work after checking the Work exists in the
// caller's tenant.
func (a *API) sub(fn func(ctx context.Context, tenant, workID string) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, ok := principal(w, r)
		if !ok {
			return
		}
		id := r.PathValue("id")
		if _, err := a.Store.GetWork(r.Context(), p.Tenant, id); err != nil {
			a.fail(w, r, err)
			return
		}
		items, err := fn(r.Context(), p.Tenant, id)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}
}

// wantsContract reports whether the client asked for the stable event
// contract (protocol=1 or the contract media type). Clients that ask for
// nothing keep getting the timeline as before.
func wantsContract(r *http.Request) bool {
	return r.URL.Query().Get("protocol") == "1" || strings.Contains(r.Header.Get("Accept"), protocol.MediaType)
}

// ---- works ----

func (a *API) createWork(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var req orchestration.CreateRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Template != nil && !p.Has(auth.Admin) {
		writeErr(w, http.StatusForbidden, "forbidden", "inline templates require the admin role")
		return
	}
	for _, ref := range req.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.create", "subject", ref, "subject "+ref+" is outside the caller's scope")
			return
		}
	}
	req.TenantID, req.ActorID, req.ActorType, req.IdempotencyKey = p.Tenant, p.Subject, "user", key
	wk, created, err := a.Service.Create(r.Context(), req)
	if err != nil {
		a.failV3(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
		a.log().Info("work created", "tenant", p.Tenant, "work_id", wk.ID, "kind", wk.Kind, "actor", p.Subject)
	}
	etag(w, wk.Version)
	w.Header().Set("Location", "/v1/works/"+wk.ID)
	writeJSON(w, code, wk)
}

func (a *API) listWorks(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	f := work.Filter{TenantID: p.Tenant, Kind: work.Kind(q.Get("kind")), Subject: q.Get("subject"),
		Queue: work.Queue(q.Get("queue")), Limit: queryInt(r, "limit", 50, 200), BeforeID: q.Get("before_id")}
	if f.Queue != "" && !work.ValidQueue(f.Queue) {
		writeErr(w, http.StatusBadRequest, "invalid", "unknown queue")
		return
	}
	for _, s := range q["status"] {
		for _, x := range strings.Split(s, ",") {
			if x = strings.TrimSpace(strings.ToUpper(x)); x != "" {
				st := work.Status(x)
				if !st.Valid() {
					writeErr(w, http.StatusBadRequest, "invalid_status", "unknown status "+x)
					return
				}
				f.Status = append(f.Status, st)
			}
		}
	}
	if v := q.Get("before_updated"); v != "" {
		t, ok := queryTime(w, r, "before_updated", time.Time{})
		if !ok {
			return
		}
		f.BeforeUpdated = &t
	}
	items, err := a.Store.ListWorks(r.Context(), f)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := map[string]any{"items": items}
	if len(items) == f.Limit && len(items) > 0 {
		last := items[len(items)-1]
		resp["next"] = map[string]any{"before_updated": last.UpdatedAt, "before_id": last.ID}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) getWork(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	wk, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	etag(w, wk.Version)
	writeJSON(w, http.StatusOK, wk)
}

func (a *API) workEvents(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := a.Store.GetWork(r.Context(), p.Tenant, id); err != nil {
		a.fail(w, r, err)
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	evs, err := a.Store.ListWorkEvents(r.Context(), p.Tenant, id, after, queryInt(r, "limit", 200, 1000))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	next := after
	if len(evs) > 0 {
		next = evs[len(evs)-1].Seq
	}
	if wantsContract(r) {
		writeJSON(w, http.StatusOK, map[string]any{"schema_version": protocol.SchemaVersion, "items": protocol.FromWorkEvents(evs),
			"next_after_seq": next})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": evs, "next_after_seq": next})
}

// streamEvents is an SSE stream of the timeline read from the durable log,
// resumable with Last-Event-ID (the work_seq of the last event received).
func (a *API) streamEvents(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := a.Store.GetWork(r.Context(), p.Tenant, id); err != nil {
		a.fail(w, r, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming_unsupported", "streaming unsupported")
		return
	}
	after, _ := strconv.ParseInt(r.URL.Query().Get("after_seq"), 10, 64)
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			after = n
		}
	}
	poll, max := a.StreamPoll, a.StreamMax
	if poll <= 0 {
		poll = time.Second
	}
	if max <= 0 {
		max = 30 * time.Minute
	}
	contract := wantsContract(r)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	if contract {
		w.Header().Set("X-Harness-Event-Schema", strconv.Itoa(protocol.SchemaVersion))
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "retry: 3000\n\n")
	flusher.Flush()
	ctx, cancel := context.WithTimeout(r.Context(), max)
	defer cancel()
	tick := time.NewTicker(poll)
	defer tick.Stop()
	lastBeat := time.Now()
	for {
		evs, err := a.Store.ListWorkEvents(ctx, p.Tenant, id, after, 200)
		if err != nil {
			if ctx.Err() == nil {
				fmt.Fprintf(w, "event: error\ndata: {\"error\":\"store\"}\n\n")
				flusher.Flush()
			}
			return
		}
		for _, e := range evs {
			if contract {
				// The stable event contract: the sequence is the SSE id, so
				// Last-Event-ID resumes exactly after the last event seen.
				pe := protocol.FromWork(e)
				b, _ := json.Marshal(pe)
				fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", pe.Sequence, pe.Type, b)
			} else {
				b, _ := json.Marshal(e)
				fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, b)
			}
			after = e.Seq
		}
		if len(evs) > 0 {
			flusher.Flush()
			lastBeat = time.Now()
			if len(evs) == 200 {
				continue
			}
		} else if time.Since(lastBeat) > 15*time.Second {
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
			lastBeat = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (a *API) command(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	key, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	version, ok := ifMatch(w, r)
	if !ok {
		return
	}
	var req orchestration.CommandRequest
	if !decode(w, r, &req) {
		return
	}
	if (req.Type == work.CmdConfirm || req.Type == work.CmdReopen) && !p.Has(auth.Approver) {
		writeErr(w, http.StatusForbidden, "forbidden", req.Type+" requires the approver role")
		return
	}
	req.TenantID, req.WorkID, req.ActorID, req.IdempotencyKey, req.ExpectedVersion = p.Tenant, r.PathValue("id"), p.Subject, key, version
	if len(req.Payload) > 0 && p.Scope != nil {
		var cp orchestration.CommandPayload
		if err := json.Unmarshal(req.Payload, &cp); err == nil && cp.Goal != nil {
			for _, ref := range cp.Goal.SubjectRefs {
				if !p.AllowsResource(ref) {
					a.outOfScope(w, r, p, "work.command", "subject", ref, "subject "+ref+" is outside the caller's scope")
					return
				}
			}
		}
	}
	wk, err := a.Service.Command(r.Context(), req)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.log().Info("work command", "tenant", p.Tenant, "work_id", wk.ID, "command", req.Type, "actor", p.Subject)
	etag(w, wk.Version)
	writeJSON(w, http.StatusOK, wk)
}

// Assignment is one agent assignment of a Work stage (RFC §13.2): an owner
// run started for a stage role, with its result once the run ended.
type Assignment struct {
	RunID        string     `json:"run_id"`
	Generation   int        `json:"generation"`
	StageID      string     `json:"stage_id"`
	Role         string     `json:"role"`
	Required     bool       `json:"required"`
	Status       string     `json:"status,omitempty"`
	Outcome      string     `json:"outcome,omitempty"`
	CostMicros   int64      `json:"cost_micros"`
	EvidenceRefs []string   `json:"evidence_refs,omitempty"`
	AssignedAt   time.Time  `json:"assigned_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
}

func (a *API) assignments(ctx context.Context, tenantID, workID string) ([]Assignment, error) {
	links, err := a.Store.ListRunLinks(ctx, tenantID, workID)
	if err != nil {
		return nil, err
	}
	out := []Assignment{}
	for _, l := range links {
		if l.Role != work.RoleOwner || l.StageID == "" {
			continue
		}
		role := l.AgentRole
		if role == "" {
			role = l.Phase
		}
		out = append(out, Assignment{RunID: l.RunID, Generation: l.Generation, StageID: l.StageID, Role: role,
			Required: l.Required, Status: l.Status, Outcome: l.Outcome, CostMicros: l.CostMicros,
			EvidenceRefs: l.EvidenceRefs, AssignedAt: l.CreatedAt, FinishedAt: l.FinishedAt})
	}
	return out, nil
}

// ---- events ----

// sourceEvents accepts a source-native webhook (Alertmanager, Kubernetes
// events, Argo CD, ...), normalizes it with the source's domain capability
// and ingests every envelope. All envelopes must be within the caller's
// scope or none is ingested.
func (a *API) sourceEvents(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	if a.Ingestor == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "event ingestion is disabled")
		return
	}
	source := r.PathValue("source")
	norm, ok := a.Sources[source]
	if !ok {
		writeErr(w, http.StatusNotFound, "not_found", "unknown event source "+source)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid", "read body: "+err.Error())
		return
	}
	if len(body) > MaxBody {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "body exceeds 1 MiB")
		return
	}
	// Source attestation: an operator-declared source must be enabled for
	// the tenant and, when it signs its requests, prove the body is its own.
	if pol, ok := a.Ingestor.Sources[source]; ok {
		if err := pol.Verify(p.Tenant, r.Header, body); err != nil {
			a.outOfScope(w, r, p, "event.ingest", "source", source, err.Error())
			return
		}
	}
	envs, err := norm(r.Context(), p.Tenant, r.Header, body, a.Clock.Now())
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid", cpkit.Truncate(err.Error(), 500))
		return
	}
	if len(envs) > 500 {
		writeErr(w, http.StatusRequestEntityTooLarge, "too_large", "a webhook may carry at most 500 events")
		return
	}
	for i := range envs {
		envs[i].TenantID = p.Tenant
		if !p.AllowsEvent(envs[i].Source, envs[i].Type, envs[i].SubjectRef) {
			a.outOfScope(w, r, p, "event.ingest", "event", envs[i].Source+"/"+envs[i].Type,
				"event source, type or subject is outside the caller's scope")
			return
		}
	}
	type item struct {
		ID       string `json:"id"`
		InboxID  string `json:"inbox_id,omitempty"`
		Created  bool   `json:"created"`
		Decision string `json:"decision,omitempty"`
		Error    string `json:"error,omitempty"`
	}
	out := make([]item, 0, len(envs))
	failed := 0
	for _, env := range envs {
		rec, created, err := a.Ingestor.Ingest(r.Context(), env)
		it := item{ID: env.ID, InboxID: rec.InboxID, Created: created, Decision: rec.Decision}
		if err != nil {
			it.Error, failed = cpkit.Truncate(err.Error(), 300), failed+1
		}
		out = append(out, it)
	}
	code := http.StatusAccepted
	if failed > 0 && failed == len(envs) {
		code = http.StatusBadRequest
	}
	writeJSON(w, code, map[string]any{"source": source, "events": out, "failed": failed})
}

func (a *API) ingest(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	if a.Ingestor == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "event ingestion is disabled")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBody))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large", "request body too large")
		return
	}
	// Schema 1 and the v2 wire envelope are both accepted.
	env, err := eventing.Upcast(body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if pol, ok := a.Ingestor.Sources[env.Source]; ok {
		if err := pol.Verify(p.Tenant, r.Header, body); err != nil {
			a.outOfScope(w, r, p, "event.ingest", "source", env.Source, err.Error())
			return
		}
	}
	if env.TenantID != "" && env.TenantID != p.Tenant {
		writeErr(w, http.StatusForbidden, "forbidden", "tenant_id does not match the caller")
		return
	}
	env.TenantID = p.Tenant
	if !p.AllowsEvent(env.Source, env.Type, env.SubjectRef) {
		a.outOfScope(w, r, p, "event.ingest", "event", env.Source+"/"+env.Type,
			"event source, type or subject is outside the caller's scope")
		return
	}
	rec, created, err := a.Ingestor.Ingest(r.Context(), env)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	code := http.StatusAccepted
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"inbox_id": rec.InboxID, "duplicate": !created, "state": rec.State,
		"work_id": rec.WorkID, "decision": rec.Decision})
}

func (a *API) getEvent(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	rec, err := a.Store.GetEvent(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if r.URL.Query().Get("decision") != "" {
		// Why the event went where it went: rule and version, candidates.
		d, _, err := a.Store.GetDecision(r.Context(), p.Tenant, rec.InboxID)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"event": rec, "decision": d})
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// ---- world ----

func (a *API) entities(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	if ref := q.Get("ref"); ref != "" {
		e, err := a.Store.GetEntity(r.Context(), p.Tenant, ref)
		if err != nil {
			a.fail(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, e)
		return
	}
	if q.Get("attr") == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "ref or attr/value is required")
		return
	}
	items, err := a.Store.FindEntities(r.Context(), p.Tenant, q.Get("attr"), q.Get("value"), queryInt(r, "limit", 20, 100))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (a *API) facts(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	subjects := q["subject"]
	if len(subjects) == 0 || len(subjects) > 50 {
		writeErr(w, http.StatusBadRequest, "invalid_request", "1–50 subject parameters are required")
		return
	}
	now := a.Clock.Now()
	asOf, ok := queryTime(w, r, "as_of", now)
	if !ok {
		return
	}
	knownAt, ok := queryTime(w, r, "known_at", now)
	if !ok {
		return
	}
	items, err := a.Store.FactsAt(r.Context(), p.Tenant, subjects, q["predicate"], asOf, knownAt)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "as_of": asOf, "known_at": knownAt})
}

func (a *API) assertFact(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	var f world.Fact
	if !decode(w, r, &f) {
		return
	}
	f.ID, f.TenantID = "", p.Tenant
	if !p.AllowsResource(f.SubjectRef) {
		a.outOfScope(w, r, p, "world.assert_fact", "subject", f.SubjectRef, "subject is outside the caller's scope")
		return
	}
	// API writers are untrusted unless admin: a model or script posting
	// facts must not outrank first-party sources.
	if !p.Has(auth.Admin) || f.TrustTier == "" {
		f.TrustTier = world.TrustUntrusted
	}
	if f.Source == "" {
		f.Source = "api:" + p.Subject
	}
	out, created, err := a.Store.AssertFact(r.Context(), f)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, out)
}

type snapshotQuery struct {
	Subjects []string       `json:"subjects"`
	AsOf     *time.Time     `json:"as_of,omitempty"`
	KnownAt  *time.Time     `json:"known_at,omitempty"`
	MaxAge   cpkit.Duration `json:"max_age,omitempty"`
	Required []string       `json:"required,omitempty"`
	Depth    int            `json:"depth,omitempty"`
	MaxNodes int            `json:"max_nodes,omitempty"`
	Persist  bool           `json:"persist,omitempty"`
}

func (a *API) snapshot(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	if a.World == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "world state is not configured")
		return
	}
	var q snapshotQuery
	if !decode(w, r, &q) {
		return
	}
	req := world.SnapshotRequest{TenantID: p.Tenant, Subjects: q.Subjects, MaxAge: q.MaxAge.D(), Required: q.Required,
		Depth: q.Depth, MaxNodes: q.MaxNodes, Persist: q.Persist}
	if q.AsOf != nil {
		req.AsOf = *q.AsOf
	}
	if q.KnownAt != nil {
		req.KnownAt = *q.KnownAt
	}
	snap, err := a.World.Snapshot(r.Context(), req)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (a *API) getSnapshot(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	snap, err := a.Store.GetSnapshot(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// ---- actions ----

func (a *API) getAction(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	rec, err := a.Store.GetAction(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rec)
}

// actionEffects lists the kernel tool effects on the action's target: the
// agent-side ledger of the same resource (open ones block the dispatch).
func (a *API) actionEffects(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	rec, err := a.Store.GetAction(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	effects, err := a.Store.KernelEffects(r.Context(), p.Tenant, action.EffectTargets(rec.TargetRef), false, 100)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	open := 0
	for _, e := range effects {
		if e.Open() {
			open++
		}
	}
	if effects == nil {
		effects = []action.KernelEffect{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"action_id": rec.ID, "target_ref": rec.TargetRef,
		"targets": action.EffectTargets(rec.TargetRef), "open": open, "effects": effects})
}

func (a *API) actionsEnabled(w http.ResponseWriter) bool {
	if a.Actions == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "the action runtime is disabled (HARNESS_ACTION_V2_ENABLED)")
		return false
	}
	return true
}

// wake asks the orchestrator to look at the action's Work now.
func (a *API) wake(ctx context.Context, rec action.Record, reason, actor string) {
	if err := a.Store.WakeWork(ctx, rec.TenantID, rec.WorkID, reason, "user", actor,
		map[string]any{"action_id": rec.ID, "state": rec.State}); err != nil && !errors.Is(err, work.ErrNotFound) {
		a.log().Warn("wake work after action decision failed", "work_id", rec.WorkID, "err", err)
	}
}

func (a *API) approve(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.actionsEnabled(w) {
		return
	}
	var req struct {
		Decision   string `json:"decision"`
		IntentHash string `json:"intent_hash"`
		Comment    string `json:"comment,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Decision == "" {
		req.Decision = "approve"
	}
	cur, err := a.Store.GetAction(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if req.Decision == "approve" && (!p.AllowsResource(cur.TargetRef) || !p.AllowsRisk(string(cur.Risk))) {
		a.outOfScope(w, r, p, "action.approve", "action", cur.ID,
			fmt.Sprintf("the caller may not approve %s actions on %s", cur.Risk, cur.TargetRef))
		return
	}
	owner := ""
	if wk, err := a.Store.GetWork(r.Context(), p.Tenant, cur.WorkID); err == nil {
		owner = wk.OwnerActor
	}
	rec, err := a.Actions.Decide(r.Context(), p.Tenant, cur.ID, p.Subject, req.Decision, req.IntentHash,
		cpkit.Truncate(req.Comment, 500), owner)
	if err != nil {
		a.audit(r.Context(), p, "action."+req.Decision, "action", cur.ID, "rejected", map[string]any{"reason": err.Error()})
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "action."+req.Decision, "action", rec.ID, "allowed", map[string]any{
		"work_id": rec.WorkID, "target": rec.TargetRef, "risk": rec.Risk, "intent_hash": rec.IntentHash, "state": rec.State})
	a.log().Info("action decision", "tenant", p.Tenant, "action_id", rec.ID, "decision", req.Decision, "actor", p.Subject,
		"state", rec.State)
	a.wake(r.Context(), rec, "action_"+req.Decision, p.Subject)
	writeJSON(w, http.StatusOK, rec)
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.actionsEnabled(w) {
		return
	}
	rec, err := a.Actions.Reconcile(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.wake(r.Context(), rec, "action_reconciled", p.Subject)
	writeJSON(w, http.StatusOK, rec)
}

func (a *API) resolve(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.actionsEnabled(w) {
		return
	}
	var req struct {
		Applied *bool  `json:"applied"`
		Comment string `json:"comment"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Applied == nil || strings.TrimSpace(req.Comment) == "" {
		writeErr(w, http.StatusBadRequest, "invalid_request", "applied and comment are required")
		return
	}
	cur, err := a.Store.GetAction(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if !p.AllowsResource(cur.TargetRef) {
		a.outOfScope(w, r, p, "action.resolve", "action", cur.ID, "the action target is outside the caller's scope")
		return
	}
	rec, err := a.Actions.Resolve(r.Context(), p.Tenant, cur.ID, p.Subject, *req.Applied, cpkit.Truncate(req.Comment, 1000))
	if err == nil {
		a.audit(r.Context(), p, "action.resolve", "action", rec.ID, "applied", map[string]any{"applied": *req.Applied,
			"state": rec.State})
	}
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.log().Info("action resolved by human", "tenant", p.Tenant, "action_id", rec.ID, "applied", *req.Applied, "actor", p.Subject)
	a.wake(r.Context(), rec, "action_resolved", p.Subject)
	writeJSON(w, http.StatusOK, rec)
}

// breakGlass releases a held action in an emergency (§19.4): a named human
// with the break_glass role, a justification, the intent hash they saw,
// and a separate audit entry and Work timeline event.
func (a *API) breakGlass(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok || !a.actionsEnabled(w) {
		return
	}
	var req struct {
		IntentHash    string `json:"intent_hash"`
		Justification string `json:"justification"`
		// IncidentRef names the incident the release belongs to (required
		// for R2 actions).
		IncidentRef string `json:"incident_ref,omitempty"`
	}
	if !decode(w, r, &req) {
		return
	}
	cur, err := a.Store.GetAction(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	if p.Method == "dev" {
		a.outOfScope(w, r, p, "action.break_glass", "action", cur.ID, "break-glass requires an authenticated identity")
		return
	}
	if !p.AllowsResource(cur.TargetRef) {
		a.outOfScope(w, r, p, "action.break_glass", "action", cur.ID, "the action target is outside the caller's scope")
		return
	}
	just, _ := memory.Redact(req.Justification)
	rec, err := a.Actions.BreakGlass(r.Context(), p.Tenant, cur.ID, p.Subject, req.IntentHash, just, req.IncidentRef)
	if err != nil {
		a.audit(r.Context(), p, "action.break_glass", "action", cur.ID, "rejected", map[string]any{"reason": err.Error()})
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "action.break_glass", "action", rec.ID, "applied", map[string]any{
		"work_id": rec.WorkID, "target": rec.TargetRef, "risk": rec.Risk, "intent_hash": rec.IntentHash,
		"justification": cpkit.Truncate(just, 500), "incident_ref": req.IncidentRef, "grant_expires_at": rec.BreakGlass.ExpiresAt})
	if err := a.Store.WakeWork(r.Context(), rec.TenantID, rec.WorkID, "action_break_glass", "user", p.Subject,
		map[string]any{"action_id": rec.ID, "justification": cpkit.Truncate(just, 500)}); err != nil &&
		!errors.Is(err, work.ErrNotFound) {
		a.log().Warn("wake work after break-glass failed", "work_id", rec.WorkID, "err", err)
	}
	writeJSON(w, http.StatusOK, rec)
}

// proposePlaybook turns a verified successful Work into a CANDIDATE
// procedural memory. Promotion to ACTIVE goes through the memory API
// (reviewer distinct from the proposer, verified success required).
func (a *API) proposePlaybook(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	if a.Playbooks == nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "playbooks need a memory store")
		return
	}
	wk, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	for _, ref := range wk.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.propose_playbook", "work", wk.ID, "the Work is outside the caller's scope")
			return
		}
	}
	var b struct {
		Title              string   `json:"title"`
		Environments       []string `json:"environments"`
		VersionConstraints []string `json:"version_constraints"`
		RiskOfReuse        string   `json:"risk_of_reuse"`
		ValidForDays       int      `json:"valid_for_days"`
	}
	if !decode(w, r, &b) {
		return
	}
	if b.ValidForDays < 0 || b.ValidForDays > 3650 {
		writeErr(w, http.StatusBadRequest, "invalid_request", "valid_for_days must be 0-3650")
		return
	}
	m, err := a.Playbooks.Propose(r.Context(), playbook.ProposeRequest{TenantID: p.Tenant, WorkID: wk.ID, ActorID: p.Subject,
		Title: b.Title, Environments: b.Environments, VersionConstraints: b.VersionConstraints, RiskOfReuse: b.RiskOfReuse,
		ValidFor: time.Duration(b.ValidForDays) * 24 * time.Hour})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.audit(r.Context(), p, "work.propose_playbook", "memory", m.ID, "applied", map[string]any{"work_id": wk.ID})
	writeJSON(w, http.StatusCreated, m)
}

// addEvidence attaches operator-supplied evidence to a Work. It is stored
// as untrusted (T4) unless an admin states a trust tier; summaries are
// redacted and bounded.
func (a *API) addEvidence(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	wk, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	for _, ref := range wk.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, "work.add_evidence", "work", wk.ID, "the Work is outside the caller's scope")
			return
		}
	}
	var e evidence.Record
	if !decode(w, r, &e) {
		return
	}
	e.ID, e.TenantID, e.WorkID, e.ActorID, e.RunID, e.AgentRole = "", p.Tenant, wk.ID, p.Subject, "", ""
	if !p.Has(auth.Admin) || e.TrustTier == "" {
		e.TrustTier = world.TrustUntrusted
	}
	if e.SourceType == "" {
		e.SourceType = evidence.SourceHuman
	}
	if e.SourceRef == "" {
		e.SourceRef = "api:" + p.Subject
	}
	if n := 0; e.Summary != "" {
		e.Summary, n = memory.Redact(e.Summary)
		if n > 0 {
			e.Redaction = "secrets_removed"
		}
	}
	out, created, err := a.Store.AddEvidence(r.Context(), e)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
		if err := a.Store.WakeWork(r.Context(), p.Tenant, wk.ID, "evidence_added", "user", p.Subject,
			map[string]any{"evidence_id": out.ID}); err != nil && !errors.Is(err, work.ErrNotFound) {
			a.log().Warn("wake work after evidence failed", "work_id", wk.ID, "err", err)
		}
	}
	writeJSON(w, code, out)
}

// shadowReport compares v1 trigger outcomes with v2 routing decisions for
// mirrored events (default: the last 24 hours).
func (a *API) shadowReport(w http.ResponseWriter, r *http.Request) {
	p, ok := principal(w, r)
	if !ok {
		return
	}
	if a.Ingestor == nil {
		writeErr(w, http.StatusServiceUnavailable, "disabled", "event ingestion is disabled")
		return
	}
	until := a.Clock.Now()
	since := until.Add(-24 * time.Hour)
	for name, dst := range map[string]*time.Time{"since": &since, "until": &until} {
		if v := r.URL.Query().Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				writeErr(w, http.StatusBadRequest, "invalid_request", name+" must be RFC 3339")
				return
			}
			*dst = t
		}
	}
	if !until.After(since) || until.Sub(since) > 31*24*time.Hour {
		writeErr(w, http.StatusBadRequest, "invalid_request", "since must be before until, at most 31 days apart")
		return
	}
	rep, err := a.Ingestor.ShadowReport(r.Context(), p.Tenant, since, until, 5000)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// postmortemFor loads a Work the caller may fully see and builds its report.
func (a *API) postmortemFor(w http.ResponseWriter, r *http.Request, act string) (auth.Principal, work.Work, postmortem.Report, bool) {
	p, ok := principal(w, r)
	if !ok {
		return p, work.Work{}, postmortem.Report{}, false
	}
	if a.Postmortems == nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "postmortems are not configured")
		return p, work.Work{}, postmortem.Report{}, false
	}
	wk, err := a.Store.GetWork(r.Context(), p.Tenant, r.PathValue("id"))
	if err != nil {
		a.fail(w, r, err)
		return p, wk, postmortem.Report{}, false
	}
	for _, ref := range wk.Goal.SubjectRefs {
		if !p.AllowsResource(ref) {
			a.outOfScope(w, r, p, act, "work", wk.ID, "the Work is outside the caller's scope")
			return p, wk, postmortem.Report{}, false
		}
	}
	rep, err := a.Postmortems.Build(r.Context(), p.Tenant, wk.ID)
	if err != nil {
		a.fail(w, r, err)
		return p, wk, rep, false
	}
	return p, wk, rep, true
}

// getPostmortem returns the deterministic ledger report as JSON, or as
// Markdown with ?format=markdown (or Accept: text/markdown).
func (a *API) getPostmortem(w http.ResponseWriter, r *http.Request) {
	_, _, rep, ok := a.postmortemFor(w, r, "work.postmortem")
	if !ok {
		return
	}
	if r.URL.Query().Get("format") == "markdown" || strings.Contains(r.Header.Get("Accept"), "text/markdown") {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, postmortem.Markdown(rep))
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

// createPostmortem opens a postmortem Work for a closed Work: the ledger
// report becomes its goal description and the draft is reviewed by a
// human. It is idempotent per source Work unless an Idempotency-Key is
// given.
func (a *API) createPostmortem(w http.ResponseWriter, r *http.Request) {
	p, src, rep, ok := a.postmortemFor(w, r, "work.create_postmortem")
	if !ok {
		return
	}
	if !src.Status.Terminal() {
		writeErr(w, http.StatusConflict, "work_open", "a postmortem needs a closed Work")
		return
	}
	if src.Template.ID == work.TemplatePostmortem || src.Template.ID == work.TemplateFollowUp {
		writeErr(w, http.StatusBadRequest, "invalid_request", "postmortem and follow-up Works do not get postmortems")
		return
	}
	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(key) > 200 {
		writeErr(w, http.StatusBadRequest, "invalid_request", "Idempotency-Key is limited to 200 chars")
		return
	}
	if key == "" {
		key = "postmortem:" + src.ID
	}
	title := "Postmortem: " + firstNonEmptyStr(src.Title, src.ID)
	if len(title) > 200 {
		title = title[:200]
	}
	req := orchestration.CreateRequest{TenantID: p.Tenant, ActorID: p.Subject, ActorType: "user", IdempotencyKey: key,
		Kind: work.KindRequest, Title: title, TemplateID: work.TemplatePostmortem,
		Goal: work.GoalSpec{Intent: "postmortem", Description: postmortem.Brief(rep, work.MaxDescription),
			SubjectRefs: src.Goal.SubjectRefs, Labels: map[string]string{"source_work": src.ID}}}
	if env := src.Goal.Labels["environment"]; env != "" {
		req.Goal.Labels["environment"] = env
	}
	wk, created, err := a.Service.Create(r.Context(), req)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
		a.audit(r.Context(), p, "work.create_postmortem", "work", wk.ID, "applied", map[string]any{"source_work": src.ID})
	}
	etag(w, wk.Version)
	w.Header().Set("Location", "/v1/works/"+wk.ID)
	writeJSON(w, code, wk)
}

func firstNonEmptyStr(xs ...string) string {
	for _, x := range xs {
		if x != "" {
			return x
		}
	}
	return ""
}
