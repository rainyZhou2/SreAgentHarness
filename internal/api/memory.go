package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/curate"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
	"go-sre-harness/internal/store/pg"
)

type MemoryStore interface {
	CreateMemory(ctx context.Context, m memory.NewMemory) (memory.Memory, bool, error)
	TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error)
	GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error)
	ListMemories(ctx context.Context, tenantID string, f pg.MemoryFilter) ([]memory.Memory, error)
	PutServiceDependency(ctx context.Context, tenantID, service, dependsOn, relation string) error
	PutFeedback(ctx context.Context, tenantID, runID string, f memory.Feedback) error
	ListFeedback(ctx context.Context, tenantID, runID string) ([]memory.Feedback, error)
	ListExperiences(ctx context.Context, tenantID, runID string) ([]pg.ExperienceView, error)
	AppendIncidentEvent(ctx context.Context, tenantID, incidentID string, e pg.IncidentEvent, expected *int64) (int64, error)
	ListIncidentEvents(ctx context.Context, tenantID, incidentID string, upTo int64) ([]pg.IncidentEvent, error)
	CreateIncident(ctx context.Context, tenantID, serviceID, environment string) (string, error)
	UpdateMemory(ctx context.Context, r memory.Revision) (memory.Memory, error)
	LiveMemoryBySubject(ctx context.Context, tenantID, scope, scopeID, subject string) (memory.Memory, bool, error)
	PersonalMemories(ctx context.Context, tenantID, userID string, limit int) ([]memory.Memory, error)
}

// MemoryAPI exposes the memory lifecycle, feedback and incident working memory.
type MemoryAPI struct {
	Store  MemoryStore
	Writer memory.Writer
	Server *Server
	// Curator enables the memory curation routes (nil: not served).
	Curator *curate.Curator
}

func (m *MemoryAPI) RouteRoles() map[string]auth.Role {
	roles := map[string]auth.Role{
		"POST /v1/memories":                              auth.Operator,
		"POST /v1/memories/{memory_id}/transition":       auth.Approver,
		"PATCH /v1/memories/{memory_id}":                 auth.Approver,
		"PUT /v1/me/memories/{key}":                      auth.Viewer,
		"DELETE /v1/me/memories/{key}":                   auth.Viewer,
		"POST /v1/service-dependencies":                  auth.Admin,
		"POST /v1/runs/{run_id}/feedback":                auth.Operator,
		"POST /v1/incidents":                             auth.Operator,
		"POST /v1/incidents/{incident_id}/memory-events": auth.Operator,
	}
	for k, v := range m.curationRoutes() {
		roles[k] = v
	}
	return roles
}

func (m *MemoryAPI) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/memories", m.create)
	mux.HandleFunc("GET /v1/memories", m.list)
	mux.HandleFunc("GET /v1/memories/{memory_id}", m.get)
	mux.HandleFunc("POST /v1/memories/{memory_id}/transition", m.transition)
	mux.HandleFunc("PATCH /v1/memories/{memory_id}", m.update)
	mux.HandleFunc("GET /v1/me/memories", m.listMine)
	mux.HandleFunc("PUT /v1/me/memories/{key}", m.putMine)
	mux.HandleFunc("DELETE /v1/me/memories/{key}", m.deleteMine)
	mux.HandleFunc("POST /v1/service-dependencies", m.putDependency)
	mux.HandleFunc("POST /v1/runs/{run_id}/feedback", m.feedback)
	mux.HandleFunc("GET /v1/runs/{run_id}/feedback", m.listFeedback)
	mux.HandleFunc("GET /v1/experiences", m.experiences)
	mux.HandleFunc("POST /v1/incidents", m.createIncident)
	mux.HandleFunc("POST /v1/incidents/{incident_id}/memory-events", m.appendIncident)
	mux.HandleFunc("GET /v1/incidents/{incident_id}/memory-events", m.listIncident)
	m.registerCuration(mux)
}

func (m *MemoryAPI) create(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Scope        string   `json:"scope"`
		ScopeID      string   `json:"scope_id"`
		Type         string   `json:"memory_type"`
		Subject      string   `json:"subject"`
		Title        string   `json:"title"`
		Content      string   `json:"content"`
		EvidenceRefs []string `json:"evidence_refs"`
		Sensitivity  string   `json:"sensitivity"`
		Confidence   *float64 `json:"confidence"`
		TTLHours     int      `json:"ttl_hours"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&b); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if b.Scope == "user" {
		writeErr(w, http.StatusBadRequest, "invalid_scope", "personal memory is written with PUT /v1/me/memories/{key}")
		return
	}
	nm := memory.NewMemory{TenantID: id.tenant, Scope: b.Scope, ScopeID: b.ScopeID, Type: b.Type, Subject: b.Subject,
		Title: b.Title, Content: b.Content, EvidenceRefs: b.EvidenceRefs, Sensitivity: b.Sensitivity,
		Confidence: b.Confidence, CreatedBy: id.actor, SourceRefs: []string{"user:" + id.actor},
		Status: "CANDIDATE", Trust: "T3"} // human-written memories still go through validation
	if b.TTLHours > 0 {
		t := time.Now().Add(time.Duration(b.TTLHours) * time.Hour)
		nm.ExpiresAt = &t
	}
	prepared, err := m.Writer.Prepare(r.Context(), nm)
	if errors.Is(err, memory.ErrPoisoned) {
		writeErr(w, http.StatusUnprocessableEntity, "memory_rejected_poisoning", err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_memory", err.Error())
		return
	}
	mem, created, err := m.Store.CreateMemory(r.Context(), prepared)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"memory": mem, "created": created, "redactions": prepared.Redactions})
}

func (m *MemoryAPI) list(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	list, err := m.Store.ListMemories(r.Context(), id.tenant, pg.MemoryFilter{Status: q.Get("status"), Scope: q.Get("scope"),
		ScopeID: q.Get("scope_id"), Query: q.Get("q"), Limit: limit})
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	visible := make([]memory.Memory, 0, len(list)) // [] rather than null when empty
	for _, mem := range list {
		if canSeeMemory(r, id, mem) {
			visible = append(visible, mem)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"memories": visible})
}

func (m *MemoryAPI) get(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	mem, err := m.Store.GetMemory(r.Context(), id.tenant, r.PathValue("memory_id"))
	if err == nil && !canSeeMemory(r, id, mem) {
		err = domain.ErrNotFound
	}
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mem)
}

// canSeeMemory hides personal memories from everyone but their owner and
// tenant admins.
func canSeeMemory(r *http.Request, id identity, mem memory.Memory) bool {
	if mem.Scope != "user" || mem.ScopeID == id.actor {
		return true
	}
	p, _ := auth.FromContext(r.Context())
	return p.Has(auth.Admin)
}

// update writes a new version of a memory's title and/or content. The
// memory keeps its status; citations of the old version keep resolving.
func (m *MemoryAPI) update(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		ExpectedVersion int64    `json:"expected_version"`
		Title           string   `json:"title"`
		Content         string   `json:"content"`
		EvidenceRefs    []string `json:"evidence_refs"`
		Reason          string   `json:"reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&b); err != nil || b.ExpectedVersion <= 0 ||
		(b.Title == "" && b.Content == "") {
		writeErr(w, http.StatusBadRequest, "invalid_body", "expected_version and a title or content are required")
		return
	}
	cur, err := m.Store.GetMemory(r.Context(), id.tenant, r.PathValue("memory_id"))
	if err == nil && cur.Scope == "user" && cur.ScopeID != id.actor {
		err = domain.ErrNotFound
	}
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	m.revise(w, r, id, cur, b.ExpectedVersion, b.Title, b.Content, b.EvidenceRefs, b.Reason)
}

func (m *MemoryAPI) revise(w http.ResponseWriter, r *http.Request, id identity, cur memory.Memory, expected int64,
	title, content string, evidence []string, reason string) {
	prepared, err := m.Writer.PrepareRevision(r.Context(), cur, title, content, evidence, "user:"+id.actor)
	if errors.Is(err, memory.ErrPoisoned) {
		writeErr(w, http.StatusUnprocessableEntity, "memory_rejected_poisoning", err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_memory", err.Error())
		return
	}
	mem, err := m.Store.UpdateMemory(r.Context(), memory.Revision{TenantID: id.tenant, ID: cur.ID, ExpectedVersion: expected,
		Prepared: prepared, Actor: id.actor, Reason: reason})
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"memory": mem, "redactions": prepared.Redactions})
}

// listMine returns the caller's personal memory.
func (m *MemoryAPI) listMine(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	list, err := m.Store.PersonalMemories(r.Context(), id.tenant, id.actor, 200)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	if list == nil {
		list = []memory.Memory{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"memories": list})
}

// putMine sets one of the caller's preferences: a new personal memory, or
// a new version of the existing one with that key.
func (m *MemoryAPI) putMine(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Value string `json:"value"`
	}
	key := strings.TrimSpace(r.PathValue("key"))
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || key == "" ||
		strings.TrimSpace(b.Value) == "" || utf8.RuneCountInString(key) > domain.MaxNoteKeyRunes ||
		utf8.RuneCountInString(b.Value) > 4*domain.MaxNoteValueRunes {
		writeErr(w, http.StatusBadRequest, "invalid_body", "a key (≤64 characters) and a value (≤2000) are required")
		return
	}
	cur, found, err := m.Store.LiveMemoryBySubject(r.Context(), id.tenant, "user", id.actor, memory.PersonalSubject(key))
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	if found && cur.Status == "ACTIVE" {
		m.revise(w, r, id, cur, cur.Version, key, b.Value, nil, "set by the user")
		return
	}
	prepared, err := m.Writer.Prepare(r.Context(), memory.NewMemory{TenantID: id.tenant, Scope: "user", ScopeID: id.actor,
		Type: "semantic", Status: "ACTIVE", Subject: memory.PersonalSubject(key), Title: key, Content: b.Value,
		SourceRefs: []string{"user:" + id.actor}, Trust: "T2", Sensitivity: "restricted", CreatedBy: id.actor})
	if errors.Is(err, memory.ErrPoisoned) {
		writeErr(w, http.StatusUnprocessableEntity, "memory_rejected_poisoning", err.Error())
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_memory", err.Error())
		return
	}
	mem, created, err := m.Store.CreateMemory(r.Context(), prepared)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	writeJSON(w, code, map[string]any{"memory": mem, "created": created, "redactions": prepared.Redactions})
}

// deleteMine archives one of the caller's preferences.
func (m *MemoryAPI) deleteMine(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	cur, found, err := m.Store.LiveMemoryBySubject(r.Context(), id.tenant, "user", id.actor,
		memory.PersonalSubject(r.PathValue("key")))
	if err == nil && !found {
		err = domain.ErrNotFound
	}
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	to := "ARCHIVED"
	if cur.Status != "ACTIVE" {
		to = "REJECTED"
	}
	mem, err := m.Store.TransitionMemory(r.Context(), id.tenant, cur.ID, cur.Status, to, id.actor, "deleted by the user")
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mem)
}

func (m *MemoryAPI) transition(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		From   string `json:"expected_status"`
		To     string `json:"to"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || b.To == "" || b.From == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "expected_status and to are required")
		return
	}
	if cur, err := m.Store.GetMemory(r.Context(), id.tenant, r.PathValue("memory_id")); err != nil || !canSeeMemory(r, id, cur) {
		m.Server.domainErr(w, domain.ErrNotFound)
		return
	}
	mem, err := m.Store.TransitionMemory(r.Context(), id.tenant, r.PathValue("memory_id"), b.From, b.To, id.actor, b.Reason)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, mem)
}

func (m *MemoryAPI) putDependency(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Service   string `json:"service"`
		DependsOn string `json:"depends_on"`
		Relation  string `json:"relation"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || b.Service == "" || b.DependsOn == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "service and depends_on are required")
		return
	}
	if err := m.Store.PutServiceDependency(r.Context(), id.tenant, b.Service, b.DependsOn, b.Relation); err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

func (m *MemoryAPI) feedback(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	run, err := m.Server.Store.GetRun(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	if !run.Status.Terminal() {
		writeErr(w, http.StatusConflict, "run_not_finished", "feedback is accepted for finished runs only")
		return
	}
	var f memory.Feedback
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&f); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", err.Error())
		return
	}
	if f.Rating != nil && (*f.Rating < 1 || *f.Rating > 5) {
		writeErr(w, http.StatusBadRequest, "invalid_rating", "rating must be 1..5")
		return
	}
	f.ActorID = id.actor
	if f.Notes != nil {
		red, _ := memory.Redact(*f.Notes)
		f.Notes = &red
	}
	if err := m.Store.PutFeedback(r.Context(), id.tenant, run.ID, f); err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, f)
}

func (m *MemoryAPI) listFeedback(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	list, err := m.Store.ListFeedback(r.Context(), id.tenant, r.PathValue("run_id"))
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"feedback": list})
}

func (m *MemoryAPI) experiences(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	list, err := m.Store.ListExperiences(r.Context(), id.tenant, r.URL.Query().Get("run_id"))
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"experiences": list})
}

func (m *MemoryAPI) createIncident(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		Service     string `json:"service"`
		Environment string `json:"environment"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&b); err != nil || b.Service == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "service is required")
		return
	}
	if b.Environment == "" {
		b.Environment = "production"
	}
	incID, err := m.Store.CreateIncident(r.Context(), id.tenant, b.Service, b.Environment)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"incident_id": incID})
}

func (m *MemoryAPI) appendIncident(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	var b struct {
		EventType       string          `json:"event_type"`
		Payload         json.RawMessage `json:"payload"`
		ExpectedVersion *int64          `json:"expected_version"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&b); err != nil || b.EventType == "" ||
		!json.Valid(b.Payload) {
		writeErr(w, http.StatusBadRequest, "invalid_body", "event_type and JSON payload are required")
		return
	}
	switch b.EventType {
	case "evidence_added", "hypothesis_updated", "decision_recorded", "action_taken", "fact_confirmed":
	default:
		writeErr(w, http.StatusBadRequest, "invalid_event_type", "unsupported event_type")
		return
	}
	red, _ := memory.Redact(string(b.Payload))
	actor := id.actor
	v, err := m.Store.AppendIncidentEvent(r.Context(), id.tenant, r.PathValue("incident_id"), pg.IncidentEvent{
		EventType: b.EventType, ActorType: "user", ActorID: &actor, Payload: json.RawMessage(red), Trust: "T1"}, b.ExpectedVersion)
	if err != nil {
		if errors.Is(err, domain.ErrVersionConflict) {
			writeErr(w, http.StatusConflict, "version_conflict", "incident memory changed")
			return
		}
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]int64{"version": v})
}

func (m *MemoryAPI) listIncident(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	upTo, _ := strconv.ParseInt(r.URL.Query().Get("up_to"), 10, 64)
	list, err := m.Store.ListIncidentEvents(r.Context(), id.tenant, r.PathValue("incident_id"), upTo)
	if err != nil {
		m.Server.domainErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": list})
}
