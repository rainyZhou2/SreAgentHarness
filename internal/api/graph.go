package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/store/pg"
)

func (s *Server) graphSearch(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if s.Graph == nil {
		writeErr(w, http.StatusServiceUnavailable, "graph_unavailable", "graph service is not configured")
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	var kinds []string
	if raw := strings.TrimSpace(r.URL.Query().Get("kinds")); raw != "" {
		for _, k := range strings.Split(raw, ",") {
			if k = strings.TrimSpace(k); k != "" {
				kinds = append(kinds, k)
			}
		}
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	nodes, err := s.Graph.Search(r.Context(), id.tenant, q, kinds, limit)
	if err != nil {
		s.Log.Error("graph", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "cannot search the graph")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodes": nodes})
}

func (s *Server) graphNeighborhood(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if s.Graph == nil {
		writeErr(w, http.StatusServiceUnavailable, "graph_unavailable", "graph service is not configured")
		return
	}
	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref == "" {
		writeErr(w, http.StatusBadRequest, "invalid_body", "ref is required")
		return
	}
	depth, _ := strconv.Atoi(r.URL.Query().Get("depth"))
	out, err := s.Graph.Neighborhood(r.Context(), id.tenant, ref, depth)
	if err != nil {
		s.Log.Error("graph", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "cannot read the graph")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) capabilities(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if s.Graph == nil {
		writeErr(w, http.StatusServiceUnavailable, "graph_unavailable", "graph service is not configured")
		return
	}
	nodes, err := s.Graph.Capabilities(r.Context(), id.tenant)
	if err != nil {
		s.Log.Error("graph", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal", "cannot list capabilities")
		return
	}
	days, _ := strconv.Atoi(r.URL.Query().Get("days"))
	if days <= 0 || days > 365 {
		days = 30
	}
	usage, err := s.Graph.Usage(r.Context(), id.tenant, time.Now().AddDate(0, 0, -days), 100)
	if err != nil {
		s.Log.Warn("graph usage", "err", err)
		usage = nil
	}
	writeJSON(w, http.StatusOK, map[string]any{"capabilities": nodes, "usage": usage})
}

type relationBody struct {
	SrcRef   string         `json:"src_ref"`
	DstRef   string         `json:"dst_ref"`
	Relation string         `json:"relation"`
	Weight   float64        `json:"weight,omitempty"`
	Attrs    map[string]any `json:"attrs,omitempty"`
}

func (s *Server) graphRelate(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if s.Graph == nil {
		writeErr(w, http.StatusServiceUnavailable, "graph_unavailable", "graph service is not configured")
		return
	}
	var body relationBody
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&body); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_body", "src_ref, dst_ref and relation are required")
		return
	}
	rel := domain.CapabilityRelation{TenantID: id.tenant, SrcRef: body.SrcRef, DstRef: body.DstRef,
		Relation: body.Relation, Source: "declared", Weight: body.Weight, Attrs: body.Attrs}
	if err := s.Graph.Relate(r.Context(), rel); err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "invalid_relation", err.Error())
		return
	}
	s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor,
		Action: "graph.relate", ResourceType: "graph_relation", ResourceID: body.SrcRef,
		Outcome: "applied", Details: map[string]any{"dst_ref": body.DstRef, "relation": body.Relation}})
	writeJSON(w, http.StatusOK, rel)
}

func (s *Server) graphUnrelate(w http.ResponseWriter, r *http.Request) {
	id, ok := ident(w, r)
	if !ok {
		return
	}
	if s.Graph == nil {
		writeErr(w, http.StatusServiceUnavailable, "graph_unavailable", "graph service is not configured")
		return
	}
	q := r.URL.Query()
	src, dst, relation := q.Get("src"), q.Get("dst"), q.Get("relation")
	if err := s.Graph.Unrelate(r.Context(), id.tenant, src, dst, relation); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid_relation", err.Error())
		return
	}
	s.audit(r.Context(), pg.AuditEntry{TenantID: id.tenant, ActorType: "user", ActorID: id.actor,
		Action: "graph.unrelate", ResourceType: "graph_relation", ResourceID: src,
		Outcome: "applied", Details: map[string]any{"dst_ref": dst, "relation": relation}})
	w.WriteHeader(http.StatusNoContent)
}
