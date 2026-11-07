package localstore

import (
	"context"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
)

// capNodeRec keeps the tenant next to the node: CapabilityNode hides TenantID
// from JSON (it is implied by the table key in PostgreSQL).
type capNodeRec struct {
	TenantID string                `json:"tenant_id"`
	Node     domain.CapabilityNode `json:"node"`
}

type capRelRec struct {
	TenantID string                    `json:"tenant_id"`
	Rel      domain.CapabilityRelation `json:"rel"`
}

func capNodeKey(tenantID, ref string) string { return tenantID + "\x00" + ref }

func capRelKey(tenantID string, r domain.CapabilityRelation) string {
	return tenantID + "\x00" + r.SrcRef + "\x00" + r.DstRef + "\x00" + r.Relation + "\x00" + r.Source
}

func (s *Store) UpsertCapabilityNodes(ctx context.Context, nodes []domain.CapabilityNode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range nodes {
		n.SyncedAt = now()
		rec := capNodeRec{TenantID: n.TenantID, Node: n}
		s.capNodes[capNodeKey(n.TenantID, n.Ref)] = rec
		s.put("capnode", capNodeKey(n.TenantID, n.Ref), n.TenantID, 0, rec)
	}
	return nil
}

func (s *Store) CapabilityNodes(ctx context.Context, tenantID string) ([]domain.CapabilityNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.CapabilityNode
	for _, rec := range s.capNodes {
		if rec.TenantID == tenantID {
			out = append(out, rec.Node)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].Name < out[j].Name
		}
		return out[i].Kind < out[j].Kind
	})
	return out, nil
}

func (s *Store) UpsertCapabilityRelation(ctx context.Context, rel domain.CapabilityRelation) error {
	if rel.Source != "declared" && rel.Source != "derived" {
		return &domain.RejectedError{Reason: "capability_relation_source_invalid"}
	}
	if rel.Weight == 0 {
		rel.Weight = 1
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := capRelRec{TenantID: rel.TenantID, Rel: rel}
	s.capRels[capRelKey(rel.TenantID, rel)] = rec
	s.put("caprel", capRelKey(rel.TenantID, rel), rel.TenantID, 0, rec)
	return nil
}

func (s *Store) DeleteCapabilityRelation(ctx context.Context, tenantID, src, dst, relation, source string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, rec := range s.capRels {
		r := rec.Rel
		if rec.TenantID == tenantID && r.SrcRef == src && r.DstRef == dst &&
			r.Relation == relation && r.Source == source {
			delete(s.capRels, key)
			s.del("caprel", key)
		}
	}
	return nil
}

func (s *Store) CapabilityRelations(ctx context.Context, tenantID string) ([]domain.CapabilityRelation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.CapabilityRelation
	for _, rec := range s.capRels {
		if rec.TenantID == tenantID {
			out = append(out, rec.Rel)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SrcRef == out[j].SrcRef {
			return out[i].Relation < out[j].Relation
		}
		return out[i].SrcRef < out[j].SrcRef
	})
	return out, nil
}

// graphViewLocked projects runs, steps, capabilities, memories and services
// into a searchable graph. The projection is computed on demand; only
// capability nodes/relations are persisted because they come from sync.
func (s *Store) graphViewLocked(tenantID string) (map[string]domain.GraphNode, []domain.GraphEdge) {
	nodes := map[string]domain.GraphNode{}
	edges := []domain.GraphEdge{}
	addNode := func(n domain.GraphNode) {
		if n.Ref == "" {
			return
		}
		if old, ok := nodes[n.Ref]; ok && old.Kind != "" {
			return
		}
		nodes[n.Ref] = n
	}
	addEdge := func(src, relation, dst string) {
		edges = append(edges, domain.GraphEdge{Src: src, Relation: relation, Dst: dst, Source: "derived", Weight: 1})
	}
	for _, rec := range s.capNodes {
		if rec.TenantID != tenantID {
			continue
		}
		n := rec.Node
		addNode(domain.GraphNode{Ref: n.Ref, Kind: n.Kind, Label: n.Name, UpdatedAt: n.SyncedAt,
			Attrs: map[string]any{"version": n.Version, "risk": n.Risk, "trust": n.Trust,
				"source": n.Source, "service_id": n.ServiceID, "labels": n.Labels}})
		if n.ServiceID != "" {
			svc := "service:" + n.ServiceID
			addNode(domain.GraphNode{Ref: svc, Kind: "service", Label: n.ServiceID})
			addEdge(svc, "provides", n.Ref)
		}
	}
	for _, rec := range s.capRels {
		if rec.TenantID != tenantID {
			continue
		}
		addEdge(rec.Rel.SrcRef, rec.Rel.Relation, rec.Rel.DstRef)
	}
	for _, r := range s.runs {
		if r.TenantID != tenantID {
			continue
		}
		ref := "run:" + r.ID
		addNode(domain.GraphNode{Ref: ref, Kind: "run", Label: r.ID, UpdatedAt: r.UpdatedAt,
			Attrs: map[string]any{"status": string(r.Status), "profile_id": r.ProfileID,
				"profile_version": r.ProfileVersion}})
		if r.IncidentID != nil {
			inc := "incident:" + *r.IncidentID
			addNode(domain.GraphNode{Ref: inc, Kind: "incident", Label: *r.IncidentID})
			addEdge(ref, "part_of", inc)
		}
	}
	for _, st := range s.steps {
		if st.TenantID != tenantID {
			continue
		}
		runRef := "run:" + st.RunID
		ref := "step:" + st.ID
		addNode(domain.GraphNode{Ref: ref, Kind: "step", Label: st.InputRef, UpdatedAt: st.UpdatedAt,
			Attrs: map[string]any{"kind": string(st.Kind), "status": string(st.Status), "seq": st.Seq}})
		addEdge(runRef, "contains", ref)
		for _, cap := range s.caps[st.ID] {
			addNode(domain.GraphNode{Ref: cap.Ref, Kind: "capability", Label: cap.Ref})
			addEdge(ref, "used", cap.Ref)
		}
	}
	for _, rec := range s.memories {
		m := rec.Mem
		if m.TenantID != tenantID {
			continue
		}
		ref := "memory:" + m.ID
		addNode(domain.GraphNode{Ref: ref, Kind: "memory", Label: m.Title, UpdatedAt: m.UpdatedAt,
			Attrs: map[string]any{"scope": m.Scope, "scope_id": m.ScopeID, "status": m.Status, "trust": m.Trust}})
		if m.Scope == "service" && m.ScopeID != "" {
			addEdge("service:"+m.ScopeID, "has_memory", ref)
		}
		if m.Scope == "incident" && m.ScopeID != "" {
			addEdge("incident:"+m.ScopeID, "has_memory", ref)
		}
		for _, src := range m.SourceRefs {
			if strings.HasPrefix(src, "run:") {
				addEdge(src, "produced", ref)
			}
		}
		for _, sh := range s.shards[m.ID] {
			if strings.HasPrefix(sh.ShardKey, "capability:") {
				addEdge(ref, "about", strings.TrimPrefix(sh.ShardKey, "capability:"))
			}
		}
	}
	return nodes, edges
}

func (s *Store) GraphSearch(ctx context.Context, tenantID, q string, kinds []string, limit int) ([]domain.GraphNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 100 {
		limit = 40
	}
	kindSet := map[string]bool{}
	for _, k := range kinds {
		kindSet[k] = true
	}
	q = strings.ToLower(strings.TrimSpace(q))
	nodes, _ := s.graphViewLocked(tenantID)
	var out []domain.GraphNode
	for _, n := range nodes {
		if len(kindSet) > 0 && !kindSet[n.Kind] {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(n.Ref), q) && !strings.Contains(strings.ToLower(n.Label), q) {
			continue
		}
		out = append(out, n)
	}
	priority := func(kind string) int {
		switch kind {
		case "tool":
			return 0
		case "skill":
			return 1
		case "mcp_server":
			return 2
		case "service":
			return 3
		case "memory":
			return 4
		case "run":
			return 5
		case "step":
			return 6
		case "incident":
			return 7
		case "plugin":
			return 8
		default:
			return 9
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if priority(out[i].Kind) == priority(out[j].Kind) {
			return out[i].UpdatedAt.After(out[j].UpdatedAt)
		}
		return priority(out[i].Kind) < priority(out[j].Kind)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) GraphNeighborhood(ctx context.Context, tenantID, ref string, depth int) (domain.Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref == "" {
		return domain.Graph{}, &domain.RejectedError{Reason: "ref_required"}
	}
	if depth <= 0 || depth > 3 {
		depth = 2
	}
	nodes, edges := s.graphViewLocked(tenantID)
	if _, ok := nodes[ref]; !ok {
		return domain.Graph{}, nil
	}
	adj := map[string][]string{}
	for _, e := range edges {
		adj[e.Src] = append(adj[e.Src], e.Dst)
		adj[e.Dst] = append(adj[e.Dst], e.Src)
	}
	seen := map[string]bool{ref: true}
	frontier := []string{ref}
	for d := 0; d < depth; d++ {
		var next []string
		for _, cur := range frontier {
			for _, nb := range adj[cur] {
				if !seen[nb] {
					seen[nb] = true
					next = append(next, nb)
				}
			}
		}
		frontier = next
	}
	out := domain.Graph{}
	refs := make([]string, 0, len(seen))
	for r := range seen {
		refs = append(refs, r)
	}
	sort.Strings(refs)
	if len(refs) > 200 {
		refs = refs[:200]
		out.Truncated = true
	}
	in := map[string]bool{}
	for _, r := range refs {
		in[r] = true
		if n, ok := nodes[r]; ok {
			out.Nodes = append(out.Nodes, n)
		} else {
			out.Nodes = append(out.Nodes, domain.GraphNode{Ref: r, Kind: "unknown", Label: r})
		}
	}
	seenEdge := map[string]bool{}
	for _, e := range edges {
		if !in[e.Src] || !in[e.Dst] {
			continue
		}
		key := e.Src + "\x00" + e.Relation + "\x00" + e.Dst + "\x00" + e.Source
		if seenEdge[key] {
			continue
		}
		seenEdge[key] = true
		if len(out.Edges) >= 500 {
			out.Truncated = true
			break
		}
		out.Edges = append(out.Edges, e)
	}
	return out, nil
}

func (s *Store) CapabilityUsage(ctx context.Context, tenantID string, since time.Time, limit int) (map[string]domain.GraphNode, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	type usage struct {
		count int
		last  time.Time
	}
	byRef := map[string]usage{}
	for stepID, refs := range s.caps {
		st, ok := s.steps[stepID]
		if !ok || st.TenantID != tenantID || st.UpdatedAt.Before(since) {
			continue
		}
		for _, cap := range refs {
			if cap.Role != "" && cap.Role != "used" {
				continue
			}
			u := byRef[cap.Ref]
			u.count++
			if st.UpdatedAt.After(u.last) {
				u.last = st.UpdatedAt
			}
			byRef[cap.Ref] = u
		}
	}
	type named struct {
		ref string
		u   usage
	}
	list := make([]named, 0, len(byRef))
	for ref, u := range byRef {
		list = append(list, named{ref, u})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].u.count == list[j].u.count {
			return list[i].u.last.After(list[j].u.last)
		}
		return list[i].u.count > list[j].u.count
	})
	if len(list) > limit {
		list = list[:limit]
	}
	out := map[string]domain.GraphNode{}
	for _, item := range list {
		out[item.ref] = domain.GraphNode{Ref: item.ref, Kind: "capability", Label: item.ref,
			UpdatedAt: item.u.last, Attrs: map[string]any{"uses": item.u.count}}
	}
	return out, nil
}

func (s *Store) ServiceCapabilityNotes(ctx context.Context, tenantID string, services []string, limit int) ([]domain.CapabilityNote, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(services) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	wantSvc := map[string]bool{}
	wantRef := map[string]bool{}
	for _, svc := range services {
		wantSvc[svc] = true
		wantRef["service:"+svc] = true
	}
	related := map[string]bool{}
	for _, rec := range s.capRels {
		if rec.TenantID != tenantID {
			continue
		}
		if wantRef[rec.Rel.SrcRef] {
			related[rec.Rel.DstRef] = true
		}
		if wantRef[rec.Rel.DstRef] {
			related[rec.Rel.SrcRef] = true
		}
	}
	var out []domain.CapabilityNote
	for _, rec := range s.capNodes {
		n := rec.Node
		if rec.TenantID != tenantID {
			continue
		}
		if !wantSvc[n.ServiceID] && !related[n.Ref] {
			continue
		}
		note := domain.CapabilityNote{Node: n}
		var lessonAt time.Time
		for _, mem := range s.memories {
			m := mem.Mem
			if m.TenantID != tenantID || m.Status != "ACTIVE" || m.Sensitivity == "restricted" || m.Trust == "T4" {
				continue
			}
			if m.ExpiresAt != nil && !m.ExpiresAt.After(now()) {
				continue
			}
			for _, sh := range s.shards[m.ID] {
				if sh.ShardKey == "capability:"+n.Ref && m.UpdatedAt.After(lessonAt) {
					note.Lesson = m.Title
					lessonAt = m.UpdatedAt
				}
			}
		}
		out = append(out, note)
	}
	priority := func(kind string) int {
		switch kind {
		case "tool":
			return 0
		case "skill":
			return 1
		case "mcp_server":
			return 2
		default:
			return 3
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if priority(out[i].Node.Kind) == priority(out[j].Node.Kind) {
			return out[i].Node.Name < out[j].Node.Name
		}
		return priority(out[i].Node.Kind) < priority(out[j].Node.Kind)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// noteTime removed: the lesson lookup keeps the newest matching memory.
