package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/domain"
)

const (
	maxGraphNodes  = 200
	maxGraphEdges  = 500
	maxThoughts    = 10
	maxCapsPerStep = 20
)

func (s *Store) UpsertCapabilityNodes(ctx context.Context, nodes []domain.CapabilityNode) error {
	if len(nodes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, n := range nodes {
		batch.Queue(`INSERT INTO capability_nodes (tenant_id, ref, kind, name, version, owner, source, risk, trust,
			service_id, labels, synced_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,now())
			ON CONFLICT (tenant_id, ref) DO UPDATE SET kind=EXCLUDED.kind, name=EXCLUDED.name,
				version=EXCLUDED.version, owner=EXCLUDED.owner, source=EXCLUDED.source, risk=EXCLUDED.risk,
				trust=EXCLUDED.trust, service_id=EXCLUDED.service_id, labels=EXCLUDED.labels, synced_at=now()`,
			n.TenantID, n.Ref, n.Kind, n.Name, n.Version, n.Owner, n.Source, n.Risk, n.Trust, n.ServiceID,
			jsonb(n.Labels))
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range nodes {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("upsert capability node: %w", err)
		}
	}
	return nil
}

func (s *Store) CapabilityNodes(ctx context.Context, tenantID string) ([]domain.CapabilityNode, error) {
	rows, err := s.pool.Query(ctx, `SELECT ref, kind, name, version, owner, source, risk, trust, service_id, labels, synced_at
		FROM capability_nodes WHERE tenant_id=$1 ORDER BY kind, name`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.CapabilityNode
	for rows.Next() {
		n := domain.CapabilityNode{TenantID: tenantID}
		var labels []byte
		if err := rows.Scan(&n.Ref, &n.Kind, &n.Name, &n.Version, &n.Owner, &n.Source, &n.Risk, &n.Trust,
			&n.ServiceID, &labels, &n.SyncedAt); err != nil {
			return nil, err
		}
		n.Labels = decodeMap(labels)
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) UpsertCapabilityRelation(ctx context.Context, rel domain.CapabilityRelation) error {
	if rel.Source != "declared" && rel.Source != "derived" {
		return errors.New("capability relation source must be declared or derived")
	}
	if rel.Weight == 0 {
		rel.Weight = 1
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO capability_relations (tenant_id, src_ref, dst_ref, relation, source, weight, attrs)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (tenant_id, src_ref, dst_ref, relation, source)
		DO UPDATE SET weight=EXCLUDED.weight, attrs=EXCLUDED.attrs`,
		rel.TenantID, rel.SrcRef, rel.DstRef, rel.Relation, rel.Source, rel.Weight, jsonb(rel.Attrs))
	return err
}

func (s *Store) CapabilityRelations(ctx context.Context, tenantID string) ([]domain.CapabilityRelation, error) {
	rows, err := s.pool.Query(ctx, `SELECT src_ref, dst_ref, relation, source, weight, attrs
		FROM capability_relations WHERE tenant_id=$1 ORDER BY src_ref, relation`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.CapabilityRelation
	for rows.Next() {
		rel := domain.CapabilityRelation{TenantID: tenantID}
		var attrs []byte
		if err := rows.Scan(&rel.SrcRef, &rel.DstRef, &rel.Relation, &rel.Source, &rel.Weight, &attrs); err != nil {
			return nil, err
		}
		rel.Attrs = decodeMap(attrs)
		out = append(out, rel)
	}
	return out, rows.Err()
}

func (s *Store) DeleteCapabilityRelation(ctx context.Context, tenantID, src, dst, relation, source string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM capability_relations
		WHERE tenant_id=$1 AND src_ref=$2 AND dst_ref=$3 AND relation=$4 AND source=$5`,
		tenantID, src, dst, relation, source)
	return err
}

func (s *Store) GraphSearch(ctx context.Context, tenantID, q string, kinds []string, limit int) ([]domain.GraphNode, error) {
	if limit <= 0 || limit > 100 {
		limit = 40
	}
	rows, err := s.pool.Query(ctx, `SELECT ref, kind, label, attrs, updated_at FROM graph_nodes
		WHERE tenant_id=$1 AND ($2='' OR label ILIKE '%'||$2||'%' OR ref ILIKE '%'||$2||'%')
		  AND (COALESCE(cardinality($3::text[]),0)=0 OR kind = ANY($3))
		ORDER BY CASE kind
			WHEN 'tool' THEN 0 WHEN 'skill' THEN 1 WHEN 'mcp_server' THEN 2 WHEN 'service' THEN 3
			WHEN 'memory' THEN 4 WHEN 'run' THEN 5 WHEN 'step' THEN 6 WHEN 'thought' THEN 7
			WHEN 'incident' THEN 8 WHEN 'plugin' THEN 9 WHEN 'shard' THEN 10 ELSE 11 END,
		  updated_at DESC LIMIT $4`, tenantID, q, kinds, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.GraphNode
	for rows.Next() {
		var n domain.GraphNode
		var attrs []byte
		if err := rows.Scan(&n.Ref, &n.Kind, &n.Label, &attrs, &n.UpdatedAt); err != nil {
			return nil, err
		}
		n.Attrs = decodeMap(attrs)
		out = append(out, n)
	}
	return out, rows.Err()
}

// GraphNeighborhood walks at most depth edges from ref. The node and edge caps
// are part of the API contract so one focused node cannot exhaust the client.
func (s *Store) GraphNeighborhood(ctx context.Context, tenantID, ref string, depth int) (domain.Graph, error) {
	if ref == "" {
		return domain.Graph{}, errors.New("ref is required")
	}
	if depth <= 0 || depth > 3 {
		depth = 2
	}
	rows, err := s.pool.Query(ctx, `WITH RECURSIVE walk(ref, depth) AS (
			SELECT $2::text, 0
			WHERE EXISTS (SELECT 1 FROM graph_nodes n WHERE n.tenant_id = $1 AND n.ref = $2)
			UNION
			SELECT CASE WHEN e.src = w.ref THEN e.dst ELSE e.src END, w.depth + 1
			FROM graph_edges e JOIN walk w ON e.src = w.ref OR e.dst = w.ref
			WHERE e.tenant_id = $1 AND w.depth < $3
		)
		SELECT DISTINCT ref FROM walk LIMIT $4`, tenantID, ref, depth, maxGraphNodes+1)
	if err != nil {
		return domain.Graph{}, err
	}
	refs := make([]string, 0, maxGraphNodes+1)
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			rows.Close()
			return domain.Graph{}, err
		}
		refs = append(refs, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.Graph{}, err
	}
	rows.Close()

	out := domain.Graph{Truncated: len(refs) > maxGraphNodes}
	if out.Truncated {
		refs = refs[:maxGraphNodes]
	}
	if len(refs) == 0 {
		return out, nil
	}

	nodes, err := s.pool.Query(ctx, `SELECT ref, kind, label, attrs, updated_at FROM graph_nodes
		WHERE tenant_id=$1 AND ref = ANY($2)`, tenantID, refs)
	if err != nil {
		return domain.Graph{}, err
	}
	seen := map[string]bool{}
	for nodes.Next() {
		var n domain.GraphNode
		var attrs []byte
		if err := nodes.Scan(&n.Ref, &n.Kind, &n.Label, &attrs, &n.UpdatedAt); err != nil {
			nodes.Close()
			return domain.Graph{}, err
		}
		n.Attrs = decodeMap(attrs)
		out.Nodes = append(out.Nodes, n)
		seen[n.Ref] = true
	}
	if err := nodes.Err(); err != nil {
		nodes.Close()
		return domain.Graph{}, err
	}
	nodes.Close()
	for _, r := range refs {
		if !seen[r] {
			out.Nodes = append(out.Nodes, domain.GraphNode{Ref: r, Kind: "unknown", Label: r})
		}
	}

	edges, err := s.pool.Query(ctx, `SELECT src, relation, dst, source, weight, attrs FROM graph_edges
		WHERE tenant_id=$1 AND src = ANY($2) AND dst = ANY($2)
		LIMIT $3`, tenantID, refs, maxGraphEdges+1)
	if err != nil {
		return domain.Graph{}, err
	}
	defer edges.Close()
	seenEdges := map[string]bool{}
	for edges.Next() {
		var e domain.GraphEdge
		var attrs []byte
		if err := edges.Scan(&e.Src, &e.Relation, &e.Dst, &e.Source, &e.Weight, &attrs); err != nil {
			return domain.Graph{}, err
		}
		key := e.Src + "\x00" + e.Relation + "\x00" + e.Dst + "\x00" + e.Source
		if seenEdges[key] {
			continue
		}
		seenEdges[key] = true
		e.Attrs = decodeMap(attrs)
		if len(out.Edges) >= maxGraphEdges {
			out.Truncated = true
			break
		}
		out.Edges = append(out.Edges, e)
	}
	return out, edges.Err()
}

func (s *Store) ServiceCapabilityNotes(ctx context.Context, tenantID string, services []string, limit int) ([]domain.CapabilityNote, error) {
	if len(services) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 10 {
		limit = 5
	}
	refs := make([]string, 0, len(services))
	for _, svc := range services {
		refs = append(refs, "service:"+svc)
	}
	rows, err := s.pool.Query(ctx, `SELECT c.ref, c.kind, c.name, c.version, c.owner, c.source, c.risk, c.trust,
			c.service_id, c.labels, c.synced_at, COALESCE(l.title, '')
		FROM capability_nodes c
		LEFT JOIN LATERAL (
			SELECT m.title FROM memory_shards s
			JOIN agent_memories m ON m.tenant_id = s.tenant_id AND m.id = s.memory_id
			WHERE s.tenant_id = c.tenant_id AND s.shard_key = 'capability:' || c.ref
			  AND m.status = 'ACTIVE' AND m.sensitivity <> 'restricted' AND m.trust <> 'T4'
			  AND (m.expires_at IS NULL OR m.expires_at > now())
			ORDER BY m.updated_at DESC LIMIT 1
		) l ON true
		WHERE c.tenant_id=$1 AND (
			c.service_id = ANY($2)
			OR EXISTS (
				SELECT 1 FROM capability_relations r
				WHERE r.tenant_id = c.tenant_id
				  AND (r.src_ref = c.ref OR r.dst_ref = c.ref)
				  AND (r.src_ref = ANY($3) OR r.dst_ref = ANY($3))
			)
		)
		ORDER BY CASE c.kind WHEN 'tool' THEN 0 WHEN 'skill' THEN 1 WHEN 'mcp_server' THEN 2 ELSE 3 END,
			c.name LIMIT $4`, tenantID, services, refs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.CapabilityNote
	for rows.Next() {
		n := domain.CapabilityNote{Node: domain.CapabilityNode{TenantID: tenantID}}
		var labels []byte
		if err := rows.Scan(&n.Node.Ref, &n.Node.Kind, &n.Node.Name, &n.Node.Version, &n.Node.Owner,
			&n.Node.Source, &n.Node.Risk, &n.Node.Trust, &n.Node.ServiceID, &labels, &n.Node.SyncedAt,
			&n.Lesson); err != nil {
			return nil, err
		}
		n.Node.Labels = decodeMap(labels)
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) CapabilityUsage(ctx context.Context, tenantID string, since time.Time, limit int) (map[string]domain.GraphNode, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `SELECT capability_ref, max(created_at), count(*)
		FROM step_capability_refs WHERE tenant_id=$1 AND created_at >= $2
		GROUP BY capability_ref ORDER BY 2 DESC LIMIT $3`, tenantID, since, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]domain.GraphNode{}
	for rows.Next() {
		var ref string
		var last time.Time
		var n int64
		if err := rows.Scan(&ref, &last, &n); err != nil {
			return nil, err
		}
		out[ref] = domain.GraphNode{Ref: ref, Kind: "capability", Label: ref,
			Attrs: map[string]any{"last_used": last, "uses": n}, UpdatedAt: last}
	}
	return out, rows.Err()
}

func (s *Store) AttachMemoryShards(ctx context.Context, tenantID, memoryID string, shards []domain.MemoryShard) error {
	if len(shards) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	queued := 0
	for _, sh := range shards {
		if sh.ShardKey == "" {
			continue
		}
		if sh.Weight == 0 {
			sh.Weight = 1
		}
		batch.Queue(`INSERT INTO memory_shards (tenant_id, memory_id, shard_key, weight, source)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT (tenant_id, memory_id, shard_key)
			DO UPDATE SET weight=GREATEST(memory_shards.weight, EXCLUDED.weight)`,
			tenantID, memoryID, sh.ShardKey, sh.Weight, sh.Source)
		queued++
	}
	if queued == 0 {
		return nil
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < queued; i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("attach memory shard: %w", err)
		}
	}
	return nil
}

func (s *Store) MemoryShards(ctx context.Context, tenantID, memoryID string) ([]domain.MemoryShard, error) {
	rows, err := s.pool.Query(ctx, `SELECT memory_id, shard_key, weight, source FROM memory_shards
		WHERE tenant_id=$1 AND memory_id=$2 ORDER BY weight DESC, shard_key`, tenantID, memoryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.MemoryShard
	for rows.Next() {
		var sh domain.MemoryShard
		if err := rows.Scan(&sh.MemoryID, &sh.ShardKey, &sh.Weight, &sh.Source); err != nil {
			return nil, err
		}
		out = append(out, sh)
	}
	return out, rows.Err()
}

func (s *Store) RunTrace(ctx context.Context, tenantID, runID string) (map[string][]domain.Thought, map[string][]domain.CapabilityRef, error) {
	thoughts := map[string][]domain.Thought{}
	rows, err := s.pool.Query(ctx, `SELECT id, step_id, seq, kind, summary, detail_ref, status, confidence, structured, attrs
		FROM run_thoughts WHERE tenant_id=$1 AND run_id=$2 ORDER BY step_id, seq`, tenantID, runID)
	if err != nil {
		return nil, nil, err
	}
	byID := map[string]*domain.Thought{}
	for rows.Next() {
		var th domain.Thought
		var stepID, kind, status string
		var conf *float64
		var attrs []byte
		if err := rows.Scan(&th.ID, &stepID, &th.Seq, &kind, &th.Summary, &th.DetailRef, &status, &conf,
			&th.Structured, &attrs); err != nil {
			rows.Close()
			return nil, nil, err
		}
		th.Kind, th.Status = domain.ThoughtKind(kind), domain.ThoughtStatus(status)
		if conf != nil {
			th.Confidence = *conf
		}
		th.Attrs = decodeMap(attrs)
		thoughts[stepID] = append(thoughts[stepID], th)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	rows.Close()
	// Refs are attached after scanning so each thought pointer stays valid.
	for stepID, list := range thoughts {
		for i := range list {
			byID[list[i].ID] = &thoughts[stepID][i]
		}
	}
	refs, err := s.pool.Query(ctx, `SELECT r.thought_id, r.ref, r.relation FROM thought_refs r
		JOIN run_thoughts t ON t.tenant_id = r.tenant_id AND t.id = r.thought_id
		WHERE r.tenant_id=$1 AND t.run_id=$2 ORDER BY r.thought_id, r.relation, r.ref`, tenantID, runID)
	if err != nil {
		return nil, nil, err
	}
	for refs.Next() {
		var id, ref, relation string
		if err := refs.Scan(&id, &ref, &relation); err != nil {
			refs.Close()
			return nil, nil, err
		}
		if th := byID[id]; th != nil {
			th.Refs = append(th.Refs, domain.ThoughtRef{Ref: ref, Relation: relation})
		}
	}
	if err := refs.Err(); err != nil {
		refs.Close()
		return nil, nil, err
	}
	refs.Close()

	caps := map[string][]domain.CapabilityRef{}
	rows, err = s.pool.Query(ctx, `SELECT step_id, capability_ref, role FROM step_capability_refs
		WHERE tenant_id=$1 AND run_id=$2 ORDER BY step_id, role, capability_ref`, tenantID, runID)
	if err != nil {
		return thoughts, caps, err
	}
	defer rows.Close()
	for rows.Next() {
		var stepID string
		var ref domain.CapabilityRef
		if err := rows.Scan(&stepID, &ref.Ref, &ref.Role); err != nil {
			return thoughts, caps, err
		}
		caps[stepID] = append(caps[stepID], ref)
	}
	return thoughts, caps, rows.Err()
}

func insertThoughts(ctx context.Context, tx pgx.Tx, tenantID, runID, stepID string, thoughts []domain.Thought) error {
	if len(thoughts) > maxThoughts {
		thoughts = thoughts[:maxThoughts]
	}
	for i, th := range thoughts {
		th.ID = newID()
		seq := th.Seq
		if seq == 0 {
			seq = i + 1
		}
		status := th.Status
		if status == "" {
			status = domain.ThoughtUnknown
		}
		tag, err := tx.Exec(ctx, `INSERT INTO run_thoughts (id, tenant_id, run_id, step_id, seq, kind, summary,
			detail_ref, status, confidence, structured, attrs)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,0),$11,$12)
			ON CONFLICT DO NOTHING`,
			th.ID, tenantID, runID, stepID, seq, string(th.Kind), th.Summary, th.DetailRef, string(status), th.Confidence,
			th.Structured, jsonb(th.Attrs))
		if err != nil {
			return fmt.Errorf("insert thought: %w", err)
		}
		if tag.RowsAffected() == 0 {
			continue
		}
		for _, ref := range th.Refs {
			if ref.Ref == "" || ref.Relation == "" {
				continue
			}
			if _, err := tx.Exec(ctx, `INSERT INTO thought_refs (tenant_id, thought_id, ref, relation)
				VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
				tenantID, th.ID, ref.Ref, ref.Relation); err != nil {
				return fmt.Errorf("insert thought ref: %w", err)
			}
		}
	}
	return nil
}

func insertCapabilityRefs(ctx context.Context, tx pgx.Tx, tenantID, runID, stepID string, refs []domain.CapabilityRef) error {
	if len(refs) > maxCapsPerStep {
		refs = refs[:maxCapsPerStep]
	}
	for _, ref := range refs {
		if ref.Ref == "" || (ref.Role != "used" && ref.Role != "considered") {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO step_capability_refs (tenant_id, run_id, step_id, capability_ref, role)
			VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`,
			tenantID, runID, stepID, ref.Ref, ref.Role); err != nil {
			return fmt.Errorf("insert capability ref: %w", err)
		}
	}
	return nil
}

func decodeMap(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var out map[string]any
	if json.Unmarshal(raw, &out) != nil {
		return nil
	}
	return out
}
