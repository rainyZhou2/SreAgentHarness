package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"go-sre-harness/internal/cognitive"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/memory"
)

func (s *Store) KnownServices(ctx context.Context, tenantID string, limit int) ([]string, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `SELECT service_id FROM (
			SELECT DISTINCT scope_id AS service_id FROM agent_memories WHERE tenant_id=$1 AND scope='service'
			UNION SELECT DISTINCT service_id FROM incidents WHERE tenant_id=$1
			UNION SELECT DISTINCT service_id FROM service_dependencies WHERE tenant_id=$1
		) s WHERE service_id <> '' ORDER BY service_id LIMIT $2`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) ServiceShards(ctx context.Context, tenantID string, services []string, limit int) ([]string, error) {
	if len(services) == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT s.shard_key FROM memory_shards s
		JOIN agent_memories m ON m.tenant_id = s.tenant_id AND m.id = s.memory_id
		WHERE s.tenant_id=$1 AND s.shard_key LIKE 'capability:%'
		  AND m.scope='service' AND m.scope_id = ANY($2)
		ORDER BY s.shard_key LIMIT $3`, tenantID, services, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

func (s *Store) RunCapabilities(ctx context.Context, tenantID, runID string) ([]domain.CapabilityRef, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT capability_ref, role FROM step_capability_refs
		WHERE tenant_id=$1 AND run_id=$2 AND role='used' ORDER BY capability_ref`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.CapabilityRef
	for rows.Next() {
		var ref domain.CapabilityRef
		if err := rows.Scan(&ref.Ref, &ref.Role); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

// pinnedMemories revalidates pinned versions before injecting their content.
func (s *Store) pinnedMemories(ctx context.Context, tenantID, runID, _ string, beforeSeq int64) ([]cognitive.MemoryRef, error) {
	rows, err := s.pool.Query(ctx, `WITH last AS (
			SELECT id FROM agent_steps WHERE run_id=$1 AND tenant_id=$2 AND kind='retrieve_memory' AND status='SUCCEEDED'
		AND ($3=0 OR seq < $3) ORDER BY seq DESC LIMIT 1)
		SELECT m.id, h.memory_version, m.memory_type, m.scope, m.title,
			m.content_text, m.trust, h.score, m.environment, m.applicability
		FROM step_memory_hits h JOIN last ON h.step_id = last.id
		JOIN agent_memories m ON m.id = h.memory_id AND m.tenant_id=h.tenant_id
		WHERE h.tenant_id=$2 AND m.status='ACTIVE' AND m.current_version=h.memory_version
		AND ((m.scope <> 'user' AND m.sensitivity IN ('public','internal')) OR
			(m.scope='user' AND EXISTS (SELECT 1 FROM agent_runs r WHERE r.id=$1
			AND r.tenant_id=$2 AND r.initiator_type='user' AND r.initiator_id=m.scope_id)))
		AND m.superseded_by IS NULL AND (m.expires_at IS NULL OR m.expires_at > now())
		ORDER BY h.rank`, runID, tenantID, beforeSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []cognitive.MemoryRef
	env, _ := memory.EnvFrom(ctx)
	at := time.Now().UTC()
	for rows.Next() {
		var m cognitive.MemoryRef
		candidate := memory.Memory{TenantID: tenantID, Status: "ACTIVE"}
		var applicability []byte
		if err := rows.Scan(&m.ID, &m.Version, &m.Kind, &m.Scope, &m.Title, &m.Content, &m.Trust, &m.Score, &candidate.Environment, &applicability); err != nil {
			return nil, err
		}
		candidate.Trust = m.Trust
		if len(applicability) > 0 {
			if err := json.Unmarshal(applicability, &candidate.Applicability); err != nil {
				return nil, fmt.Errorf("decode pinned memory applicability: %w", err)
			}
		}
		if valid, _ := candidate.Recallable(tenantID, env, at); !valid {
			continue
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return out, nil
	}
	ids := make([]string, 0, len(out))
	for _, m := range out {
		ids = append(ids, m.ID)
	}
	shardRows, err := s.pool.Query(ctx, `SELECT memory_id, shard_key FROM memory_shards
		WHERE tenant_id=$1 AND memory_id = ANY($2) ORDER BY memory_id, weight DESC, shard_key`, tenantID, ids)
	if err != nil {
		return nil, err
	}
	defer shardRows.Close()
	byID := map[string][]string{}
	for shardRows.Next() {
		var id, key string
		if err := shardRows.Scan(&id, &key); err != nil {
			return nil, err
		}
		byID[id] = append(byID[id], key)
	}
	if err := shardRows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Shards = byID[out[i].ID]
	}
	return out, nil
}

// ---------- long-term memory (§8–§11) ----------

var _ memory.Store = (*Store)(nil)
var _ memory.ConsolidationStore = (*Store)(nil)

const memoryCols = `id, tenant_id, scope, scope_id, memory_type, status, current_version, subject, title, content_text,
	content_ref, content_hash, source_refs, evidence_refs, trust, sensitivity, confidence, verified_by, environment,
	superseded_by, expires_at, created_by, created_at, updated_at, reinforcements, last_reinforced_at, applicability,
	verified_success_count, verified_failure_count, last_verified_at`

func scanMemory(row pgx.Row, extra ...any) (memory.Memory, error) {
	var m memory.Memory
	var src, ev, appl []byte
	dest := []any{&m.ID, &m.TenantID, &m.Scope, &m.ScopeID, &m.Type, &m.Status, &m.Version, &m.Subject, &m.Title,
		&m.Content, &m.ContentRef, &m.ContentHash, &src, &ev, &m.Trust, &m.Sensitivity, &m.Confidence, &m.VerifiedBy,
		&m.Environment, &m.SupersededBy, &m.ExpiresAt, &m.CreatedBy, &m.CreatedAt, &m.UpdatedAt, &m.Reinforcements,
		&m.LastReinforcedAt, &appl, &m.Verification.SuccessCount, &m.Verification.FailureCount,
		&m.Verification.LastVerifiedAt}
	if err := row.Scan(append(dest, extra...)...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return m, domain.ErrNotFound
		}
		return m, err
	}
	_ = json.Unmarshal(src, &m.SourceRefs)
	_ = json.Unmarshal(ev, &m.EvidenceRefs)
	if len(appl) > 0 && string(appl) != "null" {
		var a memory.Applicability
		if json.Unmarshal(appl, &a) == nil {
			m.Applicability = &a
		}
	}
	return m, nil
}

// CreateMemory inserts a memory (deduplicated by content within its scope),
// its first version and its embedding. Activating a memory supersedes other
// ACTIVE memories with the same subject in the same scope.
func (s *Store) CreateMemory(ctx context.Context, m memory.NewMemory) (memory.Memory, bool, error) {
	var out memory.Memory
	created := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if dup, ok, err := nearDuplicate(ctx, tx, m); err != nil {
			return err
		} else if ok {
			out, err = reinforce(ctx, tx, dup, m.SourceRefs)
			return err
		}
		id := newID()
		var inserted string
		err := tx.QueryRow(ctx, `INSERT INTO agent_memories (id, tenant_id, scope, scope_id, memory_type, status, subject,
			title, content_text, content_ref, content_hash, source_refs, evidence_refs, trust, sensitivity, confidence,
			verified_by, environment, expires_at, created_by, search_terms, search_terms_v, applicability)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,1,$22)
			ON CONFLICT (tenant_id, scope, scope_id, content_hash) WHERE status NOT IN ('REJECTED','ARCHIVED') DO NOTHING
			RETURNING id`, id, m.TenantID, m.Scope, m.ScopeID, m.Type, m.Status, m.Subject, m.Title, m.Content,
			m.ContentRef, m.ContentHash, jsonb(nonNil(m.SourceRefs)), jsonb(nonNil(m.EvidenceRefs)), m.Trust, m.Sensitivity,
			m.Confidence, m.VerifiedBy, m.Environment, m.ExpiresAt, m.CreatedBy, m.SearchTerms, applicabilityJSON(m.Applicability)).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			cur, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND scope=$2
				AND scope_id=$3 AND content_hash=$4 AND status NOT IN ('REJECTED','ARCHIVED') FOR UPDATE`, m.TenantID, m.Scope, m.ScopeID,
				m.ContentHash))
			if err != nil {
				return err
			}
			out, err = reinforce(ctx, tx, cur, m.SourceRefs)
			return err
		}
		if err != nil {
			return err
		}
		created = true
		if _, err := tx.Exec(ctx, `INSERT INTO agent_memory_versions (tenant_id, memory_id, version, content_ref, content_hash,
			status, provenance) VALUES ($1,$2,1,$3,$4,$5,$6)`, m.TenantID, id, m.ContentRef, m.ContentHash, m.Status,
			jsonb(map[string]any{"created_by": m.CreatedBy, "source_refs": m.SourceRefs, "redactions": m.Redactions})); err != nil {
			return err
		}
		if len(m.Embedding) > 0 {
			if _, err := tx.Exec(ctx, `INSERT INTO memory_embeddings (tenant_id, memory_id, memory_version, embedding_model,
				dims, embedding) VALUES ($1,$2,1,$3,$4,$5)`, m.TenantID, id, m.EmbeddingModel, len(m.Embedding),
				m.Embedding); err != nil {
				return err
			}
		}
		for _, sh := range memoryShards(m) {
			if _, err := tx.Exec(ctx, `INSERT INTO memory_shards (tenant_id, memory_id, shard_key, weight, source)
				VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, m.TenantID, id, sh.ShardKey, sh.Weight, sh.Source); err != nil {
				return err
			}
		}
		if m.Status == "ACTIVE" {
			if err := supersedeSubject(ctx, tx, m.TenantID, id, m.Scope, m.ScopeID, m.Subject); err != nil {
				return err
			}
		}
		out, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE id=$1`, id))
		return err
	})
	return out, created, err
}

// nearDuplicate finds a live memory with the same scope and subject that
// says the same thing (memory.NearDuplicate) and is at least as validated.
func nearDuplicate(ctx context.Context, tx pgx.Tx, m memory.NewMemory) (memory.Memory, bool, error) {
	rows, err := tx.Query(ctx, `SELECT `+prefixCols("m.", memoryCols)+`, e.embedding FROM agent_memories m
		LEFT JOIN memory_embeddings e ON e.memory_id = m.id AND e.memory_version = m.current_version AND e.embedding_model = $5
		WHERE m.tenant_id=$1 AND m.scope=$2 AND m.scope_id=$3 AND m.subject=$4
		AND m.status IN ('ACTIVE','VALIDATING','CANDIDATE') ORDER BY m.updated_at DESC LIMIT 20`,
		m.TenantID, m.Scope, m.ScopeID, m.Subject, m.EmbeddingModel)
	if err != nil {
		return memory.Memory{}, false, err
	}
	var found *memory.Memory
	for rows.Next() {
		var emb []float32
		cur, err := scanMemory(rows, &emb)
		if err != nil {
			rows.Close()
			return memory.Memory{}, false, err
		}
		if found == nil && cur.ContentHash != m.ContentHash && memory.StatusRank(cur.Status) >= memory.StatusRank(m.Status) &&
			memory.NearDuplicate(m, cur, emb) {
			c := cur
			found = &c
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil || found == nil {
		return memory.Memory{}, false, err
	}
	return *found, true, nil
}

// reinforce counts a repeated write on an existing memory when it comes
// from a source the memory does not have yet.
func reinforce(ctx context.Context, tx pgx.Tx, cur memory.Memory, sources []string) (memory.Memory, error) {
	merged, changed := memory.MergeSources(cur.SourceRefs, sources)
	if !changed {
		return cur, nil
	}
	return scanMemory(tx.QueryRow(ctx, `UPDATE agent_memories SET reinforcements = reinforcements + 1,
		last_reinforced_at = now(), source_refs = $3 WHERE tenant_id=$1 AND id=$2 RETURNING `+memoryCols,
		cur.TenantID, cur.ID, jsonb(merged)))
}

func memoryShards(m memory.NewMemory) []domain.MemoryShard {
	key := "org:" + m.TenantID
	if m.Scope != "organization" && m.ScopeID != "" {
		key = m.Scope + ":" + m.ScopeID
	}
	out := []domain.MemoryShard{{ShardKey: key, Weight: 1, Source: "scope"}}
	seen := map[string]bool{key: true}
	for _, sh := range m.Shards {
		if sh.ShardKey == "" || seen[sh.ShardKey] {
			continue
		}
		seen[sh.ShardKey] = true
		if sh.Weight == 0 {
			sh.Weight = 0.8
		}
		out = append(out, sh)
	}
	return out
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

func supersedeSubject(ctx context.Context, tx pgx.Tx, tenantID, keepID, scope, scopeID, subject string) error {
	_, err := tx.Exec(ctx, `UPDATE agent_memories SET status='SUPERSEDED', superseded_by=$2, updated_at=now()
		WHERE tenant_id=$1 AND id<>$2 AND scope=$3 AND scope_id=$4 AND subject=$5 AND status='ACTIVE'`,
		tenantID, keepID, scope, scopeID, subject)
	return err
}

var memoryTransitions = map[string][]string{
	"CANDIDATE":  {"VALIDATING", "ACTIVE", "REJECTED"},
	"VALIDATING": {"ACTIVE", "REJECTED"},
	"ACTIVE":     {"STALE", "SUPERSEDED", "ARCHIVED"},
	"STALE":      {"ACTIVE", "ARCHIVED"},
	"SUPERSEDED": {"ARCHIVED"},
	"REJECTED":   {"ARCHIVED"},
}

// TransitionMemory moves a memory through its lifecycle (§8.3). Activation
// records the verifier and supersedes contradicting memories.
func (s *Store) TransitionMemory(ctx context.Context, tenantID, id, from, to, actor, reason string) (memory.Memory, error) {
	var out memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			tenantID, id))
		if err != nil {
			return err
		}
		if from != "" && cur.Status != from {
			return domain.ErrVersionConflict
		}
		allowed := false
		for _, t := range memoryTransitions[cur.Status] {
			allowed = allowed || t == to
		}
		if !allowed {
			return &domain.RejectedError{Reason: fmt.Sprintf("invalid_transition_%s_to_%s", cur.Status, to)}
		}
		if to == "ACTIVE" {
			if err := memory.CanPromote(cur, actor); err != nil {
				return &domain.RejectedError{Reason: "playbook_promotion: " + err.Error()}
			}
		}
		trust := cur.Trust
		var verifier *string
		if to == "ACTIVE" {
			verifier = &actor
			if trust == "T3" {
				trust = "T2" // human-verified
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE agent_memories SET status=$3, trust=$4, verified_by=COALESCE($5, verified_by),
			updated_at=now() WHERE tenant_id=$1 AND id=$2`, tenantID, id, to, trust, verifier); err != nil {
			return err
		}
		if to == "ACTIVE" {
			if err := supersedeSubject(ctx, tx, tenantID, id, cur.Scope, cur.ScopeID, cur.Subject); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.transition','memory',$4,'applied',$5)`, newID(),
			tenantID, actor, id, jsonb(map[string]string{"from": cur.Status, "to": to, "reason": reason})); err != nil {
			return err
		}
		out, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE id=$1`, id))
		return err
	})
	return out, err
}

func applicabilityJSON(a *memory.Applicability) any {
	if a == nil {
		return nil
	}
	return jsonb(a)
}

// RecordMemoryOutcome records the machine-verified outcome of a Work that
// used a memory. It is idempotent per (memory, Work): a replayed close or
// a second evaluation never double-counts. It reports whether it counted.
func (s *Store) RecordMemoryOutcome(ctx context.Context, tenantID, memoryID, workID string, success bool, evaluationID string) (bool, error) {
	verdict, col := "failure", "verified_failure_count"
	if success {
		verdict, col = "success", "verified_success_count"
	}
	counted := false
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `INSERT INTO memory_outcomes (tenant_id, memory_id, work_id, verdict, evaluation)
			SELECT $1,$2::uuid,$3,$4,$5 WHERE EXISTS (SELECT 1 FROM agent_memories WHERE tenant_id=$1 AND id=$2::uuid)
			ON CONFLICT DO NOTHING`, tenantID, memoryID, workID, verdict, evaluationID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		counted = true
		_, err = tx.Exec(ctx, `UPDATE agent_memories SET `+col+` = `+col+` + 1, last_verified_at = now()
			WHERE tenant_id=$1 AND id=$2::uuid`, tenantID, memoryID)
		return err
	})
	return counted, err
}

// SetMemoryApplicability replaces a memory's applicability (operator
// curation; audited).
func (s *Store) SetMemoryApplicability(ctx context.Context, tenantID, id, actor string, a memory.Applicability) (memory.Memory, error) {
	if err := a.Normalize(); err != nil {
		return memory.Memory{}, &domain.RejectedError{Reason: err.Error()}
	}
	var out memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE agent_memories SET applicability=$3, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
			tenantID, id, jsonb(a))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return domain.ErrNotFound
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.applicability','memory',$4,'applied',$5)`, newID(),
			tenantID, actor, id, jsonb(a)); err != nil {
			return err
		}
		out, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE id=$1`, id))
		return err
	})
	return out, err
}

var _ memory.Updater = (*Store)(nil)
var _ memory.PersonalStore = (*Store)(nil)

// LiveMemoryBySubject returns the most validated live memory with this
// scope and subject.
func (s *Store) LiveMemoryBySubject(ctx context.Context, tenantID, scope, scopeID, subject string) (memory.Memory, bool, error) {
	m, err := scanMemory(s.pool.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND scope=$2
		AND scope_id=$3 AND subject=$4 AND status IN ('ACTIVE','VALIDATING','CANDIDATE')
		ORDER BY CASE status WHEN 'ACTIVE' THEN 0 WHEN 'VALIDATING' THEN 1 ELSE 2 END, updated_at DESC LIMIT 1`,
		tenantID, scope, scopeID, subject))
	if errors.Is(err, domain.ErrNotFound) {
		return m, false, nil
	}
	return m, err == nil, err
}

// UpdateMemory writes a new version of a live memory's content. Earlier
// versions stay in agent_memory_versions, so citations pinned to them keep
// resolving; embeddings and search terms follow the new version.
func (s *Store) UpdateMemory(ctx context.Context, r memory.Revision) (memory.Memory, error) {
	var out memory.Memory
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		cur, err := scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			r.TenantID, r.ID))
		if err != nil {
			return err
		}
		if r.ExpectedVersion > 0 && cur.Version != r.ExpectedVersion {
			return domain.ErrVersionConflict
		}
		if memory.StatusRank(cur.Status) < 0 && cur.Status != "STALE" {
			return &domain.RejectedError{Reason: "memory_not_live_" + strings.ToLower(cur.Status)}
		}
		p := r.Prepared
		if p.ContentHash == cur.ContentHash && p.Title == cur.Title {
			out = cur
			return nil
		}
		var clash string
		err = tx.QueryRow(ctx, `SELECT id FROM agent_memories WHERE tenant_id=$1 AND scope=$2 AND scope_id=$3
			AND content_hash=$4 AND id<>$5 AND status NOT IN ('REJECTED','ARCHIVED') LIMIT 1`, cur.TenantID, cur.Scope,
			cur.ScopeID, p.ContentHash, cur.ID).Scan(&clash)
		if err == nil {
			return &domain.RejectedError{Reason: "duplicate_content_of_" + clash}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		next := cur.Version + 1
		if _, err := tx.Exec(ctx, `UPDATE agent_memories SET title=$3, content_text=$4, content_ref=$5, content_hash=$6,
			source_refs=$7, evidence_refs=$8, search_terms=$9, search_terms_v=1, current_version=$10, updated_at=now()
			WHERE tenant_id=$1 AND id=$2`, cur.TenantID, cur.ID, p.Title, p.Content, p.ContentRef, p.ContentHash,
			jsonb(nonNil(p.SourceRefs)), jsonb(nonNil(p.EvidenceRefs)), p.SearchTerms, next); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO agent_memory_versions (tenant_id, memory_id, version, content_ref, content_hash,
			status, provenance) VALUES ($1,$2,$3,$4,$5,$6,$7)`, cur.TenantID, cur.ID, next, p.ContentRef, p.ContentHash,
			cur.Status, jsonb(map[string]any{"updated_by": r.Actor, "reason": r.Reason, "previous_version": cur.Version,
				"redactions": p.Redactions})); err != nil {
			return err
		}
		if len(p.Embedding) > 0 && p.EmbeddingModel != "" {
			if _, err := tx.Exec(ctx, `INSERT INTO memory_embeddings (tenant_id, memory_id, memory_version, embedding_model,
				dims, embedding) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, cur.TenantID, cur.ID, next,
				p.EmbeddingModel, len(p.Embedding), p.Embedding); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action, resource_type,
			resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.update','memory',$4,'applied',$5)`, newID(),
			cur.TenantID, r.Actor, cur.ID, jsonb(map[string]any{"from_version": cur.Version, "to_version": next,
				"reason": r.Reason})); err != nil {
			return err
		}
		out, err = scanMemory(tx.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE id=$1`, cur.ID))
		return err
	})
	return out, err
}

// RunInitiator returns the user who started the run ("" for other initiators).
func (s *Store) RunInitiator(ctx context.Context, tenantID, runID string) (string, error) {
	var typ string
	var id *string
	err := s.pool.QueryRow(ctx, `SELECT initiator_type, initiator_id FROM agent_runs WHERE tenant_id=$1 AND id=$2`,
		tenantID, runID).Scan(&typ, &id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", domain.ErrNotFound
		}
		return "", err
	}
	if typ != "user" || id == nil {
		return "", nil
	}
	return *id, nil
}

// PersonalMemories returns a user's ACTIVE personal memories.
func (s *Store) PersonalMemories(ctx context.Context, tenantID, userID string, limit int) ([]memory.Memory, error) {
	if limit <= 0 || limit > 200 {
		limit = memory.MaxPersonalMemories
	}
	rows, err := s.pool.Query(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND scope='user'
		AND scope_id=$2 AND status='ACTIVE' AND (expires_at IS NULL OR expires_at > now())
		ORDER BY updated_at DESC LIMIT $3`, tenantID, userID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (memory.Memory, error) { return scanMemory(row) })
}

func (s *Store) GetMemory(ctx context.Context, tenantID, id string) (memory.Memory, error) {
	if !validID(id) {
		return memory.Memory{}, domain.ErrNotFound
	}
	return scanMemory(s.pool.QueryRow(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1 AND id=$2`, tenantID, id))
}

type MemoryFilter struct {
	Status  string
	Scope   string
	ScopeID string
	Query   string
	Limit   int
}

func (s *Store) ListMemories(ctx context.Context, tenantID string, f MemoryFilter) ([]memory.Memory, error) {
	if f.Limit <= 0 || f.Limit > 500 {
		f.Limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT `+memoryCols+` FROM agent_memories WHERE tenant_id=$1
		AND ($2='' OR status=$2) AND ($3='' OR scope=$3) AND ($4='' OR scope_id=$4)
		AND ($5='' OR search_tsv @@ to_tsquery('simple', $5)) ORDER BY updated_at DESC LIMIT $6`,
		tenantID, f.Status, f.Scope, f.ScopeID, tsQuery(f.Query), f.Limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (memory.Memory, error) { return scanMemory(row) })
}

// tsQuery builds an OR query from sanitized tokens (no user tsquery syntax).
func tsQuery(text string) string {
	toks := memory.Tokenize(text)
	seen := map[string]bool{}
	var out []string
	for _, t := range toks {
		t = strings.Trim(t, "-_")
		if t == "" || seen[t] || len(out) >= 32 {
			continue
		}
		clean := strings.Map(func(r rune) rune {
			if r == '\'' || r == ':' || r == '&' || r == '|' || r == '!' || r == '(' || r == ')' || r == '<' || r == '>' || r == '*' {
				return -1
			}
			return r
		}, t)
		if clean != "" {
			seen[t] = true
			out = append(out, clean)
		}
	}
	return strings.Join(out, " | ")
}

// MemoryCandidates is the SQL hard-filter stage of retrieval (§9.2).
func (s *Store) MemoryCandidates(ctx context.Context, q memory.CandidateQuery) ([]memory.Candidate, error) {
	if q.Limit <= 0 {
		q.Limit = 500
	}
	allowed := []string{"public", "internal"}
	if q.MaxSensitivity == "restricted" {
		allowed = append(allowed, "restricted")
	} else if q.MaxSensitivity == "public" {
		allowed = []string{"public"}
	}
	tq := tsQuery(q.Text)
	rows, err := s.pool.Query(ctx, `SELECT `+prefixCols("m.", memoryCols)+`,
		CASE WHEN $4 = '' THEN 0 ELSE ts_rank(m.search_tsv, to_tsquery('simple', $4)) END AS rank, e.embedding,
		COALESCE((SELECT MAX(ms.weight) FROM memory_shards ms
			WHERE ms.tenant_id = m.tenant_id AND ms.memory_id = m.id AND ms.shard_key = ANY($8)), 0) AS shard_weight
		FROM agent_memories m
		LEFT JOIN memory_embeddings e ON e.memory_id = m.id AND e.memory_version = m.current_version AND e.embedding_model = $5
		WHERE m.tenant_id=$1 AND m.status='ACTIVE' AND m.sensitivity = ANY($6)
		AND (m.expires_at IS NULL OR m.expires_at > now()) AND (m.valid_until IS NULL OR m.valid_until > now())
		AND (m.scope='organization' OR (m.scope='service' AND m.scope_id = ANY($2))
		     OR (m.scope='incident' AND m.scope_id = $3) OR m.scope='agent')
		ORDER BY rank DESC, m.updated_at DESC LIMIT $7`,
		q.TenantID, q.Services, q.IncidentID, tq, q.EmbeddingModel, allowed, q.Limit, q.Shards)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memory.Candidate
	for rows.Next() {
		var c memory.Candidate
		var rank float32
		var emb []float32
		var shardW float64
		m, err := scanMemory(rows, &rank, &emb, &shardW)
		if err != nil {
			return nil, err
		}
		c.Memory, c.FTSRank, c.Embedding, c.ShardWeight = m, float64(rank), emb, shardW
		out = append(out, c)
	}
	return out, rows.Err()
}

func prefixCols(prefix, cols string) string {
	parts := strings.Split(cols, ",")
	for i, p := range parts {
		parts[i] = prefix + strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

func (s *Store) ServiceDependencies(ctx context.Context, tenantID, service string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT depends_on FROM service_dependencies WHERE tenant_id=$1 AND service_id=$2
		UNION SELECT service_id FROM service_dependencies WHERE tenant_id=$1 AND depends_on=$2 ORDER BY 1`, tenantID, service)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *Store) PutServiceDependency(ctx context.Context, tenantID, service, dependsOn, relation string) error {
	if relation == "" {
		relation = "calls"
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO service_dependencies (tenant_id, service_id, depends_on, relation)
		VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id, service_id, depends_on) DO UPDATE SET relation=EXCLUDED.relation`,
		tenantID, service, dependsOn, relation)
	return err
}

func (s *Store) RunIncident(ctx context.Context, tenantID, runID string) (string, error) {
	var inc *string
	err := s.pool.QueryRow(ctx, `SELECT incident_id FROM agent_runs WHERE tenant_id=$1 AND id=$2`, tenantID, runID).Scan(&inc)
	if err != nil || inc == nil {
		return "", err
	}
	return *inc, nil
}

// ---------- consolidation queue, experiences, feedback ----------

func (s *Store) ClaimConsolidation(ctx context.Context, limit int, claimFor time.Duration) ([]memory.ConsolidationJob, error) {
	rows, err := s.pool.Query(ctx, `UPDATE consolidation_queue q SET claimed_until = now() + $2 * interval '1 millisecond',
		attempts = attempts + 1
		WHERE run_id IN (SELECT run_id FROM consolidation_queue WHERE status='PENDING' AND attempts < 5
			AND (claimed_until IS NULL OR claimed_until < now()) ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
		RETURNING tenant_id, run_id`, limit, claimFor.Milliseconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (memory.ConsolidationJob, error) {
		var j memory.ConsolidationJob
		err := row.Scan(&j.TenantID, &j.RunID)
		return j, err
	})
}

func (s *Store) CompleteConsolidation(ctx context.Context, runID string, failure error) error {
	if failure == nil {
		_, err := s.pool.Exec(ctx, `UPDATE consolidation_queue SET status='DONE', claimed_until=NULL, last_error=NULL
			WHERE run_id=$1`, runID)
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE consolidation_queue SET claimed_until = now() + interval '30 seconds',
		last_error=$2, status = CASE WHEN attempts >= 5 THEN 'FAILED' ELSE 'PENDING' END WHERE run_id=$1`,
		runID, trimErr(failure))
	return err
}

// RequeueConsolidation re-runs consolidation, e.g. after new feedback.
func (s *Store) RequeueConsolidation(ctx context.Context, tenantID, runID string) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO consolidation_queue (tenant_id, run_id) VALUES ($1,$2)
		ON CONFLICT (run_id) DO UPDATE SET status='PENDING', attempts=0, claimed_until=NULL`, tenantID, runID)
	return err
}

func (s *Store) RunRecord(ctx context.Context, tenantID, runID string) (memory.RunRecord, error) {
	var rec memory.RunRecord
	run, err := s.GetRun(ctx, tenantID, runID)
	if err != nil {
		return rec, err
	}
	steps, err := s.ListSteps(ctx, tenantID, runID)
	if err != nil {
		return rec, err
	}
	fb, err := s.ListFeedback(ctx, tenantID, runID)
	if err != nil {
		return rec, err
	}
	return memory.RunRecord{Run: run, Steps: steps, Feedback: fb}, nil
}

func (s *Store) InsertExperience(ctx context.Context, e memory.Experience) (bool, error) {
	tag, err := s.pool.Exec(ctx, `INSERT INTO experiences (id, tenant_id, incident_id, run_id, outcome, outcome_ref,
		lessons_ref, source_refs, quality_state, extractor_version) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (tenant_id, run_id, extractor_version) DO UPDATE SET quality_state=EXCLUDED.quality_state,
		lessons_ref=EXCLUDED.lessons_ref`, newID(), e.TenantID, e.IncidentID, e.RunID, e.Outcome, nullable(e.OutcomeRef),
		e.LessonsRef, jsonb(nonNil(e.SourceRefs)), e.QualityState, e.ExtractorVersion)
	return err == nil && tag.RowsAffected() > 0, err
}

type ExperienceView struct {
	ID           string    `json:"id"`
	RunID        string    `json:"run_id"`
	Outcome      string    `json:"outcome"`
	LessonsRef   string    `json:"lessons_ref"`
	QualityState string    `json:"quality_state"`
	CreatedAt    time.Time `json:"created_at"`
}

func (s *Store) ListExperiences(ctx context.Context, tenantID, runID string) ([]ExperienceView, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, run_id, outcome, lessons_ref, quality_state, created_at FROM experiences
		WHERE tenant_id=$1 AND ($2='' OR run_id::text=$2) ORDER BY created_at DESC LIMIT 200`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ExperienceView, error) {
		var v ExperienceView
		err := row.Scan(&v.ID, &v.RunID, &v.Outcome, &v.LessonsRef, &v.QualityState, &v.CreatedAt)
		return v, err
	})
}

// PutFeedback upserts an actor's feedback on a finished run and applies its
// consequence to memories derived from the run (§11.3). Feedback is an
// operator action, so it can never activate a memory: a confirmed root cause
// only moves candidates to VALIDATING, where an approver must activate them
// through TransitionMemory. A refuted root cause takes memories out of
// retrieval at once (candidates are REJECTED, active ones become STALE so an
// approver can still restore them). Every change is audited.
func (s *Store) PutFeedback(ctx context.Context, tenantID, runID string, f memory.Feedback) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO run_feedback (id, tenant_id, run_id, actor_id, root_cause_correct,
			root_cause_label, rating, notes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
			ON CONFLICT (tenant_id, run_id, actor_id) DO UPDATE SET root_cause_correct=EXCLUDED.root_cause_correct,
			root_cause_label=EXCLUDED.root_cause_label, rating=EXCLUDED.rating, notes=EXCLUDED.notes`,
			newID(), tenantID, runID, f.ActorID, f.RootCauseCorrect, f.RootCauseLabel, f.Rating, f.Notes); err != nil {
			return err
		}
		if f.RootCauseCorrect != nil {
			src := jsonb([]string{"run:" + runID})
			var moves [][2]string // {from, to}
			if *f.RootCauseCorrect {
				moves = [][2]string{{"CANDIDATE", "VALIDATING"}}
			} else {
				moves = [][2]string{{"CANDIDATE", "REJECTED"}, {"VALIDATING", "REJECTED"}, {"ACTIVE", "STALE"}}
			}
			for _, m := range moves {
				rows, err := tx.Query(ctx, `UPDATE agent_memories SET status=$4, updated_at=now()
					WHERE tenant_id=$1 AND source_refs @> $2 AND status=$3 RETURNING id`, tenantID, src, m[0], m[1])
				if err != nil {
					return err
				}
				ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
				if err != nil {
					return err
				}
				for _, id := range ids {
					if _, err := tx.Exec(ctx, `INSERT INTO security_audit_log (id, tenant_id, actor_type, actor_id, action,
						resource_type, resource_id, outcome, details) VALUES ($1,$2,'user',$3,'memory.feedback','memory',$4,'applied',$5)`,
						newID(), tenantID, f.ActorID, id, jsonb(map[string]string{"from": m[0], "to": m[1], "run_id": runID})); err != nil {
						return err
					}
				}
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO consolidation_queue (tenant_id, run_id) VALUES ($1,$2)
			ON CONFLICT (run_id) DO UPDATE SET status='PENDING', attempts=0, claimed_until=NULL`, tenantID, runID)
		return err
	})
}

func (s *Store) ListFeedback(ctx context.Context, tenantID, runID string) ([]memory.Feedback, error) {
	rows, err := s.pool.Query(ctx, `SELECT actor_id, root_cause_correct, root_cause_label, rating, notes FROM run_feedback
		WHERE tenant_id=$1 AND run_id=$2 ORDER BY created_at`, tenantID, runID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (memory.Feedback, error) {
		var f memory.Feedback
		err := row.Scan(&f.ActorID, &f.RootCauseCorrect, &f.RootCauseLabel, &f.Rating, &f.Notes)
		return f, err
	})
}

// ---------- incident shared working memory (§8.1) ----------

type IncidentEvent struct {
	Version   int64           `json:"version"`
	EventType string          `json:"event_type"`
	RunID     *string         `json:"run_id,omitempty"`
	StepID    *string         `json:"step_id,omitempty"`
	ActorType string          `json:"actor_type"`
	ActorID   *string         `json:"actor_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
	Trust     string          `json:"trust"`
	CreatedAt time.Time       `json:"created_at"`
}

// AppendIncidentEvent appends to the incident's ordered working memory. The
// incident's memory_version is what new context snapshots pin.
func (s *Store) AppendIncidentEvent(ctx context.Context, tenantID, incidentID string, e IncidentEvent, expectedVersion *int64) (int64, error) {
	var v int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var cur int64
		if err := tx.QueryRow(ctx, `SELECT memory_version FROM incidents WHERE tenant_id=$1 AND id=$2 FOR UPDATE`,
			tenantID, incidentID).Scan(&cur); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrNotFound
			}
			return err
		}
		if expectedVersion != nil && *expectedVersion != cur {
			return domain.ErrVersionConflict
		}
		v = cur + 1
		trust := e.Trust
		if trust == "" {
			trust = "T2"
		}
		if _, err := tx.Exec(ctx, `INSERT INTO incident_memory_events (tenant_id, incident_id, version, event_type, run_id,
			step_id, actor_type, actor_id, payload, trust) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, tenantID, incidentID, v,
			e.EventType, e.RunID, e.StepID, e.ActorType, e.ActorID, jsonb(e.Payload), trust); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE incidents SET memory_version=$3 WHERE tenant_id=$1 AND id=$2`, tenantID, incidentID, v)
		return err
	})
	return v, err
}

func (s *Store) ListIncidentEvents(ctx context.Context, tenantID, incidentID string, upTo int64) ([]IncidentEvent, error) {
	rows, err := s.pool.Query(ctx, `SELECT version, event_type, run_id, step_id, actor_type, actor_id, payload, trust, created_at
		FROM incident_memory_events WHERE tenant_id=$1 AND incident_id=$2 AND ($3 = 0 OR version <= $3) ORDER BY version`,
		tenantID, incidentID, upTo)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (IncidentEvent, error) {
		var e IncidentEvent
		err := row.Scan(&e.Version, &e.EventType, &e.RunID, &e.StepID, &e.ActorType, &e.ActorID, &e.Payload, &e.Trust, &e.CreatedAt)
		return e, err
	})
}

// ---------- search index backfill ----------

var _ memory.IndexStore = (*Store)(nil)

// MemoriesToIndex returns live memories without search terms or without an
// embedding of the given model for their current version (all tenants:
// the indexer is a background job).
func (s *Store) MemoriesToIndex(ctx context.Context, model string, limit int) ([]memory.IndexItem, error) {
	rows, err := s.pool.Query(ctx, `SELECT m.tenant_id, m.id, m.current_version, m.title, m.content_text, m.subject,
		m.search_terms_v < 1,
		$1 <> '' AND NOT EXISTS (SELECT 1 FROM memory_embeddings e WHERE e.memory_id = m.id
			AND e.memory_version = m.current_version AND e.embedding_model = $1)
		FROM agent_memories m
		WHERE m.status IN ('ACTIVE','CANDIDATE','VALIDATING','STALE')
		AND (m.search_terms_v < 1 OR ($1 <> '' AND NOT EXISTS (SELECT 1 FROM memory_embeddings e WHERE e.memory_id = m.id
			AND e.memory_version = m.current_version AND e.embedding_model = $1)))
		ORDER BY (m.status = 'ACTIVE') DESC, m.updated_at DESC LIMIT $2`, model, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []memory.IndexItem
	for rows.Next() {
		var it memory.IndexItem
		if err := rows.Scan(&it.TenantID, &it.ID, &it.Version, &it.Title, &it.Content, &it.Subject, &it.NeedTerms,
			&it.NeedEmbedding); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

// PutMemoryIndex stores a memory's search terms and/or embedding. It is
// idempotent, so concurrent indexers do no harm.
func (s *Store) PutMemoryIndex(ctx context.Context, it memory.IndexItem, model string, emb []float32, terms string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if it.NeedTerms {
			if _, err := tx.Exec(ctx, `UPDATE agent_memories SET search_terms=$3, search_terms_v=1
				WHERE tenant_id=$1 AND id=$2`, it.TenantID, it.ID, terms); err != nil {
				return err
			}
		}
		if len(emb) > 0 && model != "" {
			if _, err := tx.Exec(ctx, `INSERT INTO memory_embeddings (tenant_id, memory_id, memory_version, embedding_model,
				dims, embedding) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, it.TenantID, it.ID, it.Version, model,
				len(emb), emb); err != nil {
				return err
			}
		}
		return nil
	})
}
