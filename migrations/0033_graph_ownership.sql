-- Ownership on the provenance graph: run, step and thought nodes carry who
-- started their run, so the API can show a user their own work and hide
-- other users' (the thought label is the agent's reasoning text). The view
-- keeps its columns; only the attrs of those three kinds grow.
CREATE OR REPLACE VIEW graph_nodes AS
SELECT tenant_id, ref, kind, name AS label,
  jsonb_build_object('version', version, 'owner', owner, 'source', source,
    'risk', risk, 'trust', trust, 'service_id', service_id, 'labels', labels) AS attrs,
  synced_at AS updated_at
FROM capability_nodes
UNION ALL
SELECT tenant_id, 'memory:' || id || '@' || current_version, 'memory',
  COALESCE(NULLIF(title, ''), subject),
  jsonb_build_object('status', status, 'trust', trust, 'sensitivity', sensitivity,
    'scope', scope, 'scope_id', scope_id, 'memory_type', memory_type),
  updated_at
FROM agent_memories
UNION ALL
SELECT tenant_id, 'service:' || service_id, 'service', service_id,
  '{}'::jsonb,
  max(created_at)
FROM incidents
GROUP BY tenant_id, service_id
UNION ALL
SELECT tenant_id, 'incident:' || id, 'incident',
  COALESCE(NULLIF(service_id, ''), 'incident') || ' · ' || left(id::text, 8),
  jsonb_build_object('status', status, 'environment', environment, 'service_id', service_id),
  created_at
FROM incidents
UNION ALL
SELECT tenant_id, 'run:' || id, 'run', 'run ' || left(id::text, 8),
  jsonb_build_object('status', status, 'profile_id', profile_id, 'profile_version', profile_version,
    'initiator_type', initiator_type, 'initiator_id', initiator_id),
  created_at
FROM agent_runs
UNION ALL
SELECT s.tenant_id, 'step:' || s.id, 'step', s.kind || ' #' || s.seq,
  jsonb_build_object('status', s.status, 'kind', s.kind, 'executor', s.executor_name, 'run_id', s.run_id,
    'initiator_type', r.initiator_type, 'initiator_id', r.initiator_id),
  s.updated_at
FROM agent_steps s JOIN agent_runs r ON r.tenant_id = s.tenant_id AND r.id = s.run_id
UNION ALL
SELECT t.tenant_id, 'thought:' || t.id, 'thought', t.kind || ': ' || left(t.summary, 80),
  jsonb_build_object('status', t.status, 'confidence', t.confidence, 'step_id', t.step_id,
    'structured', t.structured, 'run_id', t.run_id,
    'initiator_type', r.initiator_type, 'initiator_id', r.initiator_id),
  t.created_at
FROM run_thoughts t JOIN agent_runs r ON r.tenant_id = t.tenant_id AND r.id = t.run_id
UNION ALL
SELECT tenant_id, shard_key, 'shard', shard_key, '{}'::jsonb, max(created_at)
FROM memory_shards
GROUP BY tenant_id, shard_key;
