-- Provenance graph: typed relations between steps, thoughts, memories and
-- capabilities, plus read-only node/edge views for the desktop graph.
CREATE TABLE capability_nodes (
  tenant_id TEXT NOT NULL,
  ref TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('tool','skill','mcp_server','plugin')),
  name TEXT NOT NULL,
  version TEXT NOT NULL DEFAULT '',
  owner TEXT NOT NULL DEFAULT '',
  source TEXT NOT NULL DEFAULT '',
  risk TEXT NOT NULL DEFAULT '',
  trust TEXT NOT NULL DEFAULT '',
  service_id TEXT NOT NULL DEFAULT '',
  labels JSONB NOT NULL DEFAULT '{}'::jsonb,
  synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, ref)
);
ALTER TABLE capability_nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE capability_nodes FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON capability_nodes
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_capability_nodes_kind ON capability_nodes (tenant_id, kind, name);
CREATE INDEX idx_capability_nodes_service ON capability_nodes (tenant_id, service_id) WHERE service_id <> '';

CREATE TABLE capability_relations (
  tenant_id TEXT NOT NULL,
  src_ref TEXT NOT NULL,
  dst_ref TEXT NOT NULL,
  relation TEXT NOT NULL CHECK (relation IN (
    'provides','belongs_to','depends_on','alternative_to','composes','precedes','related_to')),
  source TEXT NOT NULL CHECK (source IN ('declared','derived')),
  weight DOUBLE PRECISION NOT NULL DEFAULT 1,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, src_ref, dst_ref, relation, source)
);
ALTER TABLE capability_relations ENABLE ROW LEVEL SECURITY;
ALTER TABLE capability_relations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON capability_relations
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_capability_relations_src ON capability_relations (tenant_id, src_ref);
CREATE INDEX idx_capability_relations_dst ON capability_relations (tenant_id, dst_ref);

CREATE TABLE run_thoughts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  seq INT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN (
    'plan','observation','hypothesis','evidence','decision','action',
    'verification','reflection','conclusion')),
  summary TEXT NOT NULL,
  detail_ref TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK (status IN (
    'UNKNOWN','UNVERIFIED','VERIFIED','REFUTED','OK','FAILED')),
  confidence DOUBLE PRECISION,
  structured BOOLEAN NOT NULL DEFAULT false,
  attrs JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, step_id, seq),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id) ON DELETE CASCADE,
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id) ON DELETE CASCADE
);
ALTER TABLE run_thoughts ENABLE ROW LEVEL SECURITY;
ALTER TABLE run_thoughts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON run_thoughts
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_run_thoughts_run ON run_thoughts (tenant_id, run_id, step_id, seq);
CREATE INDEX idx_run_thoughts_kind ON run_thoughts (tenant_id, kind, created_at DESC);

CREATE TABLE thought_refs (
  tenant_id TEXT NOT NULL,
  thought_id UUID NOT NULL,
  ref TEXT NOT NULL,
  relation TEXT NOT NULL CHECK (relation IN (
    'based_on','supports','refutes','uses','produced','cites')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, thought_id, ref, relation),
  FOREIGN KEY (tenant_id, thought_id) REFERENCES run_thoughts(tenant_id, id) ON DELETE CASCADE
);
ALTER TABLE thought_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE thought_refs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON thought_refs
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_thought_refs_ref ON thought_refs (tenant_id, ref);

CREATE TABLE step_capability_refs (
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  step_id UUID NOT NULL,
  capability_ref TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('used','considered')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, step_id, capability_ref, role),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id) ON DELETE CASCADE,
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id) ON DELETE CASCADE
);
ALTER TABLE step_capability_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE step_capability_refs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON step_capability_refs
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_step_capability_refs_run ON step_capability_refs (tenant_id, run_id);
CREATE INDEX idx_step_capability_refs_cap ON step_capability_refs (tenant_id, capability_ref, created_at DESC);

CREATE TABLE memory_shards (
  tenant_id TEXT NOT NULL,
  memory_id UUID NOT NULL,
  shard_key TEXT NOT NULL,
  weight DOUBLE PRECISION NOT NULL DEFAULT 1,
  source TEXT NOT NULL CHECK (source IN ('scope','capability','incident','run','declared')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, memory_id, shard_key),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id) ON DELETE CASCADE
);
ALTER TABLE memory_shards ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_shards FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON memory_shards
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_memory_shards_key ON memory_shards (tenant_id, shard_key, memory_id);
CREATE INDEX idx_memory_shards_memory ON memory_shards (tenant_id, memory_id);

INSERT INTO memory_shards (tenant_id, memory_id, shard_key, weight, source)
SELECT tenant_id, id,
  CASE scope
    WHEN 'service' THEN 'service:' || scope_id
    WHEN 'incident' THEN 'incident:' || scope_id
    WHEN 'agent' THEN 'agent:' || scope_id
    ELSE 'org:' || tenant_id
  END,
  1.0,
  'scope'
FROM agent_memories
ON CONFLICT DO NOTHING;

CREATE VIEW graph_edges AS
SELECT tenant_id, 'step:' || step_id AS src, 'uses' AS relation, capability_ref AS dst,
  'runtime' AS source, 1.0 AS weight, jsonb_build_object('role', role) AS attrs
FROM step_capability_refs
UNION ALL
SELECT tenant_id, 'step:' || step_id, 'produces', 'thought:' || id,
  'runtime', 1.0, jsonb_build_object('kind', kind, 'seq', seq)
FROM run_thoughts
UNION ALL
SELECT tenant_id, 'thought:' || thought_id, relation, ref,
  'runtime', 1.0, '{}'::jsonb
FROM thought_refs
UNION ALL
SELECT h.tenant_id, 'step:' || h.step_id, 'cites', 'memory:' || h.memory_id || '@' || m.current_version,
  'runtime', 1.0, jsonb_build_object('rank', h.rank, 'score', h.score)
FROM step_memory_hits h
JOIN agent_memories m ON m.tenant_id = h.tenant_id AND m.id = h.memory_id
UNION ALL
SELECT s.tenant_id, 'memory:' || s.memory_id || '@' || m.current_version, 'belongs_to', s.shard_key,
  s.source, s.weight, '{}'::jsonb
FROM memory_shards s
JOIN agent_memories m ON m.tenant_id = s.tenant_id AND m.id = s.memory_id
UNION ALL
SELECT tenant_id, src_ref, relation, dst_ref, source, weight, attrs
FROM capability_relations
UNION ALL
SELECT tenant_id, ref, 'related_to', 'service:' || service_id,
  'derived', 1.0, '{}'::jsonb
FROM capability_nodes
WHERE service_id <> ''
UNION ALL
SELECT tenant_id, 'service:' || service_id, 'depends_on', 'service:' || depends_on,
  'derived', 1.0, jsonb_build_object('relation', relation)
FROM service_dependencies
UNION ALL
SELECT tenant_id, 'step:' || id, 'belongs_to', 'run:' || run_id,
  'derived', 1.0, '{}'::jsonb
FROM agent_steps
UNION ALL
SELECT tenant_id, 'run:' || id, 'belongs_to', 'incident:' || incident_id,
  'derived', 1.0, '{}'::jsonb
FROM agent_runs
WHERE incident_id IS NOT NULL
UNION ALL
SELECT tenant_id, 'incident:' || id, 'belongs_to', 'service:' || service_id,
  'derived', 1.0, '{}'::jsonb
FROM incidents;

CREATE VIEW graph_nodes AS
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
  jsonb_build_object('status', status, 'profile_id', profile_id, 'profile_version', profile_version),
  created_at
FROM agent_runs
UNION ALL
SELECT tenant_id, 'step:' || id, 'step', kind || ' #' || seq,
  jsonb_build_object('status', status, 'kind', kind, 'executor', executor_name),
  updated_at
FROM agent_steps
UNION ALL
SELECT tenant_id, 'thought:' || id, 'thought', kind || ': ' || left(summary, 80),
  jsonb_build_object('status', status, 'confidence', confidence, 'step_id', step_id,
    'structured', structured),
  created_at
FROM run_thoughts
UNION ALL
SELECT tenant_id, shard_key, 'shard', shard_key, '{}'::jsonb, max(created_at)
FROM memory_shards
GROUP BY tenant_id, shard_key;
