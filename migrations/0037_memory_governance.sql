ALTER TABLE agent_memories DROP CONSTRAINT IF EXISTS agent_memories_status_check;
UPDATE agent_memories SET status = CASE status
  WHEN 'ACTIVE' THEN 'APPROVED'
  WHEN 'VALIDATING' THEN 'CANDIDATE'
  WHEN 'REJECTED' THEN 'REVOKED'
  WHEN 'ARCHIVED' THEN 'REVOKED'
  ELSE status END;
ALTER TABLE agent_memories ADD CONSTRAINT agent_memories_status_check
  CHECK (status IN ('CANDIDATE','VERIFIED','APPROVED','CONTESTED','STALE','REVOKED','SUPERSEDED'));
ALTER TABLE agent_memories
  ADD COLUMN decision_ref TEXT NOT NULL DEFAULT '',
  ADD COLUMN outcome_ref TEXT NOT NULL DEFAULT '',
  ADD COLUMN embedding_version TEXT NOT NULL DEFAULT '',
  ADD COLUMN approved_by TEXT,
  ADD COLUMN approved_at TIMESTAMPTZ,
  ADD COLUMN approval_basis TEXT NOT NULL DEFAULT '',
  ADD COLUMN suspended_status TEXT;
UPDATE agent_memories SET approval_basis = 'MIGRATED_ACTIVE' WHERE status = 'APPROVED' AND approval_basis = '';

CREATE TABLE agent_memory_acl (
  tenant_id TEXT NOT NULL,
  memory_id UUID NOT NULL,
  subject_type TEXT NOT NULL CHECK (subject_type IN ('user','role','service')),
  subject_id TEXT NOT NULL,
  permission TEXT NOT NULL CHECK (permission = 'read'),
  created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, memory_id, subject_type, subject_id, permission),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id) ON DELETE CASCADE
);
INSERT INTO agent_memory_acl (tenant_id, memory_id, subject_type, subject_id, created_by)
SELECT tenant_id, id,
  CASE WHEN scope = 'user' THEN 'user' ELSE 'role' END,
  CASE WHEN scope = 'user' THEN scope_id ELSE 'viewer' END,
  'migration_0037'
FROM agent_memories
ON CONFLICT DO NOTHING;
INSERT INTO agent_memory_acl (tenant_id, memory_id, subject_type, subject_id, created_by)
SELECT tenant_id, id, 'role', 'service', 'migration_0037' FROM agent_memories
WHERE scope <> 'user' ON CONFLICT DO NOTHING;
INSERT INTO agent_memory_acl (tenant_id, memory_id, subject_type, subject_id, created_by)
SELECT tenant_id, id, 'user', created_by, 'migration_0037' FROM agent_memories
WHERE created_by <> '' ON CONFLICT DO NOTHING;
CREATE INDEX idx_memory_acl_subject ON agent_memory_acl (tenant_id, subject_type, subject_id, memory_id);
ALTER TABLE agent_memory_acl ENABLE ROW LEVEL SECURITY;
ALTER TABLE agent_memory_acl FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON agent_memory_acl
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE memory_verifications (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  memory_id UUID NOT NULL,
  memory_version BIGINT NOT NULL,
  evidence_refs JSONB NOT NULL,
  decision_ref TEXT NOT NULL,
  outcome_ref TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  verifier TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, idempotency_key),
  FOREIGN KEY (tenant_id, memory_id) REFERENCES agent_memories(tenant_id, id)
);
CREATE INDEX idx_memory_verification_version ON memory_verifications (tenant_id, memory_id, memory_version DESC);
ALTER TABLE memory_verifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_verifications FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON memory_verifications
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE memory_conflicts (
  tenant_id TEXT NOT NULL,
  first_memory_id UUID NOT NULL,
  second_memory_id UUID NOT NULL,
  reason TEXT NOT NULL,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_by TEXT,
  resolved_at TIMESTAMPTZ,
  resolution TEXT,
  PRIMARY KEY (tenant_id, first_memory_id, second_memory_id),
  CHECK (first_memory_id < second_memory_id),
  FOREIGN KEY (tenant_id, first_memory_id) REFERENCES agent_memories(tenant_id, id),
  FOREIGN KEY (tenant_id, second_memory_id) REFERENCES agent_memories(tenant_id, id)
);
ALTER TABLE memory_conflicts ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_conflicts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON memory_conflicts
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

ALTER TABLE memory_embeddings ADD COLUMN embedding_version TEXT;
UPDATE memory_embeddings SET embedding_version = CASE
  WHEN embedding_model LIKE 'hash-uni-bigram-v2-%' THEN 'v2' ELSE 'v1' END
  WHERE embedding_version IS NULL;
ALTER TABLE memory_embeddings ALTER COLUMN embedding_version SET NOT NULL;
ALTER TABLE memory_embeddings DROP CONSTRAINT IF EXISTS memory_embeddings_pkey;
ALTER TABLE memory_embeddings ADD PRIMARY KEY
  (tenant_id,memory_id,memory_version,embedding_model,embedding_version);
ALTER TABLE agent_memories ADD CONSTRAINT agent_memories_supersedes_tenant_fk
  FOREIGN KEY (tenant_id, superseded_by) REFERENCES agent_memories(tenant_id, id);
DROP INDEX IF EXISTS uq_memory_content;
CREATE UNIQUE INDEX uq_memory_content ON agent_memories (tenant_id, scope, scope_id, content_hash)
  WHERE status NOT IN ('REVOKED','SUPERSEDED');
DROP INDEX IF EXISTS idx_memory_subject;
CREATE INDEX idx_memory_subject ON agent_memories (tenant_id, subject)
  WHERE status IN ('VERIFIED','APPROVED','CANDIDATE','CONTESTED','STALE');

CREATE TABLE artifact_objects (
  tenant_id TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','DELETING','DELETED')),
  lease_token BIGINT NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, artifact_ref)
);
CREATE INDEX idx_artifact_cleanup ON artifact_objects (status,next_attempt_at,lease_expires_at);
ALTER TABLE artifact_objects ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_objects FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON artifact_objects
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE artifact_references (
  tenant_id TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  referrer_type TEXT NOT NULL,
  referrer_id TEXT NOT NULL,
  referrer_version BIGINT NOT NULL CHECK (referrer_version > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,artifact_ref,referrer_type,referrer_id,referrer_version),
  FOREIGN KEY (tenant_id,artifact_ref) REFERENCES artifact_objects(tenant_id,artifact_ref)
);
CREATE INDEX idx_artifact_reference_target ON artifact_references (tenant_id,referrer_type,referrer_id);
ALTER TABLE artifact_references ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_references FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON artifact_references
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE artifact_holds (
  tenant_id TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  hold_id TEXT NOT NULL,
  reason TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ,
  released_by TEXT,
  released_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id,artifact_ref,hold_id),
  FOREIGN KEY (tenant_id,artifact_ref) REFERENCES artifact_objects(tenant_id,artifact_ref)
);
ALTER TABLE artifact_holds ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_holds FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON artifact_holds
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE artifact_retention_policies (
  tenant_id TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  retain_until TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id,artifact_ref,policy_id),
  FOREIGN KEY (tenant_id,artifact_ref) REFERENCES artifact_objects(tenant_id,artifact_ref)
);
ALTER TABLE artifact_retention_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_retention_policies FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON artifact_retention_policies
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

INSERT INTO artifact_objects (tenant_id,artifact_ref)
SELECT DISTINCT tenant_id,segment_ref FROM model_stream_segments ON CONFLICT DO NOTHING;
INSERT INTO artifact_references (tenant_id,artifact_ref,referrer_type,referrer_id,referrer_version)
SELECT tenant_id,segment_ref,'model_stream_segment',model_request_id::text,segment_no
FROM model_stream_segments ON CONFLICT DO NOTHING;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM agent_memories WHERE status NOT IN
    ('CANDIDATE','VERIFIED','APPROVED','CONTESTED','STALE','REVOKED','SUPERSEDED')) THEN
    RAISE EXCEPTION 'invalid memory status after migration';
  END IF;
  IF EXISTS (SELECT 1 FROM agent_memories m WHERE NOT EXISTS
    (SELECT 1 FROM agent_memory_acl a WHERE a.tenant_id=m.tenant_id AND a.memory_id=m.id)) THEN
    RAISE EXCEPTION 'memory without explicit ACL after migration';
  END IF;
  IF EXISTS (SELECT 1 FROM agent_memories m WHERE m.scope<>'user' AND NOT EXISTS
    (SELECT 1 FROM agent_memory_acl a WHERE a.tenant_id=m.tenant_id AND a.memory_id=m.id
      AND a.subject_type='role' AND a.subject_id='service')) THEN
    RAISE EXCEPTION 'non-personal memory without service ACL after migration';
  END IF;
  IF EXISTS (SELECT 1 FROM memory_embeddings WHERE embedding_version IS NULL OR embedding_version='') THEN
    RAISE EXCEPTION 'unversioned embedding';
  END IF;
  WITH RECURSIVE walk AS (
    SELECT tenant_id,id,superseded_by,ARRAY[id] AS path,false AS cycle FROM agent_memories
    UNION ALL
    SELECT w.tenant_id,w.id,m.superseded_by,w.path||m.id,m.id=ANY(w.path)
    FROM walk w JOIN agent_memories m ON m.tenant_id=w.tenant_id AND m.id=w.superseded_by
    WHERE NOT w.cycle
  )
  SELECT 1 FROM walk WHERE cycle LIMIT 1;
  IF FOUND THEN
    RAISE EXCEPTION 'supersedes cycle after migration';
  END IF;
  IF EXISTS (SELECT 1 FROM artifact_references r WHERE NOT EXISTS
    (SELECT 1 FROM artifact_objects o WHERE o.tenant_id=r.tenant_id AND o.artifact_ref=r.artifact_ref)) THEN
    RAISE EXCEPTION 'orphan artifact reference after migration';
  END IF;
END $$;
