-- v2 control plane: bitemporal world state (RFC §7). Facts and relations are
-- never updated in place except to close their valid-time interval; the
-- moment of closing is kept (closed_observed_at) so "as known at" queries
-- reproduce what the platform knew then. Different sources coexist.
CREATE TABLE world_entities (
  tenant_id TEXT NOT NULL,
  ref TEXT NOT NULL,
  kind TEXT NOT NULL,
  attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  source TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, ref)
);
CREATE INDEX idx_world_entities_attrs ON world_entities USING GIN (attributes jsonb_path_ops);

CREATE TABLE world_facts (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  predicate TEXT NOT NULL,
  value TEXT NOT NULL,
  source TEXT NOT NULL,
  trust_tier TEXT NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  observed_at TIMESTAMPTZ NOT NULL,
  closed_observed_at TIMESTAMPTZ,
  confirmed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  evidence_ref TEXT,
  confidence DOUBLE PRECISION NOT NULL DEFAULT 1,
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX idx_world_facts_subject ON world_facts (tenant_id, subject_ref, predicate, valid_from DESC);
CREATE INDEX idx_world_facts_series ON world_facts (tenant_id, subject_ref, predicate, source, valid_from);

CREATE TABLE world_relations (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  from_ref TEXT NOT NULL,
  relation_type TEXT NOT NULL,
  to_ref TEXT NOT NULL,
  source TEXT NOT NULL,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_to TIMESTAMPTZ,
  observed_at TIMESTAMPTZ NOT NULL,
  closed_observed_at TIMESTAMPTZ,
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
CREATE INDEX idx_world_relations_src ON world_relations (tenant_id, from_ref, relation_type, valid_from DESC);
CREATE INDEX idx_world_relations_dst ON world_relations (tenant_id, to_ref, valid_from DESC);

CREATE TABLE world_snapshots (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  known_at TIMESTAMPTZ NOT NULL,
  subjects JSONB NOT NULL,
  body JSONB NOT NULL,
  digest TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, id)
);
CREATE INDEX idx_world_snapshots_digest ON world_snapshots (tenant_id, digest);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['world_entities','world_facts','world_relations','world_snapshots'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
