-- v3: observability Source Catalog and query audit (design §5, §16.1).
-- A source is an operator-owned record: the endpoint and the credential are
-- references resolved on the server (endpoint_ref, credentials_ref), never a
-- URL or a secret, and never taken from tool arguments. Every change of a
-- source appends a revision, so a rollout can be audited and rolled back.
CREATE TABLE observability_sources (
  id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('metrics','logs','traces','events','changes')),
  backend TEXT NOT NULL,
  endpoint_ref TEXT NOT NULL,
  credentials_ref TEXT NOT NULL DEFAULT '',
  environment TEXT NOT NULL,
  spec JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'DISABLED' CHECK (status IN ('ACTIVE','DEGRADED','DISABLED')),
  version BIGINT NOT NULL DEFAULT 1,
  health JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id)
);

CREATE TABLE observability_source_revisions (
  tenant_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  spec JSONB NOT NULL,
  status TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, source_id, version),
  FOREIGN KEY (tenant_id, source_id) REFERENCES observability_sources (tenant_id, id)
);

-- The audit keeps the query digest and the subject, never the expression,
-- a credential or the result body (the body is an artifact).
CREATE TABLE observability_queries (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL,
  source_id TEXT NOT NULL,
  work_id UUID,
  query_digest TEXT NOT NULL,
  subject_ref TEXT,
  time_start TIMESTAMPTZ NOT NULL,
  time_end TIMESTAMPTZ NOT NULL,
  result_status TEXT NOT NULL,
  result_ref TEXT,
  result_digest TEXT,
  result_count BIGINT NOT NULL DEFAULT 0,
  duration_ms BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, source_id) REFERENCES observability_sources (tenant_id, id)
);
CREATE INDEX idx_observability_queries_work ON observability_queries (tenant_id, work_id, created_at DESC);
CREATE INDEX idx_observability_queries_source ON observability_queries (tenant_id, source_id, created_at DESC);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['observability_sources','observability_source_revisions','observability_queries'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
