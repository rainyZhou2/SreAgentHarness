-- v2 control plane: shared evidence and hypotheses across runs and agents
-- (RFC §12–13). Evidence is append-only; corrections supersede.
CREATE TABLE work_evidence (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  source_type TEXT NOT NULL,
  source_ref TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  supersedes UUID,
  record JSONB NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, work_id, content_hash),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_evidence_work ON work_evidence (tenant_id, work_id, captured_at);

CREATE TABLE work_hypotheses (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  hyp_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('OPEN','SUPPORTED','REFUTED','CHALLENGED','INCONCLUSIVE')),
  record JSONB NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, work_id, hyp_key),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['work_evidence','work_hypotheses'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
