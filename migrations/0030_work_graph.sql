-- v3: Work graph and agent findings (design §8, §11, §16.3).
-- A Work has at most one parent (ownership: budget and permission ceiling)
-- and any number of dependency edges to Works of the same root. The CHECK
-- only forbids a self edge; the store verifies in the inserting transaction
-- that the graph stays acyclic, within its depth and child bounds, and
-- inside one tenant.
ALTER TABLE agent_works ADD COLUMN IF NOT EXISTS parent_work_id UUID;
ALTER TABLE agent_works ADD COLUMN IF NOT EXISTS root_work_id UUID;
ALTER TABLE agent_works ADD COLUMN IF NOT EXISTS depth INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_agent_works_parent ON agent_works (tenant_id, parent_work_id) WHERE parent_work_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_agent_works_root ON agent_works (tenant_id, root_work_id) WHERE root_work_id IS NOT NULL;

CREATE TABLE work_dependencies (
  tenant_id TEXT NOT NULL,
  parent_work_id UUID NOT NULL,
  child_work_id UUID NOT NULL,
  dependency_kind TEXT NOT NULL CHECK (dependency_kind IN ('after_success','after_terminal','after_signal')),
  predicate_ref TEXT,
  required BOOLEAN NOT NULL DEFAULT TRUE,
  satisfied_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, parent_work_id, child_work_id),
  CHECK (parent_work_id <> child_work_id),
  FOREIGN KEY (tenant_id, parent_work_id) REFERENCES agent_works (tenant_id, id),
  FOREIGN KEY (tenant_id, child_work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_dependencies_child ON work_dependencies (tenant_id, child_work_id);

-- Structured findings of agent assignments. Hypotheses stay the merged
-- ledger; a finding is what one agent reported, with the evidence it cited.
CREATE TABLE work_findings (
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  finding_id UUID NOT NULL,
  run_id UUID NOT NULL,
  role TEXT NOT NULL,
  hypothesis_key TEXT,
  verdict TEXT NOT NULL CHECK (verdict IN ('supported','refuted','insufficient','contested')),
  statement TEXT NOT NULL,
  evidence_refs JSONB NOT NULL,
  record JSONB NOT NULL,
  payload_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, finding_id),
  UNIQUE (tenant_id, work_id, payload_hash),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_findings_work ON work_findings (tenant_id, work_id, created_at);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['work_dependencies','work_findings'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
