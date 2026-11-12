-- Memory curation proposals (internal/curate): suggestions to set aside
-- duplicate, superseded, refuted or unused long-term memories. A proposal
-- changes nothing until a person approves it. The body is JSON; the columns
-- are what is queried and what keeps a finding from being raised twice.
CREATE TABLE memory_curation_proposals (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING','APPLIED','DISMISSED','OUTDATED')),
  signature TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, id),
  -- one proposal per finding, in any status: a dismissed finding is not
  -- raised again until a memory in it changes (the signature names versions)
  UNIQUE (tenant_id, signature)
);
CREATE INDEX idx_memory_curation_status ON memory_curation_proposals (tenant_id, status, created_at DESC);

ALTER TABLE memory_curation_proposals ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_curation_proposals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON memory_curation_proposals
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
