-- Reasoning quality: one score per run (recomputed when feedback arrives),
-- with the memories the run retrieved, for pattern effectiveness statistics.
CREATE TABLE reasoning_scores (
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  profile_id TEXT NOT NULL,
  profile_version BIGINT NOT NULL,
  service TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL,
  quality DOUBLE PRECISION NOT NULL,
  metrics JSONB NOT NULL,
  memory_ids TEXT[] NOT NULL DEFAULT '{}',
  scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, run_id)
);
ALTER TABLE reasoning_scores ENABLE ROW LEVEL SECURITY;
ALTER TABLE reasoning_scores FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON reasoning_scores
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_reasoning_scores_profile ON reasoning_scores (tenant_id, profile_id, profile_version, scored_at DESC);
CREATE INDEX idx_reasoning_scores_memories ON reasoning_scores USING GIN (memory_ids);
