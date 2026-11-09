-- Memory applicability and outcome verification (RFC §14.2-§14.3).
-- applicability: environments/services/teams/subject kinds, version
-- constraints, validity start, source runs/Works, reviewer, risk of reuse.
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS applicability JSONB;
-- Outcome counters move only on machine-verified Work outcomes, once per
-- (memory, Work): see memory_outcomes.
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS verified_success_count INT NOT NULL DEFAULT 0;
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS verified_failure_count INT NOT NULL DEFAULT 0;
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS last_verified_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS memory_outcomes (
  tenant_id   TEXT NOT NULL,
  memory_id   UUID NOT NULL,
  work_id     TEXT NOT NULL,
  verdict     TEXT NOT NULL CHECK (verdict IN ('success','failure')),
  evaluation  TEXT NOT NULL DEFAULT '',
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, memory_id, work_id)
);
CREATE INDEX IF NOT EXISTS idx_memory_outcomes_work ON memory_outcomes (tenant_id, work_id);
CREATE INDEX IF NOT EXISTS idx_memory_playbooks ON agent_memories (tenant_id, status)
  WHERE applicability->>'kind' = 'playbook';

ALTER TABLE memory_outcomes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS memory_outcomes_tenant ON memory_outcomes;
CREATE POLICY memory_outcomes_tenant ON memory_outcomes USING (harness_tenant_visible(tenant_id))
  WITH CHECK (harness_tenant_visible(tenant_id));
