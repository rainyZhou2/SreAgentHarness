-- Personal memory and memory updates.
-- scope 'user': memories that belong to one user (scope_id = user id). They
-- are retrieved only for runs that user started, never by shared search.
ALTER TABLE agent_memories DROP CONSTRAINT IF EXISTS agent_memories_scope_check;
ALTER TABLE agent_memories ADD CONSTRAINT agent_memories_scope_check
  CHECK (scope IN ('organization','service','incident','agent','user'));
-- Reinforcement: the same or a near-identical memory written again from a
-- new source is counted on the existing memory instead of duplicated.
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS reinforcements INT NOT NULL DEFAULT 0;
ALTER TABLE agent_memories ADD COLUMN IF NOT EXISTS last_reinforced_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_memory_personal ON agent_memories (tenant_id, scope_id, updated_at DESC)
  WHERE scope = 'user' AND status = 'ACTIVE';
