-- The plan step lets an auto session decide between ReAct and a generated
-- flow before any work starts.
ALTER TABLE agent_steps DROP CONSTRAINT IF EXISTS agent_steps_kind_check;
ALTER TABLE agent_steps ADD CONSTRAINT agent_steps_kind_check
  CHECK (kind IN ('reason','tool','retrieve_memory','compact','flow','plan'));