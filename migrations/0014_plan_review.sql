-- Generated plans get a human approval gate before execution. The step's
-- input hash is the plan hash, so an approval cannot be reused for a changed
-- plan.
ALTER TABLE agent_steps DROP CONSTRAINT IF EXISTS agent_steps_kind_check;
ALTER TABLE agent_steps ADD CONSTRAINT agent_steps_kind_check
  CHECK (kind IN ('reason','tool','retrieve_memory','compact','flow','plan','plan_review'));