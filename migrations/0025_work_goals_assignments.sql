-- v2 control plane: goal history, assignments, inbox retention and run
-- tenant consistency (RFC §17.4).

-- Goal of every Work generation. A replan or reopen may amend the goal
-- (description, success spec, constraints); the new generation's goal is
-- appended here and agent_works.goal holds the current one.
CREATE TABLE work_goals (
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  generation INT NOT NULL,
  goal JSONB NOT NULL,
  goal_hash TEXT NOT NULL,
  reason TEXT,
  actor_id TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, work_id, generation),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
INSERT INTO work_goals (tenant_id, work_id, generation, goal, goal_hash, created_at)
  SELECT tenant_id, id, generation, goal, goal_hash, created_at FROM agent_works
  ON CONFLICT DO NOTHING;

-- Assignment results on the owner run links (RFC §13.2): what each agent of a
-- stage cost, how it ended and which evidence it produced.
ALTER TABLE work_run_links
  ADD COLUMN cost_micros BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN outcome TEXT,
  ADD COLUMN finished_at TIMESTAMPTZ,
  ADD COLUMN evidence_refs JSONB;

-- Assignments are the owner runs a Work started for a stage role. Views run
-- with their owner's rights (PostgreSQL 14 has no security_invoker), so the
-- tenant filter is repeated in the view.
CREATE VIEW work_assignments AS
  SELECT tenant_id, work_id, run_id, generation, stage_id, COALESCE(agent_role, phase) AS role, required,
         status, outcome, cost_micros, evidence_refs, created_at, finished_at
  FROM work_run_links WHERE role = 'owner' AND stage_id IS NOT NULL AND harness_tenant_visible(tenant_id);

-- Inbox retention: payloads are dropped after the payload retention, rows
-- after the record retention (terminal states only; FAILED/NEW are never
-- purged). Correlation groups are purged by their own expiry.
ALTER TABLE event_inbox ADD COLUMN payload_purged_at TIMESTAMPTZ;
CREATE INDEX idx_event_inbox_retention ON event_inbox (received_at)
  WHERE processing_state IN ('PROCESSED','IGNORED','DEAD');

-- A Work may only reference runs of its own tenant. There is no foreign key
-- to agent_runs (run retention must stay independent), so a trigger checks
-- it. The check reads agent_runs without the request's tenant scope (RLS
-- would hide exactly the row it looks for) and restores the scope.
CREATE OR REPLACE FUNCTION harness_check_run_tenant() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  scope TEXT := current_setting('app.tenant_id', true);
  other TEXT;
BEGIN
  IF NEW.run_id IS NULL THEN
    RETURN NEW;
  END IF;
  PERFORM set_config('app.tenant_id', '', true);
  SELECT r.tenant_id INTO other FROM agent_runs r WHERE r.id = NEW.run_id AND r.tenant_id <> NEW.tenant_id LIMIT 1;
  PERFORM set_config('app.tenant_id', coalesce(scope, ''), true);
  IF other IS NOT NULL THEN
    RAISE EXCEPTION 'run % belongs to another tenant', NEW.run_id USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER work_run_links_run_tenant BEFORE INSERT OR UPDATE OF run_id, tenant_id ON work_run_links
  FOR EACH ROW EXECUTE FUNCTION harness_check_run_tenant();
CREATE TRIGGER work_actions_run_tenant BEFORE INSERT OR UPDATE OF run_id, tenant_id ON work_actions
  FOR EACH ROW EXECUTE FUNCTION harness_check_run_tenant();

ALTER TABLE work_goals ENABLE ROW LEVEL SECURITY;
ALTER TABLE work_goals FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON work_goals USING (harness_tenant_visible(tenant_id))
  WITH CHECK (harness_tenant_visible(tenant_id));

-- Work actions and kernel tool effects share targets: before a Work action
-- dispatches, the runtime looks for open kernel effects on its target.
CREATE INDEX idx_external_tool_effects_open ON external_tool_effects (tenant_id, target)
  WHERE state IN ('PREPARED','DISPATCHED','UNKNOWN');
