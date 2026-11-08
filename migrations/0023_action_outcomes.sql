-- v2 control plane: Work-level actions and outcome evaluations (RFC §10, §11).
-- work_actions is the effect ledger of Work actions: DISPATCHED is committed
-- before the external call, and the effect key is unique, so an action can
-- never be dispatched twice under one plan. Kernel tool effects stay in
-- external_tool_effects; the two ledgers never share keys.
CREATE TABLE work_actions (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  run_id UUID,
  stage_id TEXT,
  generation INT NOT NULL,
  action_name TEXT NOT NULL,
  action_version TEXT NOT NULL,
  target_ref TEXT NOT NULL,
  plan_hash TEXT NOT NULL,
  intent_hash TEXT NOT NULL,
  effect_key TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('PROPOSED','REJECTED','WAITING_APPROVAL','READY','DISPATCHED','APPLIED',
    'FAILED','EFFECT_UNKNOWN','MANUAL_RECOVERY','VERIFYING','VERIFIED','DEGRADED','COMPENSATION_PENDING','COMPENSATED')),
  record JSONB NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  dispatched_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, id),
  UNIQUE (tenant_id, work_id, plan_hash),
  UNIQUE (tenant_id, effect_key),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_actions_work ON work_actions (tenant_id, work_id, created_at);
CREATE INDEX idx_work_actions_state ON work_actions (tenant_id, state, updated_at);
CREATE INDEX idx_work_actions_dispatched ON work_actions (dispatched_at) WHERE state = 'DISPATCHED';

-- One in-flight action per resource (RFC §10.4 item 7).
CREATE TABLE action_target_locks (
  tenant_id TEXT NOT NULL,
  target_ref TEXT NOT NULL,
  action_id UUID NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, target_ref)
);

CREATE TABLE work_outcomes (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  spec_hash TEXT NOT NULL,
  verdict TEXT NOT NULL CHECK (verdict IN ('SATISFIED','NOT_SATISFIED','INCONCLUSIVE','UNKNOWN')),
  raw_verdict TEXT NOT NULL,
  stage_id TEXT,
  generation INT NOT NULL,
  evaluation JSONB NOT NULL,
  evaluated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_outcomes_recent ON work_outcomes (tenant_id, work_id, evaluated_at DESC);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['work_actions','action_target_locks','work_outcomes'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
