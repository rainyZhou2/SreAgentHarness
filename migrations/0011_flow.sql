-- Flow orchestration: one flow_runs row per coordinator run, one
-- flow_node_runs row per child run, and human input gates.
ALTER TABLE agent_steps DROP CONSTRAINT IF EXISTS agent_steps_kind_check;
ALTER TABLE agent_steps ADD CONSTRAINT agent_steps_kind_check
  CHECK (kind IN ('reason','tool','retrieve_memory','compact','flow'));

ALTER TABLE agent_steps DROP CONSTRAINT IF EXISTS agent_steps_status_check;
ALTER TABLE agent_steps ADD CONSTRAINT agent_steps_status_check
  CHECK (status IN (
    'PENDING','RUNNING','WAITING_APPROVAL','WAITING_CHILDREN','WAITING_INPUT',
    'RETRY_WAIT','SUCCEEDED','FAILED','SKIPPED','CANCELLED','UNKNOWN','RECONCILING','NEEDS_HUMAN'));

ALTER TABLE agent_steps ADD COLUMN flow_node_id TEXT NOT NULL DEFAULT '';

ALTER TABLE agent_runs DROP CONSTRAINT IF EXISTS agent_runs_status_check;
ALTER TABLE agent_runs ADD CONSTRAINT agent_runs_status_check
  CHECK (status IN (
    'PENDING_ACTIVATION','QUEUED','RUNNING','PAUSED','WAITING_APPROVAL',
    'WAITING_CHILDREN','WAITING_INPUT','NEEDS_HUMAN','CANCELLED','COMPLETED','FAILED'));

ALTER TABLE agent_runs
  ADD COLUMN parent_run_id UUID NULL,
  ADD COLUMN parent_step_id UUID NULL,
  ADD COLUMN flow_node_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN flow_depth INT NOT NULL DEFAULT 0;

ALTER TABLE agent_runs ADD CONSTRAINT fk_run_parent
  FOREIGN KEY (tenant_id, parent_run_id) REFERENCES agent_runs(tenant_id, id);
ALTER TABLE agent_runs ADD CONSTRAINT fk_run_parent_step
  FOREIGN KEY (tenant_id, parent_step_id) REFERENCES agent_steps(tenant_id, id);
CREATE INDEX idx_run_parent ON agent_runs (tenant_id, parent_run_id) WHERE parent_run_id IS NOT NULL;

CREATE TABLE flow_runs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  run_id UUID NOT NULL,
  compiled_ref TEXT NOT NULL,
  compiled_hash TEXT NOT NULL,
  state JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN (
    'RUNNING','WAITING_CHILDREN','WAITING_APPROVAL','WAITING_INPUT',
    'COMPLETED','FAILED','CANCELLED')),
  current_node TEXT NOT NULL DEFAULT '',
  depth INT NOT NULL DEFAULT 0,
  child_runs INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, run_id),
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, run_id) REFERENCES agent_runs(tenant_id, id)
);
CREATE INDEX idx_flow_runs_status ON flow_runs (tenant_id, status);

CREATE TABLE flow_node_runs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  flow_run_id UUID NOT NULL,
  node_id TEXT NOT NULL,
  node_type TEXT NOT NULL DEFAULT '',
  child_run_id UUID NULL,
  status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN (
    'PENDING','RUNNING','SUCCEEDED','FAILED','CANCELLED','SKIPPED')),
  output_ref TEXT,
  output_hash TEXT,
  error TEXT,
  attempt INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, flow_run_id, node_id, attempt),
  UNIQUE (tenant_id, child_run_id),
  FOREIGN KEY (tenant_id, flow_run_id) REFERENCES flow_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, child_run_id) REFERENCES agent_runs(tenant_id, id)
);
CREATE INDEX idx_flow_node_runs_flow ON flow_node_runs (tenant_id, flow_run_id, status);

CREATE TABLE flow_inputs (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  flow_run_id UUID NOT NULL,
  node_id TEXT NOT NULL,
  step_id UUID NOT NULL,
  actor_id TEXT NOT NULL,
  value JSONB NOT NULL DEFAULT 'null'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, flow_run_id, node_id),
  FOREIGN KEY (tenant_id, flow_run_id) REFERENCES flow_runs(tenant_id, id),
  FOREIGN KEY (tenant_id, step_id) REFERENCES agent_steps(tenant_id, id)
);

ALTER TABLE flow_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE flow_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON flow_runs
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

ALTER TABLE flow_node_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE flow_node_runs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON flow_node_runs
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));

ALTER TABLE flow_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE flow_inputs FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON flow_inputs
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));