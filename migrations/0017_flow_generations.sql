-- Flow replanning: a coordinator run may replace a failed generated flow with
-- a new plan. Each plan runs as a new generation of the same flow_runs row;
-- node attempts are keyed by generation so a new graph never reads, or
-- collides with, the node results of the graph it replaced.
ALTER TABLE flow_runs ADD COLUMN generation INT NOT NULL DEFAULT 0 CHECK (generation >= 0);
ALTER TABLE flow_node_runs ADD COLUMN generation INT NOT NULL DEFAULT 0 CHECK (generation >= 0);
ALTER TABLE flow_node_runs DROP CONSTRAINT flow_node_runs_tenant_id_flow_run_id_node_id_attempt_key;
ALTER TABLE flow_node_runs ADD CONSTRAINT flow_node_runs_generation_node_attempt_key
  UNIQUE (tenant_id, flow_run_id, generation, node_id, attempt);
