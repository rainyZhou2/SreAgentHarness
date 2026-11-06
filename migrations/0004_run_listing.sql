-- Run listing for the console / GET /v1/runs (newest first per tenant).
CREATE INDEX IF NOT EXISTS idx_agent_runs_tenant_created ON agent_runs (tenant_id, created_at DESC, id DESC);
