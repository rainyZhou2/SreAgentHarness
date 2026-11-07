-- Inbound triggers: a sliding deduplication window per (tenant, key). A claim
-- row names the run that handles events with the same fingerprint until it
-- expires; concurrent replicas agree on one claim through the primary key.
CREATE TABLE trigger_dedup (
  tenant_id TEXT NOT NULL,
  dedup_key TEXT NOT NULL,
  claim_id UUID NOT NULL,
  run_id UUID,
  hits INTEGER NOT NULL DEFAULT 1,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, dedup_key)
);
ALTER TABLE trigger_dedup ENABLE ROW LEVEL SECURITY;
ALTER TABLE trigger_dedup FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON trigger_dedup
  USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id));
CREATE INDEX idx_trigger_dedup_expiry ON trigger_dedup (expires_at);

-- Rate-based policy rules count planned intents per tool (and target).
CREATE INDEX IF NOT EXISTS idx_action_intents_rate ON action_intents (tenant_id, tool_name, created_at);
