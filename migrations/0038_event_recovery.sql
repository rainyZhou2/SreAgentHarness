ALTER TABLE work_events
  ADD COLUMN correlation_id TEXT,
  ADD COLUMN correlation_key TEXT,
  ADD COLUMN received_at TIMESTAMPTZ;
CREATE INDEX idx_work_events_correlation ON work_events (tenant_id, correlation_key, work_id, event_seq);

CREATE TABLE work_outbox_messages (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  subject TEXT NOT NULL,
  payload JSONB NOT NULL,
  publish_attempt INT NOT NULL DEFAULT 0,
  next_retry_at TIMESTAMPTZ NOT NULL,
  claimed_until TIMESTAMPTZ,
  published_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL,
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_outbox_due ON work_outbox_messages (next_retry_at, created_at)
  WHERE published_at IS NULL;
ALTER TABLE work_outbox_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE work_outbox_messages FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON work_outbox_messages
  USING (harness_tenant_visible(tenant_id))
  WITH CHECK (harness_tenant_visible(tenant_id));

CREATE TABLE source_reconcile_checkpoints (
  tenant_id TEXT NOT NULL,
  source TEXT NOT NULL,
  cursor TEXT NOT NULL DEFAULT '',
  owner TEXT,
  fencing_token BIGINT NOT NULL DEFAULT 0,
  lease_until TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  last_error TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, source)
);
ALTER TABLE source_reconcile_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_reconcile_checkpoints FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON source_reconcile_checkpoints
  USING (harness_tenant_visible(tenant_id))
  WITH CHECK (harness_tenant_visible(tenant_id));
