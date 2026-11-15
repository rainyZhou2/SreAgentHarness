ALTER TABLE work_events ADD COLUMN correlation_id TEXT;
ALTER TABLE work_events ADD COLUMN correlation_key TEXT;
ALTER TABLE work_events ADD COLUMN received_at INTEGER;
CREATE INDEX idx_work_events_correlation ON work_events (tenant_id, correlation_key, work_id, event_seq);

CREATE TABLE work_outbox_messages (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  subject TEXT NOT NULL,
  payload TEXT NOT NULL,
  publish_attempt INTEGER NOT NULL DEFAULT 0,
  next_retry_at INTEGER NOT NULL,
  claimed_until INTEGER,
  published_at INTEGER,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_work_outbox_due ON work_outbox_messages (next_retry_at, created_at)
  WHERE published_at IS NULL;

CREATE TABLE source_reconcile_checkpoints (
  tenant_id TEXT NOT NULL,
  source TEXT NOT NULL,
  cursor TEXT NOT NULL DEFAULT '',
  owner TEXT,
  fencing_token INTEGER NOT NULL DEFAULT 0,
  lease_until INTEGER,
  next_retry_at INTEGER,
  last_error TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, source)
);
