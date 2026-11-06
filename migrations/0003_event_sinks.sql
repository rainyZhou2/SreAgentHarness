-- Event sinks (event_sink plugin kind): a global, commit-safe position in
-- run_events. Readers only consume rows of transactions older than every
-- running transaction (tx_id < pg_snapshot_xmin), so a cursor ordered by
-- (tx_id, global_seq) never skips a late-committing event.
ALTER TABLE run_events ADD COLUMN global_seq BIGSERIAL;
ALTER TABLE run_events ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();
CREATE INDEX idx_events_log ON run_events (tx_id, global_seq);

CREATE TABLE event_sink_cursors (
  sink_id TEXT PRIMARY KEY,
  tx_id xid8 NOT NULL,
  global_seq BIGINT NOT NULL DEFAULT 0,
  owner TEXT,
  lease_until TIMESTAMPTZ,
  delivered BIGINT NOT NULL DEFAULT 0,
  skipped BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
