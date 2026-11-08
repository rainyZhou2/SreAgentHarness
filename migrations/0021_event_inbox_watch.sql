-- v2 control plane: unified event inbox and durable watches (RFC §6, §9).
CREATE TABLE event_inbox (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  source TEXT NOT NULL,
  external_event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  correlation_id TEXT,
  dedup_key TEXT,
  envelope JSONB NOT NULL,
  payload_ref TEXT,
  payload_hash TEXT,
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL,
  processing_state TEXT NOT NULL DEFAULT 'NEW'
    CHECK (processing_state IN ('NEW','PROCESSED','IGNORED','FAILED','DEAD')),
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  work_id UUID,
  decision TEXT,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, source, external_event_id)
);
CREATE INDEX idx_event_inbox_pending ON event_inbox (received_at) WHERE processing_state IN ('NEW','FAILED');
CREATE INDEX idx_event_inbox_subject_time ON event_inbox (tenant_id, subject_ref, occurred_at DESC);
CREATE INDEX idx_event_inbox_type_time ON event_inbox (tenant_id, event_type, occurred_at DESC);
CREATE INDEX idx_event_inbox_work ON event_inbox (tenant_id, work_id) WHERE work_id IS NOT NULL;

CREATE TABLE work_watches (
  id UUID PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('event','time','either','periodic','approval')),
  predicate JSONB,
  subjects JSONB,
  purpose TEXT,
  generation INT NOT NULL,
  cursor TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','FIRED','EXPIRED','CANCELLED')),
  fired_by TEXT,
  checks INT NOT NULL DEFAULT 0,
  interval_ms BIGINT NOT NULL DEFAULT 0,
  not_before TIMESTAMPTZ NOT NULL,
  next_check_at TIMESTAMPTZ,
  deadline TIMESTAMPTZ,
  version BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, id),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
CREATE INDEX idx_watches_due ON work_watches (next_check_at) WHERE status = 'ACTIVE' AND next_check_at IS NOT NULL;
CREATE INDEX idx_watches_event ON work_watches (tenant_id, mode) WHERE status = 'ACTIVE';
CREATE INDEX idx_watches_work ON work_watches (tenant_id, work_id, status);

-- One firing per (watch, trigger): an event id, or a deterministic timer id
-- per check, so a re-scanned due watch never wakes a Work twice.
CREATE TABLE watch_firings (
  tenant_id TEXT NOT NULL,
  watch_id UUID NOT NULL,
  firing_id TEXT NOT NULL,
  work_id UUID NOT NULL,
  kind TEXT NOT NULL,
  payload JSONB,
  fired_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, watch_id, firing_id),
  FOREIGN KEY (tenant_id, watch_id) REFERENCES work_watches (tenant_id, id)
);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['event_inbox','work_watches','watch_firings'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
