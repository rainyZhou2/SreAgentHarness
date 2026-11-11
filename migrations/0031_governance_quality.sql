-- v3: world collector health, correlation decisions, change ledger and the
-- runbook registry (design §9.4, §9.5, §10.4, §12.2).
-- Collector health: a collector that stops reporting never deletes facts;
-- its facts become stale and stop satisfying action preconditions.
CREATE TABLE world_collector_health (
  tenant_id TEXT NOT NULL,
  collector TEXT NOT NULL,
  last_success_at TIMESTAMPTZ,
  last_attempt_at TIMESTAMPTZ,
  last_full_sync TIMESTAMPTZ,
  last_error TEXT,
  error_count BIGINT NOT NULL DEFAULT 0,
  max_age_ms BIGINT NOT NULL DEFAULT 0,
  watch_cursor TEXT,
  source_version TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, collector)
);

-- Why an event was routed where it went: the rule and its version, the
-- candidates considered and the facts the decision relied on.
CREATE TABLE event_correlation_decisions (
  tenant_id TEXT NOT NULL,
  inbox_id UUID NOT NULL,
  rule_name TEXT,
  rule_version TEXT,
  decision TEXT NOT NULL,
  work_id UUID,
  detail JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, inbox_id)
);
CREATE INDEX idx_event_decisions_work ON event_correlation_decisions (tenant_id, work_id, created_at DESC);

-- Change ledger: releases, config, topology and manual operations with the
-- subjects they touched. A change near a symptom is a hypothesis, not a cause.
CREATE TABLE change_records (
  tenant_id TEXT NOT NULL,
  change_id TEXT NOT NULL,
  category TEXT NOT NULL CHECK (category IN ('release','config','topology','manual')),
  source TEXT NOT NULL,
  actor TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ,
  record JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, change_id)
);
CREATE INDEX idx_change_records_time ON change_records (tenant_id, started_at DESC);
CREATE TABLE change_subjects (
  tenant_id TEXT NOT NULL,
  change_id TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, change_id, subject_ref),
  FOREIGN KEY (tenant_id, change_id) REFERENCES change_records (tenant_id, change_id) ON DELETE CASCADE
);
CREATE INDEX idx_change_subjects_ref ON change_subjects (tenant_id, subject_ref, started_at DESC);

-- Runbook registry: metadata over a Work template (owner, validity,
-- rehearsal) and its track record.
CREATE TABLE runbooks (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  version INT NOT NULL,
  spec JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'DRAFT' CHECK (status IN ('DRAFT','ACTIVE','EXPIRED','RETIRED')),
  last_rehearsed_at TIMESTAMPTZ,
  executions BIGINT NOT NULL DEFAULT 0,
  successes BIGINT NOT NULL DEFAULT 0,
  unknown_effects BIGINT NOT NULL DEFAULT 0,
  approvals JSONB,
  row_version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, id, version)
);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['world_collector_health','event_correlation_decisions','change_records','change_subjects','runbooks'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
