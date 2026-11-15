CREATE TABLE alert_symptoms (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL,
  symptom_key TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  kind TEXT NOT NULL,
  impact_scope TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('FIRING','RESOLVED')),
  window_start TIMESTAMPTZ NOT NULL,
  window_end TIMESTAMPTZ NOT NULL,
  active_episodes BIGINT NOT NULL CHECK (active_episodes >= 0),
  version BIGINT NOT NULL CHECK (version > 0),
  record JSONB NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, symptom_key)
);
CREATE INDEX idx_alert_symptoms_subject ON alert_symptoms (tenant_id, subject_ref, state, window_end DESC);

CREATE TABLE alert_episodes (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL,
  fingerprint TEXT NOT NULL,
  alert_name TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('FIRING','RESOLVED')),
  symptom_id UUID NOT NULL,
  opened_at TIMESTAMPTZ NOT NULL,
  last_event_at TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ,
  reopen_count BIGINT NOT NULL DEFAULT 0 CHECK (reopen_count >= 0),
  flapping BOOLEAN NOT NULL DEFAULT false,
  version BIGINT NOT NULL CHECK (version > 0),
  record JSONB NOT NULL,
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, symptom_id) REFERENCES alert_symptoms (tenant_id, id)
);
CREATE UNIQUE INDEX idx_alert_episode_open ON alert_episodes (tenant_id, fingerprint) WHERE state='FIRING';
CREATE INDEX idx_alert_episode_resolution ON alert_episodes (tenant_id, alert_name, subject_ref, state, last_event_at DESC);
CREATE INDEX idx_alert_episode_symptom ON alert_episodes (tenant_id, symptom_id, state);

CREATE TABLE alert_instances (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL,
  inbox_id UUID NOT NULL,
  episode_id UUID,
  symptom_id UUID NOT NULL,
  fingerprint TEXT,
  alert_name TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('alert.firing','alert.resolved')),
  occurred_at TIMESTAMPTZ NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  record JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, inbox_id),
  FOREIGN KEY (tenant_id, symptom_id) REFERENCES alert_symptoms (tenant_id, id)
);
CREATE INDEX idx_alert_instances_episode ON alert_instances (tenant_id, episode_id, occurred_at DESC);
CREATE INDEX idx_alert_instances_subject ON alert_instances (tenant_id, subject_ref, occurred_at DESC);

DO $$
DECLARE tbl TEXT;
BEGIN
  FOREACH tbl IN ARRAY ARRAY['alert_symptoms','alert_episodes','alert_instances'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tbl);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tbl);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (harness_tenant_visible(tenant_id)) WITH CHECK (harness_tenant_visible(tenant_id))', tbl);
  END LOOP;
END $$;
