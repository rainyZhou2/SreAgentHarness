CREATE TABLE alert_symptoms (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  symptom_key TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  kind TEXT NOT NULL,
  impact_scope TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('FIRING','RESOLVED')),
  window_start INTEGER NOT NULL,
  window_end INTEGER NOT NULL,
  active_episodes INTEGER NOT NULL CHECK (active_episodes >= 0),
  version INTEGER NOT NULL CHECK (version > 0),
  record TEXT NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, symptom_key)
);
CREATE INDEX idx_alert_symptoms_subject ON alert_symptoms (tenant_id, subject_ref, state, window_end DESC);

CREATE TABLE alert_episodes (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  alert_name TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('FIRING','RESOLVED')),
  symptom_id TEXT NOT NULL,
  opened_at INTEGER NOT NULL,
  last_event_at INTEGER NOT NULL,
  resolved_at INTEGER,
  reopen_count INTEGER NOT NULL DEFAULT 0 CHECK (reopen_count >= 0),
  flapping INTEGER NOT NULL DEFAULT 0,
  version INTEGER NOT NULL CHECK (version > 0),
  record TEXT NOT NULL,
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, symptom_id) REFERENCES alert_symptoms (tenant_id, id)
);
CREATE UNIQUE INDEX idx_alert_episode_open ON alert_episodes (tenant_id, fingerprint) WHERE state='FIRING';
CREATE INDEX idx_alert_episode_resolution ON alert_episodes (tenant_id, alert_name, subject_ref, state, last_event_at DESC);
CREATE INDEX idx_alert_episode_symptom ON alert_episodes (tenant_id, symptom_id, state);

CREATE TABLE alert_instances (
  tenant_id TEXT NOT NULL,
  id TEXT NOT NULL,
  inbox_id TEXT NOT NULL,
  episode_id TEXT,
  symptom_id TEXT NOT NULL,
  fingerprint TEXT,
  alert_name TEXT NOT NULL,
  subject_ref TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (event_type IN ('alert.firing','alert.resolved')),
  occurred_at INTEGER NOT NULL,
  observed_at INTEGER NOT NULL,
  record TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, inbox_id),
  FOREIGN KEY (tenant_id, symptom_id) REFERENCES alert_symptoms (tenant_id, id)
);
CREATE INDEX idx_alert_instances_episode ON alert_instances (tenant_id, episode_id, occurred_at DESC);
CREATE INDEX idx_alert_instances_subject ON alert_instances (tenant_id, subject_ref, occurred_at DESC);
