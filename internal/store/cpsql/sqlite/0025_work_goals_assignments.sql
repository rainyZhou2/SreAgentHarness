-- SQLite mirror of migrations/0025_work_goals_assignments.sql. The run
-- tenant check is PostgreSQL-only: local runs live in another database and
-- the desktop runtime has one tenant.
CREATE TABLE work_goals (
  tenant_id TEXT NOT NULL,
  work_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  goal TEXT NOT NULL,
  goal_hash TEXT NOT NULL,
  reason TEXT,
  actor_id TEXT,
  created_at INTEGER NOT NULL,
  PRIMARY KEY (tenant_id, work_id, generation),
  FOREIGN KEY (tenant_id, work_id) REFERENCES agent_works (tenant_id, id)
);
INSERT OR IGNORE INTO work_goals (tenant_id, work_id, generation, goal, goal_hash, created_at)
  SELECT tenant_id, id, generation, goal, goal_hash, created_at FROM agent_works;

ALTER TABLE work_run_links ADD COLUMN cost_micros INTEGER NOT NULL DEFAULT 0;
ALTER TABLE work_run_links ADD COLUMN outcome TEXT;
ALTER TABLE work_run_links ADD COLUMN finished_at INTEGER;
ALTER TABLE work_run_links ADD COLUMN evidence_refs TEXT;

CREATE VIEW work_assignments AS
  SELECT tenant_id, work_id, run_id, generation, stage_id, COALESCE(agent_role, phase) AS role, required,
         status, outcome, cost_micros, evidence_refs, created_at, finished_at
  FROM work_run_links WHERE role = 'owner' AND stage_id IS NOT NULL;

ALTER TABLE event_inbox ADD COLUMN payload_purged_at INTEGER;
CREATE INDEX idx_event_inbox_retention ON event_inbox (received_at)
  WHERE processing_state IN ('PROCESSED','IGNORED','DEAD');
