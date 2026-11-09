-- SQLite mirror of migrations/0027_work_queues.sql.
ALTER TABLE agent_works ADD COLUMN queue TEXT NOT NULL DEFAULT 'routine-work'
  CHECK (queue IN ('critical-mitigation','incident-investigate','routine-work','batch-evaluation','background-improvement'));
UPDATE agent_works SET queue = 'incident-investigate' WHERE kind IN ('incident','ai_infra') AND queue = 'routine-work';
UPDATE agent_works SET queue = 'background-improvement'
  WHERE template_id IN ('postmortem','follow-up') AND queue = 'routine-work';
CREATE INDEX idx_agent_works_queue_wake ON agent_works (queue, wake_at) WHERE wake_at IS NOT NULL;
CREATE INDEX idx_work_run_links_active ON work_run_links (tenant_id, work_id) WHERE role = 'owner';
