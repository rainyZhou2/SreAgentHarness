-- Scheduling queues of Works (RFC §20.2): each queue has its own claim
-- loop, concurrency and model-run budget. Existing incidents investigate,
-- everything else is routine work.
ALTER TABLE agent_works ADD COLUMN IF NOT EXISTS queue TEXT NOT NULL DEFAULT 'routine-work'
  CHECK (queue IN ('critical-mitigation','incident-investigate','routine-work','batch-evaluation','background-improvement'));
UPDATE agent_works SET queue = 'incident-investigate' WHERE kind IN ('incident','ai_infra') AND queue = 'routine-work';
UPDATE agent_works SET queue = 'background-improvement'
  WHERE template_id IN ('postmortem','follow-up') AND queue = 'routine-work';
-- Claiming due Works of one queue.
CREATE INDEX IF NOT EXISTS idx_agent_works_queue_wake ON agent_works (queue, wake_at) WHERE wake_at IS NOT NULL;
-- Counting a tenant's active model runs per queue (resource pools).
CREATE INDEX IF NOT EXISTS idx_work_run_links_active ON work_run_links (tenant_id, work_id) WHERE role = 'owner';
