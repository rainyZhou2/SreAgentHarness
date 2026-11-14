-- Reservation provenance is independent of authoritative Work links. NULL
-- preserves all legacy runs and conversation activation semantics.
ALTER TABLE agent_runs ADD COLUMN work_owner jsonb;

CREATE INDEX agent_runs_work_reservations ON agent_runs (id)
    WHERE work_owner IS NOT NULL AND status NOT IN ('COMPLETED','FAILED','CANCELLED');
