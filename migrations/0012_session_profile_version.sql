-- A session pins the profile version it was created with. Flow sessions use
-- this to keep every turn on the same runbook version; legacy sessions have 0
-- and fall back to the server's default.
ALTER TABLE conversation_sessions ADD COLUMN profile_version BIGINT NOT NULL DEFAULT 0;