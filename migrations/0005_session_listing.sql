-- Session listing for clients (GET /v1/sessions): latest turn per session.
CREATE INDEX IF NOT EXISTS idx_turns_session_created ON conversation_turns (tenant_id, session_id, created_at DESC);
