-- CJK-aware memory search. The 'simple' text-search parser does not split
-- CJK text, so the application stores its character bigrams in
-- search_terms and the full-text vector indexes them with the text.
-- search_terms_v marks rows whose terms were computed (0: backfill pending).
ALTER TABLE agent_memories ADD COLUMN search_terms TEXT NOT NULL DEFAULT '';
ALTER TABLE agent_memories ADD COLUMN search_terms_v INT NOT NULL DEFAULT 0;
DROP INDEX IF EXISTS idx_memory_tsv;
ALTER TABLE agent_memories DROP COLUMN search_tsv;
ALTER TABLE agent_memories ADD COLUMN search_tsv tsvector GENERATED ALWAYS AS
  (to_tsvector('simple', coalesce(title,'') || ' ' || coalesce(content_text,'') || ' ' || subject || ' ' || search_terms)) STORED;
CREATE INDEX idx_memory_tsv ON agent_memories USING GIN (search_tsv);
CREATE INDEX idx_memory_terms_pending ON agent_memories (updated_at) WHERE search_terms_v = 0;
