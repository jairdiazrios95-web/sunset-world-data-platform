-- Optional operational task AFTER initial backfill, not executed during SQL init.
-- Must be run outside a transaction due to CONCURRENTLY.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_ai_chunk_hnsw_cosine
ON ai.document_chunk USING hnsw (embedding vector_cosine_ops)
WITH (m=16,ef_construction=64);
