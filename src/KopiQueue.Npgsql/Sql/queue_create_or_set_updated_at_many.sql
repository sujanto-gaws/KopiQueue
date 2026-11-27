-- Creates the queues a client works, or marks them worked now (FR-QUE-02, design §5.7): clients run it when they start
-- and then every minute, so the queue cleaner keeps their queues. Rows are inserted, and so locked, in name order, so
-- clients refreshing the same queues cannot deadlock. $1 queue names, distinct. Returns the rows with their pause state.
INSERT INTO {{schema}}{{prefix}}queue AS q (name, created_at, updated_at)
SELECT name, now(), now()
FROM unnest($1::text[]) AS name
ORDER BY name
ON CONFLICT (name) DO UPDATE SET updated_at = EXCLUDED.updated_at
RETURNING q.name, q.created_at, q.updated_at, q.paused_at, q.metadata;
