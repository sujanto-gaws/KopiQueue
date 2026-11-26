-- Locks and starts up to $3 due, available jobs of queue $1 for client $2 (design §5.1, FR-EXE-01, FR-EXE-02).
-- SKIP LOCKED leaves jobs another fetcher holds to that fetcher. attempted_by keeps the newest $4 entries: after the
-- append the array has cardinality + 1 elements, so the slice starts at cardinality + 2 - $4.
-- UPDATE ... RETURNING has no order; the driver sorts the rows into fetch order.
WITH locked AS (
    SELECT id
    FROM {{schema}}{{prefix}}job
    WHERE state = 'available'
      AND queue = $1
      AND scheduled_at <= now()
    ORDER BY priority, scheduled_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE {{schema}}{{prefix}}job AS j
SET state        = 'running',
    attempt      = j.attempt + 1,
    attempted_at = now(),
    attempted_by = (coalesce(j.attempted_by, '{}'::text[]) || $2::text)[greatest(1, coalesce(cardinality(j.attempted_by), 0) + 2 - $4::integer):]
FROM locked
WHERE j.id = locked.id
RETURNING
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at;
