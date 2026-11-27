-- Makes a job available now (FR-ERR-07, FR-ADM-05), in one statement. $1 schema of the channel (null: the current
-- schema), $2 job id.
--
-- A scheduled, retryable or finalised (completed, cancelled, discarded) job becomes available, scheduled at the
-- database's now, no longer finalised, and without metadata.cancel_attempted_at, so that a cancel asked of an earlier
-- attempt does not cancel the next one (§5.8). Its attempt, max_attempts and errors are kept: a job whose attempts were
-- used up gets one more. Its queue is notified on <schema>.river_insert as for an insert, delivered when the transaction
-- commits; a channel over 63 bytes is skipped. An available, pending or running job is left as it is.
--
-- Returns the job as it is after the call (the notify column is to be ignored), or no row for an unknown id. A unique
-- job whose key another job holds in a state its uniqueness covers fails with a unique violation on river_job_unique_idx.
WITH updated AS (
    UPDATE {{schema}}{{prefix}}job
    SET
        state = 'available',
        scheduled_at = now(),
        finalized_at = NULL,
        metadata = metadata - 'cancel_attempted_at'
    WHERE id = $2 AND state IN ('scheduled', 'retryable', 'completed', 'cancelled', 'discarded')
    RETURNING *
)
SELECT
    u.id, u.kind, u.queue, u.state::text, u.priority, u.args::text, u.metadata::text, u.tags::text[], u.attempt, u.max_attempts,
    u.attempted_at, u.attempted_by, u.errors::text[], u.created_at, u.scheduled_at, u.finalized_at,
    CASE WHEN octet_length(c.channel) <= 63
         THEN pg_notify(c.channel, jsonb_build_object('queue', u.queue)::text) END
FROM updated AS u
CROSS JOIN (SELECT coalesce($1::text, current_schema()) || '.river_insert' AS channel) AS c
UNION ALL
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at,
    NULL
FROM {{schema}}{{prefix}}job AS j
WHERE j.id = $2 AND NOT EXISTS (SELECT 1 FROM updated);
