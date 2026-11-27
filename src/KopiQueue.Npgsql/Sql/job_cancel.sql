-- Cancels a job from outside (design §5.8, FR-ERR-06, FR-ADM-05), in one statement. $1 schema of the channel (null: the
-- current schema), $2 job id.
--
-- A job that has not started (available, scheduled, retryable, pending) becomes cancelled at once, finalised at the
-- database's now, with no error appended. A running job stays running: metadata.cancel_attempted_at is set to the
-- database's now (§4.1), and {"action": "cancel", "job_id": ..., "queue": ...} goes out on <schema>.river_control
-- (§4.3), delivered when the transaction commits; the completer's guard and the worker settle how it ends. A finalised
-- job (completed, cancelled, discarded) is left as it is. A channel over 63 bytes is skipped, as for inserts.
-- Returns the job as it is after the call (the notify column is to be ignored), or no row for an unknown id.
WITH updated AS (
    UPDATE {{schema}}{{prefix}}job
    SET
        state = CASE WHEN state = 'running' THEN state ELSE 'cancelled'::{{schema}}{{prefix}}job_state END,
        finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE now() END,
        metadata = CASE
            WHEN state = 'running' THEN metadata || jsonb_build_object('cancel_attempted_at', now())
            ELSE metadata
        END
    WHERE id = $2 AND state IN ('available', 'scheduled', 'retryable', 'pending', 'running')
    RETURNING *
)
SELECT
    u.id, u.kind, u.queue, u.state::text, u.priority, u.args::text, u.metadata::text, u.tags::text[], u.attempt, u.max_attempts,
    u.attempted_at, u.attempted_by, u.errors::text[], u.created_at, u.scheduled_at, u.finalized_at,
    CASE WHEN u.state = 'running' AND octet_length(c.channel) <= 63
         THEN pg_notify(c.channel, json_build_object('action', 'cancel', 'job_id', u.id, 'queue', u.queue)::text) END
FROM updated AS u
CROSS JOIN (SELECT coalesce($1::text, current_schema()) || '.river_control' AS channel) AS c
UNION ALL
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at,
    NULL
FROM {{schema}}{{prefix}}job AS j
WHERE j.id = $2 AND NOT EXISTS (SELECT 1 FROM updated);
