-- The scheduler's batch (design §5.7, FR-SCH-01, FR-MNT-02), in one statement. $1 batch size, $2 the error message for
-- a unique conflict, $3 schema of the insert channel (null: the current schema).
--
-- Up to $1 scheduled and retryable jobs that are due by the database's clock, most overdue first, become available.
-- Rows another transaction holds are skipped, never waited for. Nothing else about a promoted job changes.
--
-- Unique jobs (design §5.6): the partial unique index covers a job only while its state is in its unique_states. A due
-- job "enters" the index on promotion when its states include available but not its current state, which for a valid
-- set (FR-UNQ-04) means a retryable job whose states leave out retryable. It conflicts if another job already holds
-- its key, or if an older due job of the batch enters with the same key. A conflicting job is discarded instead: it
-- cannot be retried. It gets finalized_at and one error, in the format the executor writes. One whose states include
-- discarded would conflict either way, so it is left as it is until the slot is free.
--
-- Each queue that got an available job is notified once, in the statement's transaction, so only on commit. As for
-- inserts, a channel over 63 bytes is skipped rather than failing. Returns the changed jobs' id, queue and new state.
WITH due AS (
    SELECT id, state, scheduled_at, unique_key, unique_states
    FROM {{schema}}{{prefix}}job
    WHERE state IN ('retryable', 'scheduled')
      AND scheduled_at <= now()
    ORDER BY scheduled_at, id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
),
entering AS (
    SELECT
        id,
        unique_key,
        unique_states,
        row_number() OVER (PARTITION BY unique_key ORDER BY scheduled_at, id) AS place
    FROM due
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND {{schema}}{{prefix}}job_state_in_bitmask(unique_states, 'available'::{{schema}}{{prefix}}job_state)
      AND NOT {{schema}}{{prefix}}job_state_in_bitmask(unique_states, state)
),
conflicting AS (
    SELECT e.id, e.unique_states
    FROM entering AS e
    WHERE e.place > 1
       OR EXISTS (
           SELECT 1
           FROM {{schema}}{{prefix}}job AS holder
           WHERE holder.unique_key = e.unique_key
             AND holder.unique_key IS NOT NULL
             AND holder.unique_states IS NOT NULL
             AND {{schema}}{{prefix}}job_state_in_bitmask(holder.unique_states, holder.state)
       )
),
updated AS (
    UPDATE {{schema}}{{prefix}}job AS j
    SET
        state = CASE
            WHEN c.id IS NULL THEN 'available'::{{schema}}{{prefix}}job_state
            ELSE 'discarded'::{{schema}}{{prefix}}job_state
        END,
        finalized_at = CASE WHEN c.id IS NULL THEN j.finalized_at ELSE now() END,
        errors = CASE
            WHEN c.id IS NULL THEN j.errors
            ELSE array_append(
                j.errors,
                jsonb_build_object(
                    'at', rtrim(rtrim(to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US'), '0'), '.') || 'Z',
                    'attempt', j.attempt,
                    'error', $2::text,
                    'trace', ''))
        END
    FROM due AS d
    LEFT JOIN conflicting AS c ON c.id = d.id
    WHERE j.id = d.id
      AND (c.id IS NULL OR NOT {{schema}}{{prefix}}job_state_in_bitmask(c.unique_states, 'discarded'::{{schema}}{{prefix}}job_state))
    RETURNING j.id, j.queue, j.state
),
ranked AS (
    SELECT
        id,
        queue,
        state,
        state = 'available' AND row_number() OVER (PARTITION BY queue, state ORDER BY id) = 1 AS announce
    FROM updated
)
SELECT
    r.id,
    r.queue,
    r.state::text,
    CASE WHEN r.announce AND octet_length(c.channel) <= 63
        THEN pg_notify(c.channel, jsonb_build_object('queue', r.queue)::text)
    END AS notified
FROM ranked AS r
CROSS JOIN (SELECT coalesce($3::text, current_schema()) || '.river_insert' AS channel) AS c
ORDER BY r.id;
