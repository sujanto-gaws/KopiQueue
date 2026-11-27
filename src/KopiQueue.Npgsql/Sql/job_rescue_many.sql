-- Rescues stuck jobs (design §5.7, FR-MNT-03, NFR-REL-02) in one statement. Each parameter is an array with one element
-- per job: $1 id, $2 attempted_at as the rescuer read it, $3 new state, $4 scheduled_at (null: keep), $5 error to
-- append. $6 schema of the insert channel (null: the current schema).
--
-- A job changes only while it is still running with the attempted_at the rescuer saw, so one that finished, or was
-- fetched again, since it was read is left alone. Rows another transaction holds are skipped, never waited for. The
-- attempt is kept: the stuck attempt counts. A discarded job is finalised at the database's now.
--
-- Each queue that got an available job is notified once, in the statement's transaction, so only on commit. As for
-- inserts, a channel over 63 bytes is skipped rather than failing. Returns the changed jobs, in id order.
WITH job_input AS (
    SELECT *
    FROM unnest($1::bigint[], $2::timestamptz[], $3::text[], $4::timestamptz[], $5::jsonb[])
        AS i (id, attempted_at, state, scheduled_at, error)
),
locked AS (
    SELECT j.id
    FROM {{schema}}{{prefix}}job AS j
    JOIN job_input AS i ON i.id = j.id
    WHERE j.state = 'running'
      AND j.attempted_at = i.attempted_at
    ORDER BY j.id
    FOR UPDATE OF j SKIP LOCKED
),
updated AS (
    UPDATE {{schema}}{{prefix}}job AS j
    SET
        state = i.state::{{schema}}{{prefix}}job_state,
        scheduled_at = coalesce(i.scheduled_at, j.scheduled_at),
        finalized_at = CASE WHEN i.state = 'discarded' THEN now() ELSE j.finalized_at END,
        errors = array_append(j.errors, i.error)
    FROM job_input AS i
    JOIN locked AS l ON l.id = i.id
    WHERE j.id = i.id
      AND j.state = 'running'
      AND j.attempted_at = i.attempted_at
    RETURNING j.*
),
ranked AS (
    SELECT
        u.*,
        u.state = 'available' AND row_number() OVER (PARTITION BY u.queue, u.state ORDER BY u.id) = 1 AS announce
    FROM updated AS u
)
SELECT
    r.id, r.kind, r.queue, r.state::text, r.priority, r.args::text, r.metadata::text, r.tags::text[], r.attempt, r.max_attempts,
    r.attempted_at, r.attempted_by, r.errors::text[], r.created_at, r.scheduled_at, r.finalized_at,
    CASE WHEN r.announce AND octet_length(c.channel) <= 63
        THEN pg_notify(c.channel, jsonb_build_object('queue', r.queue)::text)
    END AS notified
FROM ranked AS r
CROSS JOIN (SELECT coalesce($6::text, current_schema()) || '.river_insert' AS channel) AS c
ORDER BY r.id;
