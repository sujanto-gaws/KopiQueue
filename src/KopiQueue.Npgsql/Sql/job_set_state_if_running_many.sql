-- Records the outcomes of finished attempts (design §5.3), as River's JobSetStateIfRunningMany (v0.51.2). Each
-- parameter is an array with one element per job; a NULL element leaves that column as it is.
-- $1 id, $2 attempt, $3 error to append, $4 finalized_at, $5 metadata to merge, $6 scheduled_at, $7 state.
-- Only a job that is still running changes, so a job that was rescued, cancelled or deleted meanwhile is not
-- clobbered; metadata is merged either way, as River does. A job whose cancellation was asked for while it ran
-- (metadata.cancel_attempted_at) and that would run again is cancelled instead (design §5.8).
-- Every given job that exists is returned, changed or not, in id order.
WITH job_input AS (
    SELECT *
    FROM unnest(
        $1::bigint[], $2::integer[], $3::jsonb[], $4::timestamptz[], $5::jsonb[], $6::timestamptz[], $7::text[]
    ) AS i (id, attempt, error, finalized_at, metadata_updates, scheduled_at, state)
),
updated AS (
    UPDATE {{schema}}{{prefix}}job AS j
    SET
        attempt = CASE
            WHEN j.state = 'running'
                 AND NOT (i.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                 AND i.attempt IS NOT NULL
            THEN i.attempt
            ELSE j.attempt
        END,
        errors = CASE
            WHEN j.state = 'running' AND i.error IS NOT NULL
            THEN array_append(j.errors, i.error)
            ELSE j.errors
        END,
        finalized_at = CASE
            WHEN j.state = 'running'
                 AND (i.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
            THEN now()
            WHEN j.state = 'running' AND i.finalized_at IS NOT NULL
            THEN i.finalized_at
            ELSE j.finalized_at
        END,
        metadata = CASE
            WHEN i.metadata_updates IS NOT NULL
            THEN j.metadata || i.metadata_updates
            ELSE j.metadata
        END,
        scheduled_at = CASE
            WHEN j.state = 'running'
                 AND NOT (i.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
                 AND i.scheduled_at IS NOT NULL
            THEN i.scheduled_at
            ELSE j.scheduled_at
        END,
        state = CASE
            WHEN j.state = 'running'
                 AND (i.state IN ('available', 'retryable', 'scheduled') AND j.metadata ? 'cancel_attempted_at')
            THEN 'cancelled'::{{schema}}{{prefix}}job_state
            WHEN j.state = 'running'
            THEN i.state::{{schema}}{{prefix}}job_state
            ELSE j.state
        END
    FROM job_input AS i
    WHERE j.id = i.id
      AND (j.state = 'running' OR i.metadata_updates IS NOT NULL)
    RETURNING j.*
)
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at
FROM {{schema}}{{prefix}}job AS j
JOIN job_input AS i ON i.id = j.id
WHERE NOT EXISTS (SELECT 1 FROM updated AS u WHERE u.id = j.id)
UNION ALL
SELECT
    u.id, u.kind, u.queue, u.state::text, u.priority, u.args::text, u.metadata::text, u.tags::text[], u.attempt, u.max_attempts,
    u.attempted_at, u.attempted_by, u.errors::text[], u.created_at, u.scheduled_at, u.finalized_at
FROM updated AS u
ORDER BY id;
