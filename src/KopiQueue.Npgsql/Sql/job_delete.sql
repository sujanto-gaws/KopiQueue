-- Deletes a job for the admin API (FR-ADM-03), in one statement, unless it is running. $1 job id.
--
-- Returns the job and whether it was deleted, or no row for an unknown id. A job that started running while this ran is
-- kept: the delete checks the state again once it holds the row. The second branch reads the statement's snapshot, so
-- for such a job it can show the state the job had just before.
WITH deleted AS (
    DELETE FROM {{schema}}{{prefix}}job
    WHERE id = $1 AND state <> 'running'
    RETURNING *
)
SELECT
    d.id, d.kind, d.queue, d.state::text, d.priority, d.args::text, d.metadata::text, d.tags::text[], d.attempt, d.max_attempts,
    d.attempted_at, d.attempted_by, d.errors::text[], d.created_at, d.scheduled_at, d.finalized_at,
    TRUE
FROM deleted AS d
UNION ALL
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at,
    FALSE
FROM {{schema}}{{prefix}}job AS j
WHERE j.id = $1 AND NOT EXISTS (SELECT 1 FROM deleted);
