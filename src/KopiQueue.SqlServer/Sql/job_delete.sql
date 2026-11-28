-- Deletes a job for the admin API (FR-ADM-03), unless it is running. @id the job.
-- Returns the job and whether it was deleted, or no row for an unknown id. The delete checks the state as it holds the
-- row, so a job that started running meanwhile is kept and returned as it is now.
DELETE FROM {{schema}}{{prefix}}job
OUTPUT
    deleted.id, deleted.kind, deleted.queue, deleted.state, deleted.priority, deleted.args, deleted.metadata,
    deleted.tags, deleted.attempt, deleted.max_attempts, deleted.attempted_at, deleted.attempted_by,
    deleted.errors, deleted.created_at, deleted.scheduled_at, deleted.finalized_at,
    CAST(1 AS bit)
WHERE id = @id AND state <> 'running';

IF @@ROWCOUNT = 0
    SELECT
        j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at,
        CAST(0 AS bit)
    FROM {{schema}}{{prefix}}job AS j
    WHERE j.id = @id;
