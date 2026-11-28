-- Cancels a job from outside (design §5.8, FR-ERR-06, FR-ADM-05), with the semantics of the PostgreSQL driver's
-- statement. @id the job.
--
-- A job that has not started (available, scheduled, retryable, pending) becomes cancelled at once, finalised at the
-- database's now, with no error appended. A running job stays running: metadata.cancel_attempted_at is set to the
-- database's now (§4.1), and the completer's guard and the worker settle how it ends; SQL Server has no LISTEN/NOTIFY,
-- so the worker's client finds the request by polling (job_list_cancel_attempted.sql). A finalised job is left as it is.
-- Returns the job as it is after the call, or no row for an unknown id.
DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE {{schema}}{{prefix}}job
SET state = CASE WHEN state = 'running' THEN state ELSE 'cancelled' END,
    finalized_at = CASE WHEN state = 'running' THEN finalized_at ELSE @now END,
    metadata = CASE
        WHEN state = 'running' THEN JSON_MODIFY(metadata, '$.cancel_attempted_at', CONVERT(nvarchar(30), @now, 126) + N'Z')
        ELSE metadata
    END
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at
WHERE id = @id AND state IN ('available', 'scheduled', 'retryable', 'pending', 'running');

IF @@ROWCOUNT = 0
    SELECT
        j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at
    FROM {{schema}}{{prefix}}job AS j
    WHERE j.id = @id;
