-- Makes a job available now (FR-ERR-07, FR-ADM-05), with the semantics of the PostgreSQL driver's statement. @id the job.
--
-- A scheduled, retryable or finalised (completed, cancelled, discarded) job becomes available, scheduled at the
-- database's now, no longer finalised, and without metadata.cancel_attempted_at, so that a cancel asked of an earlier
-- attempt does not cancel the next one (§5.8). Its attempt, max_attempts and errors are kept: a job whose attempts were
-- used up gets one more. An available, pending or running job is left as it is. Clients find the job at their next
-- poll, since SQL Server has no LISTEN/NOTIFY.
--
-- Returns the job as it is after the call, or no row for an unknown id. A unique job whose key another job holds fails
-- with a duplicate key error on the unique index (2601), which the driver reports.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE {{schema}}{{prefix}}job
SET state = 'available',
    scheduled_at = @now,
    finalized_at = NULL,
    metadata = JSON_MODIFY(metadata, '$.cancel_attempted_at', NULL)
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at
WHERE id = @id AND state IN ('scheduled', 'retryable', 'completed', 'cancelled', 'discarded');

IF @@ROWCOUNT = 0
    SELECT
        j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at
    FROM {{schema}}{{prefix}}job AS j
    WHERE j.id = @id;
