-- Rescues stuck jobs (design §5.7, FR-MNT-03, NFR-REL-02), up to 1,000 in one statement. @batch is a JSON array with one
-- object per job: i id, at attempted_at as the rescuer read it, st new state, s scheduled_at (absent: keep), e the
-- error to append (as a string).
--
-- A job changes only while it is still running with the attempted_at the rescuer saw, so one that finished, or was
-- fetched again, since it was read is left alone. Rows another transaction holds are skipped, never waited for. The
-- attempt is kept: the stuck attempt counts. A discarded job is finalised at the database's now.
-- Returns the changed jobs; the driver sorts them by id.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE j
SET state = r.state,
    scheduled_at = ISNULL(r.scheduled_at, j.scheduled_at),
    finalized_at = CASE WHEN r.state = 'discarded' THEN @now ELSE j.finalized_at END,
    errors = JSON_MODIFY(ISNULL(j.errors, N'[]'), 'append $', JSON_QUERY(r.error))
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at
FROM {{schema}}{{prefix}}job AS j WITH (UPDLOCK, READPAST, ROWLOCK)
JOIN OPENJSON(@batch) WITH (
    id           bigint        '$.i',
    attempted_at datetime2(7)  '$.at',
    state        varchar(16)   '$.st',
    scheduled_at datetime2(7)  '$.s',
    error        nvarchar(max) '$.e'
) AS r ON r.id = j.id
WHERE j.state = 'running'
  AND j.attempted_at = r.attempted_at;
