-- The rescuer's candidates (design §5.7, FR-MNT-03): up to @max running jobs whose attempt started more than the
-- stuck period (@stuck_s seconds plus @stuck_us microseconds) before the database's now, oldest attempt first. Nothing
-- is locked: the rescue that follows checks each job again, and rows another transaction holds, such as a job being
-- completed, are skipped rather than waited for. The database's now comes back as the last column, also on the single
-- row of nulls returned when nothing is stuck.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @cutoff datetime2(6) = DATEADD(microsecond, -@stuck_us, DATEADD(second, -@stuck_s, @now));

SELECT
    j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at,
    c.now
FROM (SELECT @now AS now) AS c
OUTER APPLY (
    SELECT TOP (@max) *
    FROM {{schema}}{{prefix}}job WITH (READPAST, FORCESEEK ({{prefix}}job_running_idx (attempted_at)))
    WHERE state = 'running'
      AND attempted_at < @cutoff
    ORDER BY attempted_at, id
) AS j
ORDER BY j.attempted_at, j.id;
