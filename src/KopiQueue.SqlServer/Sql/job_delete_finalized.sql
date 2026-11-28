-- One statement of the job cleaner's batch (design §5.7, §8.1, FR-MNT-04). The retention of completed, cancelled and
-- discarded jobs is @completed_s/@completed_us, @cancelled_s/@cancelled_us and @discarded_s/@discarded_us, seconds plus
-- microseconds (null: keep that state's jobs); @max the most jobs, at most 1,000.
--
-- Deletes up to @max finalised jobs whose finalized_at is older than their state's retention by the database's clock,
-- oldest first. The jobs are found first, without locks: each state's oldest from the (state, finalized_at) index, so
-- no more rows are read than could be deleted. A locking read would lock every row the sort reads, not only the first
-- @max, and could escalate. The delete then checks the state and cutoff again, skipping rows another transaction holds,
-- so a job that left its finalised state meanwhile (JobRetry, FR-ERR-07) is not deleted. Returns the deleted jobs' ids.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @completed_cutoff datetime2(6) = DATEADD(microsecond, -@completed_us, DATEADD(second, -@completed_s, @now));
DECLARE @cancelled_cutoff datetime2(6) = DATEADD(microsecond, -@cancelled_us, DATEADD(second, -@cancelled_s, @now));
DECLARE @discarded_cutoff datetime2(6) = DATEADD(microsecond, -@discarded_us, DATEADD(second, -@discarded_s, @now));
DECLARE @expired TABLE (id bigint PRIMARY KEY);

INSERT INTO @expired (id)
SELECT TOP (@max) e.id
FROM (
    SELECT TOP (@max) id, finalized_at
    FROM {{schema}}{{prefix}}job WITH (READPAST, FORCESEEK ({{prefix}}job_finalized_idx (state, finalized_at)))
    WHERE finalized_at IS NOT NULL AND state = 'completed' AND finalized_at < @completed_cutoff
    ORDER BY finalized_at, id
    UNION ALL
    SELECT TOP (@max) id, finalized_at
    FROM {{schema}}{{prefix}}job WITH (READPAST, FORCESEEK ({{prefix}}job_finalized_idx (state, finalized_at)))
    WHERE finalized_at IS NOT NULL AND state = 'cancelled' AND finalized_at < @cancelled_cutoff
    ORDER BY finalized_at, id
    UNION ALL
    SELECT TOP (@max) id, finalized_at
    FROM {{schema}}{{prefix}}job WITH (READPAST, FORCESEEK ({{prefix}}job_finalized_idx (state, finalized_at)))
    WHERE finalized_at IS NOT NULL AND state = 'discarded' AND finalized_at < @discarded_cutoff
    ORDER BY finalized_at, id
) AS e
ORDER BY e.finalized_at, e.id;

DELETE j
OUTPUT deleted.id
FROM {{schema}}{{prefix}}job AS j WITH (READPAST, ROWLOCK)
JOIN @expired AS e ON e.id = j.id
WHERE (j.state = 'completed' AND j.finalized_at < @completed_cutoff)
   OR (j.state = 'cancelled' AND j.finalized_at < @cancelled_cutoff)
   OR (j.state = 'discarded' AND j.finalized_at < @discarded_cutoff);
