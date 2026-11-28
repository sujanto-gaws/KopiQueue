-- Locks and starts up to @max due, available jobs of queue @queue for client @client_id (design §5.1, §8.1,
-- FR-EXE-01, FR-EXE-02). The rules of §8.1 apply: READPAST needs READ COMMITTED, which a pooled session may have lost
-- to SNAPSHOT, and the SET lasts only for this batch; FORCESEEK keeps the plan on the filtered fetch index, so it never
-- scans and escalates. UPDLOCK with READPAST is SKIP LOCKED: jobs another fetcher holds, and uncommitted inserts, are
-- skipped without waiting.
-- attempted_by keeps the newest @max_attempted_by entries. A first attempt starts the array; a later one rebuilds it
-- from its elements, newest last.
-- OUTPUT has no order; the driver sorts the rows into fetch order.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

WITH locked AS (
    SELECT TOP (@max) *
    FROM {{schema}}{{prefix}}job WITH (UPDLOCK, READPAST, ROWLOCK, FORCESEEK ({{prefix}}job_fetch_idx (queue)))
    WHERE state = 'available'
      AND queue = @queue
      AND scheduled_at <= SYSUTCDATETIME()
    ORDER BY priority, scheduled_at, id
)
UPDATE locked
SET state        = 'running',
    attempt      = attempt + 1,
    attempted_at = SYSUTCDATETIME(),
    attempted_by = CASE
        WHEN attempted_by IS NULL THEN N'["' + STRING_ESCAPE(@client_id, 'json') + N'"]'
        ELSE (
            SELECT N'[' + STRING_AGG(CAST(e.v AS nvarchar(max)), N',') WITHIN GROUP (ORDER BY e.k) + N']'
            FROM (
                SELECT x.k, x.v, ROW_NUMBER() OVER (ORDER BY x.k DESC) AS newest
                FROM (
                    SELECT CAST(a.[key] AS int) AS k, N'"' + STRING_ESCAPE(a.[value], 'json') + N'"' AS v
                    FROM OPENJSON(attempted_by) AS a
                    UNION ALL
                    SELECT 2147483647, N'"' + STRING_ESCAPE(@client_id, 'json') + N'"'
                ) AS x
            ) AS e
            WHERE e.newest <= @max_attempted_by
        )
    END
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at;
