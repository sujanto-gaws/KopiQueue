-- One statement of the scheduler's batch (design §5.7, §8.1, FR-SCH-01, FR-MNT-02), with the semantics of the PostgreSQL
-- driver's statement. The driver runs it in a transaction, the caller's or its own, so the due rows stay locked from the
-- read to the update. A batch larger than 1,000 runs as several statements, each continuing after the last due position
-- of the one before (@after_at, @after_id; null for the first), so jobs a statement leaves alone are not read again.
-- @max the most due jobs, @error the error message for a unique conflict, @scope the unique-key lock scope.
--
-- Up to @max scheduled and retryable jobs that are due by the database's clock, most overdue first, become available.
-- Rows another transaction holds are skipped, never waited for. Nothing else about a promoted job changes.
--
-- Unique jobs (design §5.6): a due job "enters" its key's slot on promotion when its unique states include available
-- but not its current state, which for a valid set (FR-UNQ-04) means a retryable job whose states leave out retryable.
-- Its key's lock is taken first, in key order as inserts take theirs, so an insert of the same key cannot slip in
-- between the check and the update. It conflicts if another job holds the key (unique_slot = 0), or if a more overdue
-- job of the batch enters with the same key. A conflicting job is discarded instead: it cannot be retried. It gets
-- finalized_at and one error, in the format the executor writes. One whose states include discarded would conflict
-- either way, so it is left as it is until the slot is free.
--
-- Returns every due job: id, queue, state, scheduled_at, and whether it changed (a job left as it is has 0).
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @now_json nvarchar(40) = CONVERT(nvarchar(30), @now, 126) + N'Z';
DECLARE @due TABLE (
    id              bigint        PRIMARY KEY,
    scheduled_at    datetime2(6)  NOT NULL,
    unique_key      varbinary(32) NULL,
    entering        bit           NOT NULL,
    keeps_discarded bit           NOT NULL
);
DECLARE @conflicting TABLE (id bigint PRIMARY KEY);

INSERT INTO @due (id, scheduled_at, unique_key, entering, keeps_discarded)
SELECT TOP (@max)
    j.id,
    j.scheduled_at,
    j.unique_key,
    CASE
        WHEN j.unique_key IS NOT NULL AND j.unique_states IS NOT NULL
         AND (j.unique_states & 1) <> 0
         AND (j.unique_states & CASE j.state WHEN 'retryable' THEN 32 ELSE 128 END) = 0
        THEN 1 ELSE 0
    END,
    CASE WHEN (ISNULL(j.unique_states, 0) & 8) <> 0 THEN 1 ELSE 0 END
FROM {{schema}}{{prefix}}job AS j WITH (UPDLOCK, READPAST, ROWLOCK, FORCESEEK ({{prefix}}job_schedule_idx (scheduled_at)))
WHERE j.state IN ('retryable', 'scheduled')
  AND j.scheduled_at <= @now
  AND j.scheduled_at >= ISNULL(@after_at, '0001-01-01')
  AND (@after_id IS NULL OR j.scheduled_at > @after_at OR j.id > @after_id)
ORDER BY j.scheduled_at, j.id;

DECLARE @unique_key varbinary(32) = (SELECT MIN(unique_key) FROM @due WHERE entering = 1);
DECLARE @unique_lock_resource nvarchar(255);
DECLARE @unique_lock_result int;
WHILE @unique_key IS NOT NULL
BEGIN
    SET @unique_lock_resource = N'kopi_unique:' + @scope + N':' + CONVERT(nvarchar(64), @unique_key, 2);
    EXEC @unique_lock_result = sys.sp_getapplock
        @Resource = @unique_lock_resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = -1;
    IF @unique_lock_result < 0
        THROW 50001, N'KopiQueue could not take a unique-key lock: sp_getapplock failed, for example as a deadlock victim.', 1;
    SET @unique_key = (SELECT MIN(unique_key) FROM @due WHERE entering = 1 AND unique_key > @unique_key);
END;

INSERT INTO @conflicting (id)
SELECT e.id
FROM (
    SELECT id, unique_key, ROW_NUMBER() OVER (PARTITION BY unique_key ORDER BY scheduled_at, id) AS place
    FROM @due
    WHERE entering = 1
) AS e
WHERE e.place > 1
   OR EXISTS (
       SELECT 1
       FROM {{schema}}{{prefix}}job AS holder WITH (READCOMMITTEDLOCK, FORCESEEK ({{prefix}}job_unique_idx (unique_key, unique_slot)))
       WHERE holder.unique_key = e.unique_key AND holder.unique_slot = 0
         AND holder.unique_key IS NOT NULL -- the unique index's filter, which FORCESEEK needs stated
   );

UPDATE j
SET state = CASE WHEN c.id IS NULL THEN 'available' ELSE 'discarded' END,
    finalized_at = CASE WHEN c.id IS NULL THEN j.finalized_at ELSE @now END,
    errors = CASE
        WHEN c.id IS NULL THEN j.errors
        ELSE JSON_MODIFY(ISNULL(j.errors, N'[]'), 'append $', JSON_QUERY((
            SELECT @now_json AS [at], j.attempt AS [attempt], @error AS [error], N'' AS [trace]
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)))
    END
OUTPUT inserted.id, inserted.queue, inserted.state, inserted.scheduled_at, CAST(1 AS bit)
FROM {{schema}}{{prefix}}job AS j
JOIN @due AS d ON d.id = j.id
LEFT JOIN @conflicting AS c ON c.id = d.id
WHERE c.id IS NULL OR d.keeps_discarded = 0;

SELECT j.id, j.queue, j.state, j.scheduled_at, CAST(0 AS bit)
FROM @due AS d
JOIN @conflicting AS c ON c.id = d.id
JOIN {{schema}}{{prefix}}job AS j ON j.id = d.id
WHERE d.keeps_discarded = 1;
