-- Inserts up to 1,000 jobs, some of them unique (design §5.6, §8.1, FR-UNQ-02, FR-UNQ-03, ADR-011). The driver runs it
-- after job_unique_lock.sql, in one transaction, so every key of the batch is locked. @jobs is the array of
-- job_insert_many.sql, plus "uk" (the unique key as 64 hex digits) and "us" (the unique-states bitmask) for unique jobs.
-- The driver sends each key at most once per batch.
-- A unique job whose key a job in one of its own unique states already holds (unique_slot = 0) is not inserted: the
-- holder is returned instead, unchanged, flagged as skipped. Nothing fails, so a caller's transaction is never doomed.
-- With the key locked, nobody else can insert it meanwhile; the unique index on (unique_key, unique_slot) stays the final
-- guarantee. READCOMMITTEDLOCK reads the latest committed holder even in a caller's SNAPSHOT transaction, and FORCESEEK
-- keeps the reads on the unique index, so they never wait on unrelated rows.
-- Ids follow the ORDER BY, so inserted jobs keep their input order. Returns the job columns, then whether the row was
-- skipped, then its unique key, to match rows to the input: the inserted rows, then the holders.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @holders TABLE (unique_key varbinary(32) PRIMARY KEY, id bigint NOT NULL);

INSERT INTO @holders (unique_key, id)
SELECT j.unique_key, j.id
FROM OPENJSON(@jobs) WITH (uk char(64) '$.uk') AS i
JOIN {{schema}}{{prefix}}job AS j WITH (READCOMMITTEDLOCK, FORCESEEK ({{prefix}}job_unique_idx (unique_key, unique_slot)))
    ON j.unique_key = CONVERT(varbinary(32), i.uk, 2)
   AND j.unique_slot = 0
WHERE i.uk IS NOT NULL
  AND j.unique_key IS NOT NULL; -- the unique index's filter, which FORCESEEK needs stated

INSERT INTO {{schema}}{{prefix}}job (
    args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states
)
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at,
    CAST(0 AS bit), inserted.unique_key
SELECT
    i.args, @now, i.kind, i.max_attempts, i.metadata, i.priority, i.queue, ISNULL(i.scheduled_at, @now), i.state, i.tags,
    CONVERT(varbinary(32), i.uk, 2), i.us
FROM OPENJSON(@jobs) WITH (
    ordinal      int           '$.o',
    args         nvarchar(max) '$.a',
    kind         nvarchar(max) '$.k',
    max_attempts smallint      '$.m',
    metadata     nvarchar(max) '$.d',
    priority     smallint      '$.p',
    queue        nvarchar(max) '$.q',
    scheduled_at datetime2(7)  '$.s',
    state        varchar(16)   '$.st',
    tags         nvarchar(max) '$.t',
    uk           char(64)      '$.uk',
    us           tinyint       '$.us'
) AS i
WHERE i.uk IS NULL
   OR NOT EXISTS (SELECT 1 FROM @holders AS h WHERE h.unique_key = CONVERT(varbinary(32), i.uk, 2))
ORDER BY i.ordinal;

SELECT
    j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at,
    CAST(1 AS bit), j.unique_key
FROM @holders AS h
JOIN {{schema}}{{prefix}}job AS j WITH (READCOMMITTEDLOCK) ON j.id = h.id;
