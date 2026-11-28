-- The fast insert path (design §8.1, FR-INS-04, ADR-014): job_insert_many.sql without OUTPUT, so no rows travel back,
-- only their count. The @jobs array, the ordering and the clock are the same. Unique jobs are rejected by the driver
-- before this runs, as on PostgreSQL. SqlBulkCopy is not used: its page locks would stall every producer until the
-- caller commits (spike 0.10).
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

INSERT INTO {{schema}}{{prefix}}job (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags)
SELECT i.args, @now, i.kind, i.max_attempts, i.metadata, i.priority, i.queue, ISNULL(i.scheduled_at, @now), i.state, i.tags
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
    tags         nvarchar(max) '$.t'
) AS i
ORDER BY i.ordinal;

SELECT CAST(@@ROWCOUNT AS bigint);
