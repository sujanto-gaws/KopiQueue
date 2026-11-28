-- Records the outcomes of finished attempts, up to 1,000 in one statement (design §5.3, §8.1, FR-EXE-06), with the
-- semantics of the PostgreSQL driver's statement. @batch is a JSON array with one object per job: i id, at attempt,
-- e error (River's error object, as a string), f finalized_at, d metadata to merge (a JSON object, as a string),
-- s scheduled_at, st state. An absent field leaves its column as it is.
-- Only a job that is still running changes, so a job that was rescued, cancelled or deleted meanwhile is not
-- clobbered; metadata is merged either way. A job whose cancellation was asked for while it ran (a top-level
-- cancel_attempted_at key in its metadata) and that would run again is cancelled instead, finalised now (design §5.8).
-- Metadata is merged at the top level, as jsonb || does: the new keys, then the old keys the update does not replace.
-- OPENJSON's key column has a binary collation, so keys compare case-sensitively, as in PostgreSQL.
-- Returns the changed rows (OUTPUT), then the given jobs that exist but did not change; the driver sorts them by id.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @changed TABLE (id bigint PRIMARY KEY);

UPDATE j
SET attempt = CASE WHEN j.state = 'running' AND c.cancel = 0 AND u.attempt IS NOT NULL THEN u.attempt ELSE j.attempt END,
    errors = CASE
        WHEN j.state = 'running' AND u.error IS NOT NULL THEN JSON_MODIFY(ISNULL(j.errors, N'[]'), 'append $', JSON_QUERY(u.error))
        ELSE j.errors
    END,
    finalized_at = CASE
        WHEN j.state = 'running' AND c.cancel = 1 THEN SYSUTCDATETIME()
        WHEN j.state = 'running' AND u.finalized_at IS NOT NULL THEN u.finalized_at
        ELSE j.finalized_at
    END,
    metadata = CASE
        WHEN u.metadata IS NULL THEN j.metadata
        ELSE N'{' + ISNULL((
            SELECT STRING_AGG(CAST(N'"' + STRING_ESCAPE(m.[key], 'json') + N'":' + m.v AS nvarchar(max)), N',')
            FROM (
                SELECT n.[key], CASE n.type WHEN 0 THEN N'null' WHEN 1 THEN N'"' + STRING_ESCAPE(n.[value], 'json') + N'"' ELSE n.[value] END AS v
                FROM OPENJSON(u.metadata) AS n
                UNION ALL
                SELECT o.[key], CASE o.type WHEN 0 THEN N'null' WHEN 1 THEN N'"' + STRING_ESCAPE(o.[value], 'json') + N'"' ELSE o.[value] END
                FROM OPENJSON(j.metadata) AS o
                WHERE NOT EXISTS (SELECT 1 FROM OPENJSON(u.metadata) AS r WHERE r.[key] = o.[key])
            ) AS m
        ), N'') + N'}'
    END,
    scheduled_at = CASE WHEN j.state = 'running' AND c.cancel = 0 AND u.scheduled_at IS NOT NULL THEN u.scheduled_at ELSE j.scheduled_at END,
    state = CASE
        WHEN j.state = 'running' AND c.cancel = 1 THEN 'cancelled'
        WHEN j.state = 'running' THEN u.state
        ELSE j.state
    END
OUTPUT inserted.id INTO @changed (id)
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at
FROM {{schema}}{{prefix}}job AS j
JOIN OPENJSON(@batch) WITH (
    id           bigint        '$.i',
    attempt      smallint      '$.at',
    error        nvarchar(max) '$.e',
    finalized_at datetime2(7)  '$.f',
    metadata     nvarchar(max) '$.d',
    scheduled_at datetime2(7)  '$.s',
    state        varchar(16)   '$.st'
) AS u ON u.id = j.id
CROSS APPLY (
    SELECT CASE
        WHEN j.state = 'running'
         AND u.state IN ('available', 'retryable', 'scheduled')
         AND EXISTS (SELECT 1 FROM OPENJSON(j.metadata) AS k WHERE k.[key] = N'cancel_attempted_at')
        THEN 1 ELSE 0
    END AS cancel
) AS c
WHERE j.state = 'running' OR u.metadata IS NOT NULL;

SELECT
    j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at
FROM {{schema}}{{prefix}}job AS j
WHERE j.id IN (SELECT b.id FROM OPENJSON(@batch) WITH (id bigint '$.i') AS b)
  AND j.id NOT IN (SELECT c.id FROM @changed AS c);
