-- Inserts up to 1,000 jobs in one statement (design §8.1, FR-INS-04). @jobs is a JSON array with one object per job:
-- o ordinal, a args, k kind, m max_attempts, d metadata, p priority, q queue, s scheduled_at (UTC, or absent for now),
-- st state, t tags. args, metadata and tags are JSON text sent as strings, so they are stored exactly as given.
-- Ids follow the ORDER BY, which SQL Server guarantees for INSERT ... SELECT, so a batch runs first in, first out. OUTPUT
-- has no order, and the driver sorts the rows by id. created_at and a missing scheduled_at share one clock reading.
-- Unique jobs take another path (plan task 5.4).
DECLARE @now datetime2(6) = SYSUTCDATETIME();

INSERT INTO {{schema}}{{prefix}}job (args, created_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags)
OUTPUT
    inserted.id, inserted.kind, inserted.queue, inserted.state, inserted.priority, inserted.args, inserted.metadata,
    inserted.tags, inserted.attempt, inserted.max_attempts, inserted.attempted_at, inserted.attempted_by,
    inserted.errors, inserted.created_at, inserted.scheduled_at, inserted.finalized_at
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
