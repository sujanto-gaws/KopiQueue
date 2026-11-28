-- Pauses a queue, or every queue for '*' (FR-QUE-03, FR-ADM-04), as the PostgreSQL driver does: a queue not yet paused
-- gets paused_at and updated_at set to now, one already paused keeps both. @name, the queue or '*'.
-- Nothing is announced, since SQL Server has no LISTEN/NOTIFY: clients see the pause when they next poll their queues'
-- state (FR-QUE-04). Returns a row if the queue exists ('*' always), none otherwise.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE {{schema}}{{prefix}}queue
SET paused_at = @now, updated_at = @now
WHERE paused_at IS NULL AND (@name = N'*' OR name = @name);

SELECT 1 WHERE @name = N'*' OR EXISTS (SELECT 1 FROM {{schema}}{{prefix}}queue WHERE name = @name);
