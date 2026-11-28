-- Resumes a queue, or every queue for '*' (FR-QUE-03, FR-ADM-04): the mirror of queue_pause.sql. A paused queue gets
-- paused_at cleared and updated_at set to now. @name, the queue or '*'. Returns a row if the queue exists ('*'
-- always), none otherwise.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE {{schema}}{{prefix}}queue
SET paused_at = NULL, updated_at = @now
WHERE paused_at IS NOT NULL AND (@name = N'*' OR name = @name);

SELECT 1 WHERE @name = N'*' OR EXISTS (SELECT 1 FROM {{schema}}{{prefix}}queue WHERE name = @name);
