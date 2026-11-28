-- Reads one queue (FR-ADM-04). @name. The columns are the queue column list (SqlServerExecutor.ReadQueue).
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT name, created_at, updated_at, paused_at, metadata
FROM {{schema}}{{prefix}}queue
WHERE name = @name;
