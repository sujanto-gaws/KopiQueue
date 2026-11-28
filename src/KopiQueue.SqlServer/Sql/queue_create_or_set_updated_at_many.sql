-- Creates the queues a client works, or marks them worked now (FR-QUE-02, design §5.7): clients run it when they start
-- and then every minute, so the queue cleaner keeps their queues. @names is a JSON array of distinct queue names.
-- Existing rows are updated and new rows inserted in name order (a nested loop over the sorted names), so clients
-- refreshing the same queues lock them in the same order. Two clients creating the same queue take turns on the
-- insert's UPDLOCK, HOLDLOCK range lock, so the second finds the first's row instead of failing.
-- Returns the rows with their pause state, in name order.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @names_table TABLE (name nvarchar(128) COLLATE Latin1_General_100_BIN2 PRIMARY KEY);

INSERT INTO @names_table (name)
SELECT n.name FROM OPENJSON(@names) WITH (name nvarchar(128) '$') AS n;

UPDATE q
SET updated_at = @now
FROM @names_table AS n
INNER LOOP JOIN {{schema}}{{prefix}}queue AS q ON q.name = n.name;

INSERT INTO {{schema}}{{prefix}}queue (name, created_at, updated_at)
SELECT n.name, @now, @now
FROM @names_table AS n
WHERE NOT EXISTS (SELECT 1 FROM {{schema}}{{prefix}}queue AS q WITH (UPDLOCK, HOLDLOCK) WHERE q.name = n.name)
ORDER BY n.name;

SELECT q.name, q.created_at, q.updated_at, q.paused_at, q.metadata
FROM @names_table AS n
JOIN {{schema}}{{prefix}}queue AS q ON q.name = n.name
ORDER BY q.name;
