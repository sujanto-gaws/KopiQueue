-- One statement of a delete for the admin API (FR-ADM-03). Filters as for job_list.sql, each null for any. @max the most
-- jobs, at most 1,000; the driver runs larger deletes as several statements in one transaction.
--
-- Deletes up to @max matching jobs in id order, never a running one. The jobs are found first, without locks, skipping
-- rows another transaction holds, such as a job a producer is fetching; the delete then checks the state again as it
-- holds each row. Returns the deleted jobs; the driver sorts them by id.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @matched TABLE (id bigint PRIMARY KEY);

INSERT INTO @matched (id)
SELECT TOP (@max) j.id
FROM {{schema}}{{prefix}}job AS j WITH (READPAST)
WHERE j.state <> 'running'
  AND (@ids IS NULL OR j.id IN (SELECT f.id FROM OPENJSON(@ids) WITH (id bigint '$') AS f))
  AND (@kinds IS NULL OR j.kind IN (SELECT f.kind COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kinds) WITH (kind nvarchar(128) '$') AS f))
  AND (@queues IS NULL OR j.queue IN (SELECT f.queue COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@queues) WITH (queue nvarchar(128) '$') AS f))
  AND (@states IS NULL OR j.state IN (SELECT f.state COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@states) WITH (state varchar(16) '$') AS f))
  AND (@priorities IS NULL OR j.priority IN (SELECT f.priority FROM OPENJSON(@priorities) WITH (priority smallint '$') AS f))
  AND @@metadata@@
ORDER BY j.id
OPTION (RECOMPILE);

DELETE j
OUTPUT
    deleted.id, deleted.kind, deleted.queue, deleted.state, deleted.priority, deleted.args, deleted.metadata,
    deleted.tags, deleted.attempt, deleted.max_attempts, deleted.attempted_at, deleted.attempted_by,
    deleted.errors, deleted.created_at, deleted.scheduled_at, deleted.finalized_at
FROM {{schema}}{{prefix}}job AS j WITH (READPAST, ROWLOCK)
JOIN @matched AS m ON m.id = j.id
WHERE j.state <> 'running';
