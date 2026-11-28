-- Lists jobs for the admin API (FR-ADM-02), a keyset page at a time, without locking. Filters, each a JSON array or null
-- for any: @ids, @kinds, @queues, @states, @priorities; the metadata filter (@@metadata@@) is compiled by the driver
-- from the JSON object, matched by containment as PostgreSQL's @> does (SqlServerJsonContainment). @after_id and
-- @after_value the position to start after: the previous page's last id and its sort value (null for none, which
-- sorts as the latest time), @after_id null to start from the first. @max the most rows.
--
-- The driver fills in @@after@@ and @@order@@ for one of its sorts: by id alone (the primary key), or by a time and
-- then id, with a null time as the latest possible time, so it sorts last ascending and first descending.
-- Under locking READ COMMITTED a row a writer holds is waited for, as any read on SQL Server; with RCSI it is not.
-- OPTION (RECOMPILE) lets each call's plan drop the filters it does not use.
SELECT TOP (@max)
    j.id, j.kind, j.queue, j.state, j.priority, j.args, j.metadata, j.tags, j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors, j.created_at, j.scheduled_at, j.finalized_at
FROM {{schema}}{{prefix}}job AS j
WHERE (@ids IS NULL OR j.id IN (SELECT f.id FROM OPENJSON(@ids) WITH (id bigint '$') AS f))
  AND (@kinds IS NULL OR j.kind IN (SELECT f.kind COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@kinds) WITH (kind nvarchar(128) '$') AS f))
  AND (@queues IS NULL OR j.queue IN (SELECT f.queue COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@queues) WITH (queue nvarchar(128) '$') AS f))
  AND (@states IS NULL OR j.state IN (SELECT f.state COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@states) WITH (state varchar(16) '$') AS f))
  AND (@priorities IS NULL OR j.priority IN (SELECT f.priority FROM OPENJSON(@priorities) WITH (priority smallint '$') AS f))
  AND @@metadata@@
  AND (@after_id IS NULL OR @@after@@)
ORDER BY @@order@@
OPTION (RECOMPILE);
