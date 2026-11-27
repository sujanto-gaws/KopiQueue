-- Lists jobs for the admin API (FR-ADM-02), a keyset page at a time. Takes no locks. Filters, each null for any: $1 ids,
-- $2 kinds, $3 queues, $4 states, $5 priorities, $6 metadata that the job's contains (@>, served by the GIN index on
-- metadata). $7 and $8 the position to start after: the previous page's last id and its sort value (null for none,
-- which sorts as infinity), $7 null to start from the first. $9 the most rows.
--
-- The driver fills in @@after@@ and @@order@@ for one of its sorts: by id alone (the primary key), or by a time and
-- then id, with a null time as 'infinity', so it sorts last ascending and first descending. Sorted by id, $8 is unused.
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at
FROM {{schema}}{{prefix}}job AS j
WHERE ($1::bigint[] IS NULL OR j.id = ANY ($1::bigint[]))
  AND ($2::text[] IS NULL OR j.kind = ANY ($2::text[]))
  AND ($3::text[] IS NULL OR j.queue = ANY ($3::text[]))
  AND ($4::text[] IS NULL OR j.state = ANY ($4::text[]::{{schema}}{{prefix}}job_state[]))
  AND ($5::smallint[] IS NULL OR j.priority = ANY ($5::smallint[]))
  AND ($6::jsonb IS NULL OR j.metadata @> $6::jsonb)
  AND ($7::bigint IS NULL OR @@after@@)
ORDER BY @@order@@
LIMIT $9;
