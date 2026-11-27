-- Deletes jobs for the admin API (FR-ADM-03), in one statement. Filters as for job_list.sql, each null for any: $1 ids,
-- $2 kinds, $3 queues, $4 states, $5 priorities, $6 metadata that the job's contains. $7 the most jobs.
--
-- Deletes up to $7 matching jobs in id order, never a running one. Rows another transaction holds, such as a job a
-- producer is fetching, are skipped rather than waited for, and the delete checks the state again once it holds the row.
-- Returns the deleted jobs, in ascending id order.
WITH matched AS (
    SELECT id
    FROM {{schema}}{{prefix}}job AS j
    WHERE j.state <> 'running'
      AND ($1::bigint[] IS NULL OR j.id = ANY ($1::bigint[]))
      AND ($2::text[] IS NULL OR j.kind = ANY ($2::text[]))
      AND ($3::text[] IS NULL OR j.queue = ANY ($3::text[]))
      AND ($4::text[] IS NULL OR j.state = ANY ($4::text[]::{{schema}}{{prefix}}job_state[]))
      AND ($5::smallint[] IS NULL OR j.priority = ANY ($5::smallint[]))
      AND ($6::jsonb IS NULL OR j.metadata @> $6::jsonb)
    ORDER BY j.id
    LIMIT $7
    FOR UPDATE SKIP LOCKED
),
deleted AS (
    DELETE FROM {{schema}}{{prefix}}job AS j
    USING matched AS m
    WHERE j.id = m.id AND j.state <> 'running'
    RETURNING j.*
)
SELECT
    d.id, d.kind, d.queue, d.state::text, d.priority, d.args::text, d.metadata::text, d.tags::text[], d.attempt, d.max_attempts,
    d.attempted_at, d.attempted_by, d.errors::text[], d.created_at, d.scheduled_at, d.finalized_at
FROM deleted AS d
ORDER BY d.id;
