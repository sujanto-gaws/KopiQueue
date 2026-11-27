-- The rescuer's candidates (design §5.7, FR-MNT-03): up to $2 running jobs whose attempt started more than $1 before
-- the database's now, oldest attempt first. Nothing is locked: the rescue that follows checks each job again. The
-- database's now comes back as the last column, also on the single row of nulls returned when nothing is stuck.
SELECT
    j.id, j.kind, j.queue, j.state::text, j.priority, j.args::text, j.metadata::text, j.tags::text[], j.attempt, j.max_attempts,
    j.attempted_at, j.attempted_by, j.errors::text[], j.created_at, j.scheduled_at, j.finalized_at,
    c.now
FROM (SELECT now() AS now) AS c
LEFT JOIN LATERAL (
    SELECT *
    FROM {{schema}}{{prefix}}job
    WHERE state = 'running'
      AND attempted_at < c.now - $1
    ORDER BY attempted_at, id
    LIMIT $2
) AS j ON true
ORDER BY j.attempted_at, j.id;
