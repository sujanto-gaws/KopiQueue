SELECT
    id, kind, queue, state::text, priority, args::text, metadata::text, tags::text[], attempt, max_attempts,
    attempted_at, attempted_by, errors::text[], created_at, scheduled_at, finalized_at
FROM {{schema}}{{prefix}}job
WHERE id = $1;
