-- Inserts a batch of jobs in one statement. Each parameter is an array with one element per job; tags are
-- comma-joined because unnest cannot pass one array per row (tags never contain commas).
-- Rows are inserted in input order, so ids ascend with the input.
INSERT INTO {{schema}}{{prefix}}job (
    args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states
)
SELECT
    j.args,
    j.kind,
    j.max_attempts,
    j.metadata,
    j.priority,
    j.queue,
    coalesce(j.scheduled_at, now()),
    j.state::{{schema}}{{prefix}}job_state,
    coalesce(string_to_array(j.tags, ','), '{}'),
    j.unique_key,
    j.unique_states::bit(8)
FROM unnest(
    $1::jsonb[], $2::text[], $3::smallint[], $4::jsonb[], $5::smallint[], $6::text[],
    $7::timestamptz[], $8::text[], $9::text[], $10::bytea[], $11::integer[]
) WITH ORDINALITY AS j (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states, ordinal)
ORDER BY j.ordinal
RETURNING
    id, kind, queue, state::text, priority, args::text, metadata::text, tags::text[], attempt, max_attempts,
    attempted_at, attempted_by, errors::text[], created_at, scheduled_at, finalized_at;
