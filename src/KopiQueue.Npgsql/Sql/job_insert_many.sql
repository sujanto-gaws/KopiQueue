-- Inserts a batch of jobs in one statement. Each parameter is an array with one element per job; tags are
-- comma-joined because unnest cannot pass one array per row (tags never contain commas).
-- Ids are drawn from the table's sequence in input order, so the ids of inserted jobs ascend with the input, and a
-- batch runs first in, first out. Rows are then inserted in unique-key order, keyless ones last: concurrent batches that
-- share keys take their key slots in one order, so none can hold a key another waits for while waiting for one that
-- other holds, which PostgreSQL would end as a deadlock. A duplicate's drawn id is left unused, as ON CONFLICT does.
-- nextval is volatile, so it is evaluated after the ORDER BY of its own query, and the CTE is not inlined.
--
-- Unique jobs (design §5.6, FR-UNQ-02, FR-UNQ-03): a job whose unique key a job in one of its unique states already
-- holds conflicts on the partial unique index. It is not inserted: the no-op update returns the existing row instead,
-- and xmax, which an update sets, flags it as skipped. The update leaves every column as it is, the kind too, since
-- ExcludeKind lets jobs of different kinds share a key. The caller passes each key at most once: a second row with the
-- same key would make the statement fail, because ON CONFLICT DO UPDATE cannot touch one row twice.
-- Returns the job columns, then whether the row was skipped, then its unique key, to match rows to the input.
WITH numbered AS (
    SELECT
        nextval(pg_get_serial_sequence('{{schema}}{{prefix}}job', 'id')) AS id,
        j.*
    FROM unnest(
        $1::jsonb[], $2::text[], $3::smallint[], $4::jsonb[], $5::smallint[], $6::text[],
        $7::timestamptz[], $8::text[], $9::text[], $10::bytea[], $11::integer[]
    ) WITH ORDINALITY AS j (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states, ordinal)
    ORDER BY j.ordinal
)
INSERT INTO {{schema}}{{prefix}}job AS existing (
    id, args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags, unique_key, unique_states
)
SELECT
    j.id,
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
FROM numbered AS j
ORDER BY j.unique_key NULLS LAST, j.ordinal
ON CONFLICT (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND {{schema}}{{prefix}}job_state_in_bitmask(unique_states, state)
DO UPDATE SET kind = existing.kind
RETURNING
    id, kind, queue, state::text, priority, args::text, metadata::text, tags::text[], attempt, max_attempts,
    attempted_at, attempted_by, errors::text[], created_at, scheduled_at, finalized_at,
    NOT (xmax = 0) AS unique_skipped_as_duplicate, unique_key;
