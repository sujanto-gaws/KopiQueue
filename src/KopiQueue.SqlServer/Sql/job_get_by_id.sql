-- Reads one job (FR-ADM-01). @id. The columns are the job column list every job statement returns (SqlServerExecutor.ReadJob).
SELECT
    id, kind, queue, state, priority, args, metadata, tags, attempt, max_attempts,
    attempted_at, attempted_by, errors, created_at, scheduled_at, finalized_at
FROM {{schema}}{{prefix}}job
WHERE id = @id;
