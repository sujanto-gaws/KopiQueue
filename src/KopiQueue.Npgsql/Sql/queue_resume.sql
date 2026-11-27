-- Resumes a queue, or every queue for '*' (FR-QUE-03, FR-ADM-04): the mirror of queue_pause.sql. A paused queue gets
-- paused_at cleared and updated_at set to now, and {"action": "resume", "queue": ...} goes out on
-- <schema>.river_control even when nothing changed. $1 schema of the channel (null: the current schema), $2 queue or
-- '*'. Returns a row if the queue exists ('*' always), none otherwise.
WITH resumed AS (
    UPDATE {{schema}}{{prefix}}queue
    SET paused_at = NULL, updated_at = now()
    WHERE paused_at IS NOT NULL AND ($2::text = '*' OR name = $2::text)
)
SELECT CASE WHEN octet_length(c.channel) <= 63
            THEN pg_notify(c.channel, json_build_object('action', 'resume', 'queue', $2::text)::text) END
FROM (SELECT coalesce($1::text, current_schema()) || '.river_control' AS channel) c
WHERE $2::text = '*' OR EXISTS (SELECT 1 FROM {{schema}}{{prefix}}queue WHERE name = $2::text);
