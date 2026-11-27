-- Pauses a queue, or every queue for '*' (FR-QUE-03, FR-ADM-04), as River does (observed black box, task 3.6): a queue
-- not yet paused gets paused_at and updated_at set to now, one already paused keeps both, and
-- {"action": "pause", "queue": ...} goes out on <schema>.river_control even when nothing changed, so every client
-- applies it (FR-QUE-04). A channel over 63 bytes is skipped, as for inserts. $1 schema of the channel (null: the
-- current schema), $2 queue or '*'. Returns a row if the queue exists ('*' always), none otherwise.
WITH paused AS (
    UPDATE {{schema}}{{prefix}}queue
    SET paused_at = now(), updated_at = now()
    WHERE paused_at IS NULL AND ($2::text = '*' OR name = $2::text)
)
SELECT CASE WHEN octet_length(c.channel) <= 63
            THEN pg_notify(c.channel, json_build_object('action', 'pause', 'queue', $2::text)::text) END
FROM (SELECT coalesce($1::text, current_schema()) || '.river_control' AS channel) c
WHERE $2::text = '*' OR EXISTS (SELECT 1 FROM {{schema}}{{prefix}}queue WHERE name = $2::text);
