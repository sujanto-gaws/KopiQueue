-- Replaces a queue's metadata (FR-ADM-04), as River's QueueUpdate does (observed black box, task 3.6): updated_at
-- becomes now, and {"action": "metadata_changed", "metadata": ..., "queue": ...} goes out on <schema>.river_control,
-- unless the channel is over 63 bytes or the payload would pass PostgreSQL's 8,000-byte limit. $1 schema of the channel
-- (null: the current schema), $2 queue, $3 metadata. Returns the updated row, none if the queue does not exist.
WITH updated AS (
    UPDATE {{schema}}{{prefix}}queue
    SET metadata = $3, updated_at = now()
    WHERE name = $2
    RETURNING name, created_at, updated_at, paused_at, metadata
), message AS (
    SELECT u.*, c.channel, json_build_object('action', 'metadata_changed', 'metadata', u.metadata, 'queue', u.name)::text AS payload
    FROM updated u
    CROSS JOIN (SELECT coalesce($1::text, current_schema()) || '.river_control' AS channel) c
)
SELECT name, created_at, updated_at, paused_at, metadata,
       CASE WHEN octet_length(channel) <= 63 AND octet_length(payload) < 8000 THEN pg_notify(channel, payload) END
FROM message;
