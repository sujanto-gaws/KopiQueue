-- Gives up a leader's lease (design §5.5) and, only if this client held it, announces that on the leadership channel in
-- the same statement, so the notification is delivered on commit. The payload is the one a River client sends when it
-- resigns (observed black-box, design §4.3). As for inserts, a channel over 63 bytes is skipped rather than failing.
-- A row comes back exactly when this client held the lease. $1 schema (null for the current one), $2 leader ID.
WITH resigned AS (
    DELETE FROM {{schema}}{{prefix}}leader
    WHERE leader_id = $2
    RETURNING leader_id
)
SELECT
    r.leader_id,
    CASE WHEN octet_length(c.channel) <= 63
        THEN pg_notify(c.channel, json_build_object('leader_id', r.leader_id, 'action', 'resigned')::text)
    END AS notified
FROM resigned AS r
CROSS JOIN (SELECT coalesce($1::text, current_schema()) || '.river_leadership' AS channel) AS c;
