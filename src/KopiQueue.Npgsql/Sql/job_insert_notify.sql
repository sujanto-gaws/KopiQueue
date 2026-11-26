-- One insert notification per queue that received available jobs (design §4.3, ADR-007). It runs in the insert's
-- transaction, so PostgreSQL delivers it only on commit. The channel is "<schema>.river_insert", as River uses, with
-- the connection's current schema when none is configured. Channels over 63 bytes, which pg_notify rejects, are
-- skipped so that a long schema name never fails an insert: such a schema cannot be listened to either.
SELECT pg_notify(c.channel, jsonb_build_object('queue', q.queue)::text)
FROM (SELECT coalesce($1::text, current_schema()) || '.river_insert' AS channel) AS c
CROSS JOIN unnest($2::text[]) AS q (queue)
WHERE octet_length(c.channel) <= 63;
