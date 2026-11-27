-- For the poll-only path of external cancel (design §5.8, FR-DB-05): which of the given jobs ($1) are still running and
-- were asked to cancel (metadata.cancel_attempted_at, §4.1). Returns their ids in ascending order. Takes no locks.
SELECT id
FROM {{schema}}{{prefix}}job
WHERE id = ANY($1::bigint[]) AND state = 'running' AND metadata ? 'cancel_attempted_at'
ORDER BY id;
