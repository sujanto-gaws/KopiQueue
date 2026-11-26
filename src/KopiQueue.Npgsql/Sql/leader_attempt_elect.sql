-- Wins the leader lease (design §5.5, FR-MNT-01): inserts the single row when there is none, and takes it over when its
-- lease has expired or this client already holds it, which extends the lease and keeps its election time. A row comes
-- back exactly when this client holds the lease afterwards. $1 leader ID, $2 lease length.
INSERT INTO {{schema}}{{prefix}}leader AS l (leader_id, elected_at, expires_at)
VALUES ($1, now(), now() + $2)
ON CONFLICT (name) DO UPDATE
SET
    leader_id = EXCLUDED.leader_id,
    elected_at = CASE WHEN l.leader_id = EXCLUDED.leader_id THEN l.elected_at ELSE EXCLUDED.elected_at END,
    expires_at = EXCLUDED.expires_at
WHERE l.expires_at < now() OR l.leader_id = EXCLUDED.leader_id
RETURNING l.leader_id;
