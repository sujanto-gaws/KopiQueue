-- Extends a leader's lease (design §5.5). A row comes back exactly when this client still holds it. $1 leader ID, $2
-- lease length.
UPDATE {{schema}}{{prefix}}leader
SET expires_at = now() + $2
WHERE leader_id = $1
RETURNING leader_id;
