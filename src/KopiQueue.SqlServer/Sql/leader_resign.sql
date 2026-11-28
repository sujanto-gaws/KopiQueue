-- Gives up a leader's lease (design §5.5). Returns a row exactly when this client held it. @leader_id.
-- SQL Server has no LISTEN/NOTIFY, so nothing is announced: other clients find the free lease at their next election
-- attempt, as poll-only clients do on PostgreSQL.
DELETE FROM {{schema}}{{prefix}}leader
OUTPUT deleted.leader_id
WHERE leader_id = @leader_id;
