-- Extends a leader's lease (design §5.5). Returns a row exactly when this client still holds it. @leader_id; the lease
-- length as @ttl_s seconds plus @ttl_us microseconds.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();

UPDATE {{schema}}{{prefix}}leader
SET expires_at = DATEADD(microsecond, @ttl_us, DATEADD(second, @ttl_s, @now))
OUTPUT inserted.leader_id
WHERE leader_id = @leader_id;
