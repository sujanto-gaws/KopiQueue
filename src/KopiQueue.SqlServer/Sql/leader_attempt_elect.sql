-- Wins the leader lease (design §5.5, §8.1, FR-MNT-01), with the semantics of the PostgreSQL driver's statement: takes
-- the single row over when its lease has expired or this client already holds it, which extends the lease and keeps
-- its election time, and inserts it when there is none. Returns a row exactly when this client holds the lease after.
-- @leader_id; the lease length as @ttl_s seconds plus @ttl_us microseconds.
-- The insert's UPDLOCK, HOLDLOCK range lock makes two clients that both found no row take turns, so one inserts and
-- the other sees its row. The times are read once, at the columns' precision (design §8.1).
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @now datetime2(6) = SYSUTCDATETIME();
DECLARE @expires_at datetime2(6) = DATEADD(microsecond, @ttl_us, DATEADD(second, @ttl_s, @now));
DECLARE @won int;

UPDATE {{schema}}{{prefix}}leader
SET elected_at = CASE WHEN leader_id = @leader_id THEN elected_at ELSE @now END,
    leader_id = @leader_id,
    expires_at = @expires_at
WHERE name = 'default' AND (expires_at < @now OR leader_id = @leader_id);
SET @won = @@ROWCOUNT;

IF @won = 0
BEGIN
    INSERT INTO {{schema}}{{prefix}}leader (name, leader_id, elected_at, expires_at)
    SELECT 'default', @leader_id, @now, @expires_at
    WHERE NOT EXISTS (SELECT 1 FROM {{schema}}{{prefix}}leader WITH (UPDLOCK, HOLDLOCK) WHERE name = 'default');
    SET @won = @@ROWCOUNT;
END;

SELECT 1 WHERE @won > 0;
