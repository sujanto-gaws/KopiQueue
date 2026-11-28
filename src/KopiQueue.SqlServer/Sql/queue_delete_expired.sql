-- Deletes queues whose row was not updated within the retention period (design §5.7, FR-MNT-05), at most @max of them,
-- in name order, skipping rows another transaction holds. Clients refresh the rows of the queues they work, so only
-- queues nobody has worked for the whole period go. The retention is @retention_s seconds plus @retention_us
-- microseconds. The queue table is small and has no index on updated_at, so this reads it whole.
-- Returns the deleted queues' names.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

DECLARE @cutoff datetime2(6) = DATEADD(microsecond, -@retention_us, DATEADD(second, -@retention_s, CAST(SYSUTCDATETIME() AS datetime2(6))));

WITH expired AS (
    SELECT TOP (@max) name
    FROM {{schema}}{{prefix}}queue WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE updated_at < @cutoff
    ORDER BY name
)
DELETE FROM expired
OUTPUT deleted.name;
