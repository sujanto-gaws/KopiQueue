-- Takes the unique-key lock of every distinct key in @jobs (the "uk" field: a key as 64 hex digits), in key order
-- (design §8.1, ADR-011, spike 0.10). Each lock is an exclusive sp_getapplock on 'kopi_unique:<@scope>:<key>', owned by
-- the transaction, so it serialises only inserts of the same key until the inserter commits or rolls back, as
-- PostgreSQL's ON CONFLICT waits for the transaction that inserted a conflicting row. A transaction must be open.
-- Every inserter takes its keys in the same order, so batches that share keys queue behind each other instead of
-- deadlocking (plan task 4.6). Locks are re-entrant: taking a key twice in one transaction is cheap.
-- The wait has no lock timeout of its own; the command timeout bounds it, as statement_timeout does on PostgreSQL.
DECLARE @unique_keys TABLE (k char(64) COLLATE Latin1_General_100_BIN2 PRIMARY KEY);

INSERT INTO @unique_keys (k)
SELECT DISTINCT i.uk
FROM OPENJSON(@jobs) WITH (uk char(64) '$.uk') AS i
WHERE i.uk IS NOT NULL;

DECLARE @unique_key char(64) = (SELECT MIN(k) FROM @unique_keys);
DECLARE @unique_lock_resource nvarchar(255);
DECLARE @unique_lock_result int;

WHILE @unique_key IS NOT NULL
BEGIN
    SET @unique_lock_resource = N'kopi_unique:' + @scope + N':' + @unique_key;
    EXEC @unique_lock_result = sys.sp_getapplock
        @Resource = @unique_lock_resource, @LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = -1;
    IF @unique_lock_result < 0
        THROW 50001, N'KopiQueue could not take a unique-key lock: sp_getapplock failed, for example as a deadlock victim.', 1;
    SET @unique_key = (SELECT MIN(k) FROM @unique_keys WHERE k > @unique_key);
END;
