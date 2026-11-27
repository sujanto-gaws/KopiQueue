-- Deletes queues whose row was not updated within the retention period (design §5.7, FR-MNT-05), at most $2 of them,
-- skipping rows another transaction holds. Clients refresh the rows of the queues they work, so only queues nobody has
-- worked for the whole period go. $1 retention, $2 batch size. Returns the deleted queues' names.
DELETE FROM {{schema}}{{prefix}}queue
WHERE name IN (
    SELECT name
    FROM {{schema}}{{prefix}}queue
    WHERE updated_at < now() - $1
    ORDER BY name
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING name;
