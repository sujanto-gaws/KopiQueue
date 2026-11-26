-- Reads one queue (FR-ADM-04). $1 name.
SELECT name, created_at, updated_at, paused_at, metadata
FROM {{schema}}{{prefix}}queue
WHERE name = $1;
