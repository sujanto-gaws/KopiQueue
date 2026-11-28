-- Replaces a queue's metadata (FR-ADM-04): updated_at becomes now. @name, @metadata (a JSON object). Returns the
-- updated row, none if the queue does not exist. Clients see the change when they next poll their queues' state.
UPDATE {{schema}}{{prefix}}queue
SET metadata = @metadata, updated_at = CAST(SYSUTCDATETIME() AS datetime2(6))
OUTPUT inserted.name, inserted.created_at, inserted.updated_at, inserted.paused_at, inserted.metadata
WHERE name = @name;
