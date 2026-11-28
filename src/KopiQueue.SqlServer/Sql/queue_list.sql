-- Lists queues in name order (FR-ADM-04), a page at a time: @after only names after this one (null: from the start),
-- @names only these names (a JSON array; null: any), @max the most rows. Clients also poll their queues' pause state
-- with it (FR-QUE-04), which on SQL Server is how every client learns of a pause or resume.
SELECT TOP (@max) q.name, q.created_at, q.updated_at, q.paused_at, q.metadata
FROM {{schema}}{{prefix}}queue AS q
WHERE (@after IS NULL OR q.name > @after)
  AND (@names IS NULL OR q.name IN (SELECT n.name COLLATE Latin1_General_100_BIN2 FROM OPENJSON(@names) WITH (name nvarchar(128) '$') AS n))
ORDER BY q.name;
