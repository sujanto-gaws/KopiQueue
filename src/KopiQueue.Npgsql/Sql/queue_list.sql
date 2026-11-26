-- Lists queues in name order (FR-ADM-04), a page at a time: $1 only names after this one (null: from the start),
-- $2 only these names (null: any), $3 the most rows. Clients also poll their queues' pause state with it (FR-QUE-04).
SELECT name, created_at, updated_at, paused_at, metadata
FROM {{schema}}{{prefix}}queue
WHERE ($1::text IS NULL OR name > $1::text)
  AND ($2::text[] IS NULL OR name = ANY ($2::text[]))
ORDER BY name
LIMIT $3;
