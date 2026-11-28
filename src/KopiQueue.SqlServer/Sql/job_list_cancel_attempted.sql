-- For the poll-only path of external cancel (design §5.8, FR-DB-05): which of the given jobs (@ids, a JSON array of
-- ids) are still running and were asked to cancel (metadata.cancel_attempted_at, §4.1). Returns their ids in ascending
-- order. Takes no locks: READPAST passes over a job its worker is completing just now, which needs no cancel, rather
-- than wait for it; a job only a cancel holds is read again at the next poll.
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

SELECT j.id
FROM {{schema}}{{prefix}}job AS j WITH (READPAST)
WHERE j.id IN (SELECT f.id FROM OPENJSON(@ids) WITH (id bigint '$') AS f)
    AND j.state = 'running'
    -- The key's presence, as PostgreSQL's ? operator tests it: JSON_VALUE would miss a key whose value is null.
    AND EXISTS (SELECT 1 FROM OPENJSON(j.metadata) AS m WHERE m.[key] COLLATE Latin1_General_BIN2 = N'cancel_attempted_at')
ORDER BY j.id;
