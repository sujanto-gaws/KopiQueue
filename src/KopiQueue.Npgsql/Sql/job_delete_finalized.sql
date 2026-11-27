-- The job cleaner's batch (design §5.7, FR-MNT-04), in one statement. $1, $2, $3 the retention of completed, cancelled
-- and discarded jobs (null: keep that state's jobs), $4 batch size.
--
-- Deletes up to $4 finalised jobs whose finalized_at is older than their state's retention by the database's clock,
-- oldest first; the (state, finalized_at) index serves each state's lookup. A null retention makes its comparison null,
-- so that state is kept. Rows another transaction holds are skipped, never waited for. The delete checks the state and
-- cutoff again, so a job that left its finalised state since it was locked (JobRetry, FR-ERR-07) is not deleted.
-- Returns the deleted jobs' ids.
WITH expired AS (
    SELECT id
    FROM {{schema}}{{prefix}}job
    WHERE finalized_at IS NOT NULL
      AND (
             (state = 'completed' AND finalized_at < now() - $1::interval)
          OR (state = 'cancelled' AND finalized_at < now() - $2::interval)
          OR (state = 'discarded' AND finalized_at < now() - $3::interval)
      )
    ORDER BY finalized_at, id
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
DELETE FROM {{schema}}{{prefix}}job AS j
USING expired AS e
WHERE j.id = e.id
  AND (
         (j.state = 'completed' AND j.finalized_at < now() - $1::interval)
      OR (j.state = 'cancelled' AND j.finalized_at < now() - $2::interval)
      OR (j.state = 'discarded' AND j.finalized_at < now() - $3::interval)
  )
RETURNING j.id;
