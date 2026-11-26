-- Allowed in a transaction on PostgreSQL 12+, as long as the new value is not used in the same transaction.
ALTER TYPE {{schema}}{{prefix}}job_state ADD VALUE IF NOT EXISTS 'pending' AFTER 'discarded';

UPDATE {{schema}}{{prefix}}job SET args = '{}' WHERE args IS NULL;
UPDATE {{schema}}{{prefix}}job SET metadata = '{}' WHERE metadata IS NULL;

ALTER TABLE {{schema}}{{prefix}}job
    ALTER COLUMN args SET NOT NULL,
    ALTER COLUMN metadata SET DEFAULT '{}',
    ALTER COLUMN metadata SET NOT NULL,
    DROP CONSTRAINT finalized_or_finalized_at_null,
    ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
        (finalized_at IS NULL AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
        (finalized_at IS NOT NULL AND state IN ('cancelled', 'completed', 'discarded')));

DROP TRIGGER {{prefix}}notify ON {{schema}}{{prefix}}job;
DROP FUNCTION {{schema}}{{prefix}}job_notify();

CREATE TABLE {{schema}}{{prefix}}queue (
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}',
    paused_at timestamptz,
    updated_at timestamptz NOT NULL,
    CONSTRAINT {{prefix}}queue_pkey PRIMARY KEY (name)
);

-- Fails while a leader row has another name, as the River version does.
ALTER TABLE {{schema}}{{prefix}}leader
    ALTER COLUMN name SET DEFAULT 'default',
    DROP CONSTRAINT name_length,
    ADD CONSTRAINT name_length CHECK (name = 'default');
