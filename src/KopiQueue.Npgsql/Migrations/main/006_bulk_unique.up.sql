ALTER TABLE {{schema}}{{prefix}}job ADD COLUMN unique_states bit(8);

-- True when the bit for state is set. Bits from left to right: scheduled, running, retryable, pending,
-- discarded, completed, cancelled, available. A NULL bitmask gives NULL; a NULL state gives false.
CREATE FUNCTION {{schema}}{{prefix}}job_state_in_bitmask(bitmask bit(8), state {{schema}}{{prefix}}job_state)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE state
        WHEN 'scheduled' THEN get_bit(bitmask, 0)
        WHEN 'running' THEN get_bit(bitmask, 1)
        WHEN 'retryable' THEN get_bit(bitmask, 2)
        WHEN 'pending' THEN get_bit(bitmask, 3)
        WHEN 'discarded' THEN get_bit(bitmask, 4)
        WHEN 'completed' THEN get_bit(bitmask, 5)
        WHEN 'cancelled' THEN get_bit(bitmask, 6)
        WHEN 'available' THEN get_bit(bitmask, 7)
        ELSE 0
    END = 1;
$$;

-- Existing rows keep unique_states NULL, which leaves them outside the new index.
CREATE UNIQUE INDEX {{prefix}}job_unique_idx ON {{schema}}{{prefix}}job USING btree (unique_key)
    WHERE unique_key IS NOT NULL AND unique_states IS NOT NULL
        AND {{schema}}{{prefix}}job_state_in_bitmask(unique_states, state);

DROP INDEX {{schema}}{{prefix}}job_kind_unique_key_idx;
