DROP INDEX {{schema}}{{prefix}}job_unique_idx;

-- Fails while two rows share (kind, unique_key), as the River version does.
CREATE UNIQUE INDEX {{prefix}}job_kind_unique_key_idx ON {{schema}}{{prefix}}job USING btree (kind, unique_key)
    WHERE unique_key IS NOT NULL;

DROP FUNCTION {{schema}}{{prefix}}job_state_in_bitmask(bit(8), {{schema}}{{prefix}}job_state);
ALTER TABLE {{schema}}{{prefix}}job DROP COLUMN unique_states;
