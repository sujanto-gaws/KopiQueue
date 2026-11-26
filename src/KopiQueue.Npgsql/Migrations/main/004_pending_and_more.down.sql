-- Leaves the state River's own revert of this version leaves: the 'pending' enum value stays
-- (PostgreSQL cannot drop enum values), and metadata becomes nullable without a default.
ALTER TABLE {{schema}}{{prefix}}job
    ALTER COLUMN args DROP NOT NULL,
    ALTER COLUMN metadata DROP NOT NULL,
    ALTER COLUMN metadata DROP DEFAULT,
    DROP CONSTRAINT finalized_or_finalized_at_null,
    ADD CONSTRAINT finalized_or_finalized_at_null CHECK (
        (state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR finalized_at IS NULL);

DROP TABLE {{schema}}{{prefix}}queue;

ALTER TABLE {{schema}}{{prefix}}leader
    ALTER COLUMN name DROP DEFAULT,
    DROP CONSTRAINT name_length,
    ADD CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128);

CREATE FUNCTION {{schema}}{{prefix}}job_notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = 'available' THEN
        PERFORM pg_notify('river_insert', json_build_object('queue', NEW.queue)::text);
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER {{prefix}}notify AFTER INSERT ON {{schema}}{{prefix}}job
    FOR EACH ROW EXECUTE FUNCTION {{schema}}{{prefix}}job_notify();
