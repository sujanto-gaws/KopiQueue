CREATE TYPE {{schema}}{{prefix}}job_state AS ENUM (
    'available', 'cancelled', 'completed', 'discarded', 'retryable', 'running', 'scheduled'
);

CREATE TABLE {{schema}}{{prefix}}job (
    id bigserial,
    state {{schema}}{{prefix}}job_state NOT NULL DEFAULT 'available',
    attempt smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL,
    attempted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    finalized_at timestamptz,
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    priority smallint NOT NULL DEFAULT 1,
    args jsonb,
    attempted_by text[],
    errors jsonb[],
    kind text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    queue text NOT NULL DEFAULT 'default',
    tags varchar(255)[],
    CONSTRAINT {{prefix}}job_pkey PRIMARY KEY (id),
    CONSTRAINT finalized_or_finalized_at_null CHECK (
        (state IN ('cancelled', 'completed', 'discarded') AND finalized_at IS NOT NULL) OR finalized_at IS NULL),
    CONSTRAINT max_attempts_is_positive CHECK (max_attempts > 0),
    CONSTRAINT priority_in_range CHECK (priority >= 1 AND priority <= 4),
    CONSTRAINT queue_length CHECK (char_length(queue) > 0 AND char_length(queue) < 128),
    CONSTRAINT kind_length CHECK (char_length(kind) > 0 AND char_length(kind) < 128)
);

CREATE INDEX {{prefix}}job_kind ON {{schema}}{{prefix}}job USING btree (kind);
CREATE INDEX {{prefix}}job_state_and_finalized_at_index ON {{schema}}{{prefix}}job USING btree (state, finalized_at)
    WHERE finalized_at IS NOT NULL;
CREATE INDEX {{prefix}}job_prioritized_fetching_index ON {{schema}}{{prefix}}job USING btree (state, queue, priority, scheduled_at, id);
CREATE INDEX {{prefix}}job_args_index ON {{schema}}{{prefix}}job USING gin (args);
CREATE INDEX {{prefix}}job_metadata_index ON {{schema}}{{prefix}}job USING gin (metadata);

-- Wakes producers when an available job is inserted. Versions 2 and 3 only: version 4 drops it.
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

CREATE UNLOGGED TABLE {{schema}}{{prefix}}leader (
    elected_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    leader_id text NOT NULL,
    name text NOT NULL,
    CONSTRAINT {{prefix}}leader_pkey PRIMARY KEY (name),
    CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128),
    CONSTRAINT leader_id_length CHECK (char_length(leader_id) > 0 AND char_length(leader_id) < 128)
);
