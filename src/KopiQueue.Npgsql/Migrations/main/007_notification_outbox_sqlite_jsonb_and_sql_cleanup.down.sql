DROP TABLE {{schema}}{{prefix}}notification;

ALTER TABLE {{schema}}{{prefix}}job ALTER COLUMN max_attempts DROP DEFAULT;
ALTER TABLE {{schema}}{{prefix}}queue ALTER COLUMN updated_at DROP DEFAULT;

CREATE UNLOGGED TABLE {{schema}}{{prefix}}client (
    id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    metadata jsonb NOT NULL DEFAULT '{}',
    paused_at timestamptz,
    updated_at timestamptz NOT NULL,
    CONSTRAINT {{prefix}}client_pkey PRIMARY KEY (id),
    CONSTRAINT name_length CHECK (char_length(id) > 0 AND char_length(id) < 128)
);

CREATE UNLOGGED TABLE {{schema}}{{prefix}}client_queue (
    river_client_id text NOT NULL,
    name text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    max_workers bigint NOT NULL DEFAULT 0,
    metadata jsonb NOT NULL DEFAULT '{}',
    num_jobs_completed bigint NOT NULL DEFAULT 0,
    num_jobs_running bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL,
    CONSTRAINT {{prefix}}client_queue_pkey PRIMARY KEY (river_client_id, name),
    CONSTRAINT {{prefix}}client_queue_river_client_id_fkey FOREIGN KEY (river_client_id)
        REFERENCES {{schema}}{{prefix}}client (id) ON DELETE CASCADE,
    CONSTRAINT name_length CHECK (char_length(name) > 0 AND char_length(name) < 128),
    CONSTRAINT num_jobs_completed_zero_or_positive CHECK (num_jobs_completed >= 0),
    CONSTRAINT num_jobs_running_zero_or_positive CHECK (num_jobs_running >= 0)
);
