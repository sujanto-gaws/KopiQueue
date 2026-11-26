-- Migration tracking keyed by (line, version). Recorded versions keep their timestamps.
-- The primary key name is the one River's schema has from this version on.
ALTER TABLE {{schema}}{{prefix}}migration RENAME TO {{prefix}}migration_old;

CREATE TABLE {{schema}}{{prefix}}migration (
    line text NOT NULL,
    version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT {{prefix}}migration_pkey1 PRIMARY KEY (line, version),
    CONSTRAINT line_length CHECK (char_length(line) > 0 AND char_length(line) < 128),
    CONSTRAINT version_gte_1 CHECK (version >= 1)
);

INSERT INTO {{schema}}{{prefix}}migration (created_at, line, version)
SELECT created_at, 'main', version FROM {{schema}}{{prefix}}migration_old;

DROP TABLE {{schema}}{{prefix}}migration_old;

ALTER TABLE {{schema}}{{prefix}}job ADD COLUMN unique_key bytea;

CREATE UNIQUE INDEX {{prefix}}job_kind_unique_key_idx ON {{schema}}{{prefix}}job USING btree (kind, unique_key)
    WHERE unique_key IS NOT NULL;

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
