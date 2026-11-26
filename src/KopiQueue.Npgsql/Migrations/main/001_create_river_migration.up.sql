-- Migration tracking, original shape. Version 5 replaces it with a table keyed by (line, version).
CREATE TABLE {{schema}}{{prefix}}migration (
    id bigserial,
    created_at timestamptz NOT NULL DEFAULT now(),
    version bigint NOT NULL,
    CONSTRAINT {{prefix}}migration_pkey PRIMARY KEY (id),
    CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX {{prefix}}migration_version_idx ON {{schema}}{{prefix}}migration USING btree (version);
