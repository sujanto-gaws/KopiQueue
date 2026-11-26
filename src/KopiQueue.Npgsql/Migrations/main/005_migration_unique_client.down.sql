-- The original tracking table has no line column, so it can only hold the main line.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM {{schema}}{{prefix}}migration WHERE line <> 'main') THEN
        RAISE EXCEPTION 'Cannot revert version 5: migration lines other than main are recorded, and the original migration table cannot hold them. Revert those lines first.';
    END IF;
END
$$;

ALTER TABLE {{schema}}{{prefix}}migration RENAME TO {{prefix}}migration_old;

CREATE TABLE {{schema}}{{prefix}}migration (
    id bigserial,
    created_at timestamptz NOT NULL DEFAULT now(),
    version bigint NOT NULL,
    CONSTRAINT {{prefix}}migration_pkey PRIMARY KEY (id),
    CONSTRAINT version CHECK (version >= 1)
);

CREATE UNIQUE INDEX {{prefix}}migration_version_idx ON {{schema}}{{prefix}}migration USING btree (version);

INSERT INTO {{schema}}{{prefix}}migration (created_at, version)
SELECT created_at, version FROM {{schema}}{{prefix}}migration_old ORDER BY version;

DROP TABLE {{schema}}{{prefix}}migration_old;

DROP TABLE {{schema}}{{prefix}}client_queue;
DROP TABLE {{schema}}{{prefix}}client;

DROP INDEX {{schema}}{{prefix}}job_kind_unique_key_idx;
ALTER TABLE {{schema}}{{prefix}}job DROP COLUMN unique_key;
