ALTER TABLE {{schema}}{{prefix}}job
    ALTER COLUMN tags DROP NOT NULL,
    ALTER COLUMN tags DROP DEFAULT;
