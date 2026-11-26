-- Test line only: stands in for the real main line (plan task 1.2).
CREATE TABLE {{schema}}{{prefix}}migration (
    line text NOT NULL,
    version bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (line, version)
);
