DROP TABLE {{schema}}{{prefix}}client_queue;
DROP TABLE {{schema}}{{prefix}}client;

ALTER TABLE {{schema}}{{prefix}}job ALTER COLUMN max_attempts SET DEFAULT 25;
ALTER TABLE {{schema}}{{prefix}}queue ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP;

-- Outbox for drivers without LISTEN/NOTIFY. KopiQueue does not use it on PostgreSQL.
CREATE TABLE {{schema}}{{prefix}}notification (
    id bigserial,
    created_at timestamptz NOT NULL DEFAULT now(),
    payload text NOT NULL,
    topic text NOT NULL,
    CONSTRAINT {{prefix}}notification_pkey PRIMARY KEY (id),
    CONSTRAINT topic_length CHECK (length(topic) > 0 AND length(topic) < 128)
);

CREATE INDEX {{prefix}}notification_created_at_idx ON {{schema}}{{prefix}}notification USING btree (created_at);
CREATE INDEX {{prefix}}notification_topic_id_idx ON {{schema}}{{prefix}}notification USING btree (topic, id);
