CREATE TABLE {{schema}}{{prefix}}widget (id bigint PRIMARY KEY, name text NOT NULL);
CREATE FUNCTION {{schema}}{{prefix}}widget_count() RETURNS bigint LANGUAGE sql AS $$ SELECT count(*) FROM {{schema}}{{prefix}}widget; $$;
