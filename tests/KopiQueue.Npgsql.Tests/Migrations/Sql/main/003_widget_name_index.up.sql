-- kopi:no-transaction
CREATE INDEX CONCURRENTLY {{prefix}}widget_name_idx ON {{schema}}{{prefix}}widget (name);
