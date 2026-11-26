-- Builds an index without blocking writes.
-- kopi:no-transaction
CREATE INDEX CONCURRENTLY first_id_idx ON {{schema}}{{prefix}}first (id);
