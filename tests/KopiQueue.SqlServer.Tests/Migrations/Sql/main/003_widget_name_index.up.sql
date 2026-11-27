-- kopi:no-transaction
CREATE INDEX {{prefix}}widget_name_idx ON {{schema}}{{prefix}}widget (name);
DECLARE @trancount int = @@TRANCOUNT;
INSERT INTO {{schema}}{{prefix}}widget_log (step, trancount) VALUES (3, @trancount);
