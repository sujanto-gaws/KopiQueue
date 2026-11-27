CREATE TABLE {{schema}}{{prefix}}widget (id bigint PRIMARY KEY, name nvarchar(100) NOT NULL);
CREATE TABLE {{schema}}{{prefix}}widget_log (step int NOT NULL, trancount int NOT NULL);
-- Read @@TRANCOUNT first: inside an INSERT it also counts the statement's own transaction.
DECLARE @trancount int = @@TRANCOUNT;
INSERT INTO {{schema}}{{prefix}}widget_log (step, trancount) VALUES (2, @trancount);
GO
-- CREATE VIEW must start a batch, so this script needs the separator above.
CREATE VIEW {{schema}}{{prefix}}widget_count AS SELECT COUNT_BIG(*) AS n FROM {{schema}}{{prefix}}widget;
