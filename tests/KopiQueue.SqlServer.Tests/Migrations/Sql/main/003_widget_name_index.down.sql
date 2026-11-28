-- kopi:no-transaction
DROP INDEX {{prefix}}widget_name_idx ON {{schema}}{{prefix}}widget;
DELETE FROM {{schema}}{{prefix}}widget_log WHERE step = 3;
