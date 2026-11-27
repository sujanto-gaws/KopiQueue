-- Test line only: stands in for the real main line (plan task 5.1).
CREATE TABLE {{schema}}{{prefix}}migration (
  line       nvarchar(128) NOT NULL,
  version    bigint        NOT NULL,
  created_at datetime2(6)  NOT NULL DEFAULT SYSUTCDATETIME(),
  PRIMARY KEY (line, version)
);
