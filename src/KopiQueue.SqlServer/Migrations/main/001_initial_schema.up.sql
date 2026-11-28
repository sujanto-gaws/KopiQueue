-- KopiQueue's SQL Server schema: version 1 of the main line (design §4.4). The logical model is PostgreSQL's (§4.1),
-- but the schema is KopiQueue's own: River has no SQL Server driver, so nothing here needs River's names.
--
-- {{schema}} is the quoted schema and a dot, for example [dbo].; {{prefix}} is the table prefix, for example kopi_.
-- Every constraint is named, because SQL Server scopes constraint names to the schema: two prefixes can then share one.
--
-- Columns compared as names (state, kind, queue, the leader and the migration line) use a binary collation, so they
-- compare case-sensitively and ordinally, as on PostgreSQL, whatever the database's default collation is.
-- Times are UTC in datetime2(6): microseconds, as timestamptz.

CREATE TABLE {{schema}}{{prefix}}migration (
  line       nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  version    bigint        NOT NULL,
  created_at datetime2(6)  NOT NULL CONSTRAINT {{prefix}}migration_created_at_df DEFAULT SYSUTCDATETIME(),

  CONSTRAINT {{prefix}}migration_pk PRIMARY KEY CLUSTERED (line, version),
  CONSTRAINT {{prefix}}migration_line_length CHECK (LEN(line) BETWEEN 1 AND 127),
  CONSTRAINT {{prefix}}migration_version_gte_1 CHECK (version >= 1)
);

CREATE TABLE {{schema}}{{prefix}}job (
  id            bigint IDENTITY(1,1) NOT NULL,
  state         varchar(16)   COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT {{prefix}}job_state_df DEFAULT 'available',
  attempt       smallint      NOT NULL CONSTRAINT {{prefix}}job_attempt_df DEFAULT 0,
  max_attempts  smallint      NOT NULL CONSTRAINT {{prefix}}job_max_attempts_df DEFAULT 25,
  attempted_at  datetime2(6)  NULL,
  created_at    datetime2(6)  NOT NULL CONSTRAINT {{prefix}}job_created_at_df DEFAULT SYSUTCDATETIME(),
  finalized_at  datetime2(6)  NULL,
  scheduled_at  datetime2(6)  NOT NULL CONSTRAINT {{prefix}}job_scheduled_at_df DEFAULT SYSUTCDATETIME(),
  priority      smallint      NOT NULL CONSTRAINT {{prefix}}job_priority_df DEFAULT 1,
  args          nvarchar(max) NOT NULL,
  -- JSON arrays: client IDs, and one { "at", "attempt", "error", "trace" } object per failed attempt.
  attempted_by  nvarchar(max) NULL,
  errors        nvarchar(max) NULL,
  kind          nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  metadata      nvarchar(max) NOT NULL CONSTRAINT {{prefix}}job_metadata_df DEFAULT N'{}',
  queue         nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT {{prefix}}job_queue_df DEFAULT N'default',
  tags          nvarchar(max) NOT NULL CONSTRAINT {{prefix}}job_tags_df DEFAULT N'[]',
  unique_key    varbinary(32) NULL,
  -- The unique-states bitmask: available = 1, cancelled = 2, completed = 4, discarded = 8, pending = 16,
  -- retryable = 32, running = 64, scheduled = 128 (the same positions as PostgreSQL's bit(8)).
  unique_states tinyint       NULL,
  -- 0 while the job holds its unique key (a key, a bitmask, and a state in the bitmask), otherwise the job's own ID.
  -- The unique index on (unique_key, unique_slot) then allows one holder per key and any number of others (ADR-011).
  unique_slot AS CAST(CASE
      WHEN unique_key IS NOT NULL AND unique_states IS NOT NULL
       AND (unique_states & CASE state
             WHEN 'available' THEN 1  WHEN 'cancelled' THEN 2  WHEN 'completed' THEN 4   WHEN 'discarded' THEN 8
             WHEN 'pending'   THEN 16 WHEN 'retryable' THEN 32 WHEN 'running'   THEN 64  WHEN 'scheduled' THEN 128
           END) <> 0
      THEN 0 ELSE id END AS bigint) PERSISTED NOT NULL,

  CONSTRAINT {{prefix}}job_pk PRIMARY KEY CLUSTERED (id),
  CONSTRAINT {{prefix}}job_state_valid CHECK (state IN
    ('available', 'cancelled', 'completed', 'discarded', 'pending', 'retryable', 'running', 'scheduled')),
  CONSTRAINT {{prefix}}job_finalized_or_finalized_at_null CHECK (
    (finalized_at IS NULL     AND state NOT IN ('cancelled', 'completed', 'discarded')) OR
    (finalized_at IS NOT NULL AND state     IN ('cancelled', 'completed', 'discarded'))),
  CONSTRAINT {{prefix}}job_max_attempts_is_positive CHECK (max_attempts > 0),
  CONSTRAINT {{prefix}}job_priority_in_range CHECK (priority BETWEEN 1 AND 4),
  CONSTRAINT {{prefix}}job_kind_length CHECK (LEN(kind) BETWEEN 1 AND 127),
  CONSTRAINT {{prefix}}job_queue_length CHECK (LEN(queue) BETWEEN 1 AND 127),
  -- Before SQL Server 2022, ISJSON accepts only objects and arrays. Args and metadata are always objects.
  CONSTRAINT {{prefix}}job_args_json CHECK (ISJSON(args) = 1),
  CONSTRAINT {{prefix}}job_metadata_json CHECK (ISJSON(metadata) = 1),
  CONSTRAINT {{prefix}}job_tags_json CHECK (ISJSON(tags) = 1)
);

-- Fetch, the hot path: only available rows, so the index stays small however many jobs are finalised.
CREATE INDEX {{prefix}}job_fetch_idx ON {{schema}}{{prefix}}job (queue, priority, scheduled_at, id)
  WHERE state = 'available';

-- Scheduler: scheduled and retryable jobs that are due.
CREATE INDEX {{prefix}}job_schedule_idx ON {{schema}}{{prefix}}job (scheduled_at) INCLUDE (queue)
  WHERE state IN ('scheduled', 'retryable');

-- Rescuer: running jobs by when they were attempted.
CREATE INDEX {{prefix}}job_running_idx ON {{schema}}{{prefix}}job (attempted_at)
  WHERE state = 'running';

-- Job cleaner: finalised jobs by state and age.
CREATE INDEX {{prefix}}job_finalized_idx ON {{schema}}{{prefix}}job (state, finalized_at)
  WHERE finalized_at IS NOT NULL;

CREATE INDEX {{prefix}}job_kind_idx ON {{schema}}{{prefix}}job (kind);

-- Uniqueness: a job that does not hold its key has unique_slot = id, so the index allows any number of those. Only jobs
-- with a key are indexed. unique_slot depends on state, so an unfiltered index would delete and insert an entry for
-- every job at every state change: on a fetch or a complete, about 6 logical reads a job (plan task 5.9). Lookups name
-- unique_key IS NOT NULL, which lets them seek this index.
CREATE UNIQUE INDEX {{prefix}}job_unique_idx ON {{schema}}{{prefix}}job (unique_key, unique_slot)
  WHERE unique_key IS NOT NULL;

-- One row while a client leads. An ordinary table: it is written about every 5 s, so logging costs nothing worth saving.
CREATE TABLE {{schema}}{{prefix}}leader (
  name       varchar(16)   COLLATE Latin1_General_100_BIN2 NOT NULL CONSTRAINT {{prefix}}leader_name_df DEFAULT 'default',
  leader_id  nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  elected_at datetime2(6)  NOT NULL,
  expires_at datetime2(6)  NOT NULL,

  CONSTRAINT {{prefix}}leader_pk PRIMARY KEY CLUSTERED (name),
  CONSTRAINT {{prefix}}leader_name_is_default CHECK (name = 'default'),
  CONSTRAINT {{prefix}}leader_id_length CHECK (LEN(leader_id) BETWEEN 1 AND 127)
);

CREATE TABLE {{schema}}{{prefix}}queue (
  name       nvarchar(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
  created_at datetime2(6)  NOT NULL CONSTRAINT {{prefix}}queue_created_at_df DEFAULT SYSUTCDATETIME(),
  updated_at datetime2(6)  NOT NULL CONSTRAINT {{prefix}}queue_updated_at_df DEFAULT SYSUTCDATETIME(),
  paused_at  datetime2(6)  NULL,
  metadata   nvarchar(max) NOT NULL CONSTRAINT {{prefix}}queue_metadata_df DEFAULT N'{}',

  CONSTRAINT {{prefix}}queue_pk PRIMARY KEY CLUSTERED (name),
  CONSTRAINT {{prefix}}queue_name_length CHECK (LEN(name) BETWEEN 1 AND 127),
  CONSTRAINT {{prefix}}queue_metadata_json CHECK (ISJSON(metadata) = 1)
);
