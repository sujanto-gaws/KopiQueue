# PostgreSQL spike raw results (0.7, 0.8)

Unedited output. Reports: [0.7](0.7-npgsql-listen-notify.md), [0.8](0.8-river-schema-and-unique-keys.md).

## Spike 0.7: `dotnet run --project spikes/NpgsqlSpikes -c Release -- all`

Server: PostgreSQL 18.0 on x86_64-windows, compiled by msvc-19.44.35217, 64-bit. Npgsql 11.0.0.0, .NET 11.0.0, same machine.

## A. Commit-to-delivery latency and burst throughput

- 1,000 sequential transactions, commit start → listener event: p50 0.22 ms, p95 0.38 ms, p99 0.50 ms, max 2.30 ms
- Burst: 8 senders × 2,500 autocommit notifies = 20,000 sent in 1.97 s (10,154/s); received 20,000

## B. Transaction semantics and de-duplication

- One transaction: 100 × identical payload + 3 distinct payloads on `public.river_insert` → received **4**: {"queue":"same"}, {"queue":"a"}, {"queue":"b"}, {"queue":"c"}
- Notify then rollback → received 0
- Notify, savepoint, notify, rollback to savepoint, commit → received: {"queue":"sp"}
- Same payload in two separate transactions → received 2

## C. Payload size limit

- 7,999 ASCII bytes: OK
- 8,000 ASCII bytes: **22023** payload string too long
- 2,700 × '☕' (8,100 UTF-8 bytes): **22023** payload string too long

## D. Channel names

- `LISTEN public.river_insert`: **42601** syntax error at or near "."
- `LISTEN "public.river_insert"`: OK 
- `LISTEN "ssssssssssssssssssssssssssssssssssssssssssssssssssss…`: OK identifier "ssssssssssssssssssssssssssssssssssssssssssssssssssss.river_insert" will be truncated to "ssssssssssssssssssssssssssssssssssssssssssssssssssss.river_inse"
- pg_listening_channels(): public.river_insert, ssssssssssssssssssssssssssssssssssssssssssssssssssss.river_inse
- pg_notify with a 65-byte channel name: **22023** channel name too long

## E. LISTEN on a pooled connection

- Same physical connection after return to pool: True; listening channels after reopen: []; notifications received: 0
  → The pool reset (DISCARD ALL) silently drops LISTEN. The notifier must own a dedicated, non-pooled connection.

## F. Server-side disconnect (pg_terminate_backend)

- Round 1: detected after 2.3 ms; error: PostgresException 57P01; connection state Broken
  Reconnect + LISTEN: 47.3 ms. Of 5 notifications sent while disconnected, received after reconnect: 0
- Round 2: detected after 1.2 ms; error: PostgresException 57P01; connection state Broken
  Reconnect + LISTEN: 59.6 ms. Of 5 notifications sent while disconnected, received after reconnect: 0
- Round 3: detected after 1.2 ms; error: PostgresException 57P01; connection state Broken
  Reconnect + LISTEN: 47.5 ms. Of 5 notifications sent while disconnected, received after reconnect: 0

## G. Silent connection loss (TCP proxy that stops forwarding)

- Plain WaitAsync, no keepalive: notification before blackhole received=True; failure detected: **not detected in 40 s**
- Npgsql Keepalive=2: notification before blackhole received=True; failure detected: 3.5 s (NpgsqlException: TimeoutException)
- Npgsql Keepalive=10: notification before blackhole received=True; failure detected: 19.5 s (NpgsqlException: TimeoutException)
- WaitAsync(2 s) + `SELECT 1` probe, Command Timeout=2: notification before blackhole received=True; failure detected: 33.5 s (NpgsqlException: TimeoutException)
- Delay(2 s) + `SELECT 1` probe (no WaitAsync), Command Timeout=2: notification before blackhole received=False; failure detected: 33.5 s (NpgsqlException: TimeoutException)
- WaitAsync(2 s) + `SELECT 1` probe + 3 s external watchdog: notification before blackhole received=True; failure detected: 4.5 s (TimeoutException)


## Spike 0.8: catalog introspection (`spikes/RiverSchema/introspect.sql`) of a database migrated by River CLI v0.51.2

```text
== relations
          relname          | relkind | persistence 
---------------------------+---------+-------------
 river_job_id_seq          | S       | logged
 river_notification_id_seq | S       | logged
 river_job                 | r       | logged
 river_leader              | r       | UNLOGGED
 river_migration           | r       | logged
 river_notification        | r       | logged
 river_queue               | r       | logged

== columns
        tbl         | n  |      col      |           type           | not_null | ident | gen |                  default_expr                  
--------------------+----+---------------+--------------------------+----------+-------+-----+------------------------------------------------
 river_job          |  1 | id            | bigint                   | t        |       |     | nextval('river_job_id_seq'::regclass)
 river_job          |  2 | state         | river_job_state          | t        |       |     | 'available'::river_job_state
 river_job          |  3 | attempt       | smallint                 | t        |       |     | 0
 river_job          |  4 | max_attempts  | smallint                 | t        |       |     | 25
 river_job          |  5 | attempted_at  | timestamp with time zone | f        |       |     | 
 river_job          |  6 | created_at    | timestamp with time zone | t        |       |     | now()
 river_job          |  7 | finalized_at  | timestamp with time zone | f        |       |     | 
 river_job          |  8 | scheduled_at  | timestamp with time zone | t        |       |     | now()
 river_job          |  9 | priority      | smallint                 | t        |       |     | 1
 river_job          | 10 | args          | jsonb                    | t        |       |     | 
 river_job          | 11 | attempted_by  | text[]                   | f        |       |     | 
 river_job          | 12 | errors        | jsonb[]                  | f        |       |     | 
 river_job          | 13 | kind          | text                     | t        |       |     | 
 river_job          | 14 | metadata      | jsonb                    | t        |       |     | '{}'::jsonb
 river_job          | 15 | queue         | text                     | t        |       |     | 'default'::text
 river_job          | 16 | tags          | character varying(255)[] | t        |       |     | '{}'::character varying[]
 river_job          | 17 | unique_key    | bytea                    | f        |       |     | 
 river_job          | 18 | unique_states | bit(8)                   | f        |       |     | 
 river_leader       |  1 | elected_at    | timestamp with time zone | t        |       |     | 
 river_leader       |  2 | expires_at    | timestamp with time zone | t        |       |     | 
 river_leader       |  3 | leader_id     | text                     | t        |       |     | 
 river_leader       |  4 | name          | text                     | t        |       |     | 'default'::text
 river_migration    |  1 | line          | text                     | t        |       |     | 
 river_migration    |  2 | version       | bigint                   | t        |       |     | 
 river_migration    |  3 | created_at    | timestamp with time zone | t        |       |     | now()
 river_notification |  1 | id            | bigint                   | t        |       |     | nextval('river_notification_id_seq'::regclass)
 river_notification |  2 | created_at    | timestamp with time zone | t        |       |     | now()
 river_notification |  3 | payload       | text                     | t        |       |     | 
 river_notification |  4 | topic         | text                     | t        |       |     | 
 river_queue        |  1 | name          | text                     | t        |       |     | 
 river_queue        |  2 | created_at    | timestamp with time zone | t        |       |     | now()
 river_queue        |  3 | metadata      | jsonb                    | t        |       |     | '{}'::jsonb
 river_queue        |  4 | paused_at     | timestamp with time zone | f        |       |     | 
 river_queue        |  5 | updated_at    | timestamp with time zone | t        |       |     | CURRENT_TIMESTAMP

== constraints
        tbl         |                conname                 | contype |                                                                                                                                                     def                                                                                                                                                     
--------------------+----------------------------------------+---------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 river_job          | finalized_or_finalized_at_null         | c       | CHECK ((((finalized_at IS NULL) AND (state <> ALL (ARRAY['cancelled'::river_job_state, 'completed'::river_job_state, 'discarded'::river_job_state]))) OR ((finalized_at IS NOT NULL) AND (state = ANY (ARRAY['cancelled'::river_job_state, 'completed'::river_job_state, 'discarded'::river_job_state])))))
 river_job          | kind_length                            | c       | CHECK (((char_length(kind) > 0) AND (char_length(kind) < 128)))
 river_job          | max_attempts_is_positive               | c       | CHECK ((max_attempts > 0))
 river_job          | priority_in_range                      | c       | CHECK (((priority >= 1) AND (priority <= 4)))
 river_job          | queue_length                           | c       | CHECK (((char_length(queue) > 0) AND (char_length(queue) < 128)))
 river_job          | river_job_args_not_null                | n       | NOT NULL args
 river_job          | river_job_attempt_not_null             | n       | NOT NULL attempt
 river_job          | river_job_created_at_not_null          | n       | NOT NULL created_at
 river_job          | river_job_id_not_null                  | n       | NOT NULL id
 river_job          | river_job_kind_not_null                | n       | NOT NULL kind
 river_job          | river_job_max_attempts_not_null        | n       | NOT NULL max_attempts
 river_job          | river_job_metadata_not_null            | n       | NOT NULL metadata
 river_job          | river_job_priority_not_null            | n       | NOT NULL priority
 river_job          | river_job_queue_not_null               | n       | NOT NULL queue
 river_job          | river_job_scheduled_at_not_null        | n       | NOT NULL scheduled_at
 river_job          | river_job_state_not_null               | n       | NOT NULL state
 river_job          | river_job_tags_not_null                | n       | NOT NULL tags
 river_job          | river_job_pkey                         | p       | PRIMARY KEY (id)
 river_leader       | leader_id_length                       | c       | CHECK (((char_length(leader_id) > 0) AND (char_length(leader_id) < 128)))
 river_leader       | name_length                            | c       | CHECK ((name = 'default'::text))
 river_leader       | river_leader_elected_at_not_null       | n       | NOT NULL elected_at
 river_leader       | river_leader_expires_at_not_null       | n       | NOT NULL expires_at
 river_leader       | river_leader_leader_id_not_null        | n       | NOT NULL leader_id
 river_leader       | river_leader_name_not_null             | n       | NOT NULL name
 river_leader       | river_leader_pkey                      | p       | PRIMARY KEY (name)
 river_migration    | line_length                            | c       | CHECK (((char_length(line) > 0) AND (char_length(line) < 128)))
 river_migration    | version_gte_1                          | c       | CHECK ((version >= 1))
 river_migration    | river_migration_created_at_not_null1   | n       | NOT NULL created_at
 river_migration    | river_migration_line_not_null          | n       | NOT NULL line
 river_migration    | river_migration_version_not_null1      | n       | NOT NULL version
 river_migration    | river_migration_pkey1                  | p       | PRIMARY KEY (line, version)
 river_notification | topic_length                           | c       | CHECK (((length(topic) > 0) AND (length(topic) < 128)))
 river_notification | river_notification_created_at_not_null | n       | NOT NULL created_at
 river_notification | river_notification_id_not_null         | n       | NOT NULL id
 river_notification | river_notification_payload_not_null    | n       | NOT NULL payload
 river_notification | river_notification_topic_not_null      | n       | NOT NULL topic
 river_notification | river_notification_pkey                | p       | PRIMARY KEY (id)
 river_queue        | river_queue_created_at_not_null        | n       | NOT NULL created_at
 river_queue        | river_queue_metadata_not_null          | n       | NOT NULL metadata
 river_queue        | river_queue_name_not_null              | n       | NOT NULL name
 river_queue        | river_queue_updated_at_not_null        | n       | NOT NULL updated_at
 river_queue        | river_queue_pkey                       | p       | PRIMARY KEY (name)

== indexes
     tablename      |               indexname                |                                                                                                  indexdef                                                                                                   
--------------------+----------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 river_job          | river_job_args_index                   | CREATE INDEX river_job_args_index ON public.river_job USING gin (args)
 river_job          | river_job_kind                         | CREATE INDEX river_job_kind ON public.river_job USING btree (kind)
 river_job          | river_job_metadata_index               | CREATE INDEX river_job_metadata_index ON public.river_job USING gin (metadata)
 river_job          | river_job_pkey                         | CREATE UNIQUE INDEX river_job_pkey ON public.river_job USING btree (id)
 river_job          | river_job_prioritized_fetching_index   | CREATE INDEX river_job_prioritized_fetching_index ON public.river_job USING btree (state, queue, priority, scheduled_at, id)
 river_job          | river_job_state_and_finalized_at_index | CREATE INDEX river_job_state_and_finalized_at_index ON public.river_job USING btree (state, finalized_at) WHERE (finalized_at IS NOT NULL)
 river_job          | river_job_unique_idx                   | CREATE UNIQUE INDEX river_job_unique_idx ON public.river_job USING btree (unique_key) WHERE ((unique_key IS NOT NULL) AND (unique_states IS NOT NULL) AND river_job_state_in_bitmask(unique_states, state))
 river_leader       | river_leader_pkey                      | CREATE UNIQUE INDEX river_leader_pkey ON public.river_leader USING btree (name)
 river_migration    | river_migration_pkey1                  | CREATE UNIQUE INDEX river_migration_pkey1 ON public.river_migration USING btree (line, version)
 river_notification | river_notification_created_at_idx      | CREATE INDEX river_notification_created_at_idx ON public.river_notification USING btree (created_at)
 river_notification | river_notification_pkey                | CREATE UNIQUE INDEX river_notification_pkey ON public.river_notification USING btree (id)
 river_notification | river_notification_topic_id_idx        | CREATE INDEX river_notification_topic_id_idx ON public.river_notification USING btree (topic, id)
 river_queue        | river_queue_pkey                       | CREATE UNIQUE INDEX river_queue_pkey ON public.river_queue USING btree (name)

== enum types
     typname     |                                       labels                                       
-----------------+------------------------------------------------------------------------------------
 river_job_state | available, cancelled, completed, discarded, pending, retryable, running, scheduled

== other types
 typname | typtype 
---------+---------

== functions (signatures only)
          proname           |                args                | returns | lang | volatility | parallel 
----------------------------+------------------------------------+---------+------+------------+----------
 river_job_state_in_bitmask | bitmask bit, state river_job_state | boolean | sql  | i          | u

== triggers
 tbl | tgname | def 
-----+--------+-----

== table comments
 relname | comment 
---------+---------

```
