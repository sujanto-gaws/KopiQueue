COPY {{schema}}{{prefix}}job (args, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags) FROM STDIN (FORMAT BINARY)
