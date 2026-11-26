#!/bin/sh
# Starts two PgBouncer instances in front of the CI PostgreSQL (plan task 2.11, NFR-COMP-04):
#   127.0.0.1:6542  pool_mode = transaction
#   127.0.0.1:6543  pool_mode = session
# Usage: tests/pgbouncer/start.sh <postgres password>   (PgBouncer must be installed, e.g. apt-get install pgbouncer)
set -eu
password=$1
dir=${RUNNER_TEMP:-/tmp}/pgbouncer
mkdir -p "$dir"
printf '"postgres" "%s"\n' "$password" > "$dir/userlist.txt"

for mode in transaction session; do
  port=6542
  [ "$mode" = session ] && port=6543
  cat > "$dir/$mode.ini" <<EOF
[databases]
* = host=127.0.0.1 port=5432

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = $port
unix_socket_dir =
auth_type = scram-sha-256
auth_file = $dir/userlist.txt
pool_mode = $mode
max_client_conn = 500
default_pool_size = 20
ignore_startup_parameters = extra_float_digits
logfile = $dir/$mode.log
pidfile = $dir/$mode.pid
EOF
  pgbouncer -d "$dir/$mode.ini"
done

# Wait until both accept connections; fail with PgBouncer's log if one never does.
for mode in transaction session; do
  port=6542
  [ "$mode" = session ] && port=6543
  up=no
  for _ in $(seq 1 30); do
    if PGPASSWORD=$password psql -h 127.0.0.1 -p $port -U postgres -d postgres -Atc 'SELECT 1' >/dev/null 2>&1; then
      up=yes
      break
    fi
    sleep 1
  done
  if [ "$up" = no ]; then
    echo "pgbouncer ($mode, port $port) did not come up:" >&2
    cat "$dir/$mode.log" >&2 || true
    exit 1
  fi
  echo "pgbouncer ($mode) is up on $port"
done
