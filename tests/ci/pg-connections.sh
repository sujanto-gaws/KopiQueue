#!/bin/sh
# Samples the CI PostgreSQL's client connections once a second while the tests run, then reports the peak (#160).
#   tests/ci/pg-connections.sh sample <file>   runs until killed; start it in the background before dotnet test
#   tests/ci/pg-connections.sh report <file>   prints a Markdown summary of the samples
# Connects as the tests do (PGHOST, PGUSER, PGPASSWORD); a failed sample is skipped, never fatal.
set -u
mode=$1
file=$2

case "$mode" in
sample)
  # time, total, idle, idle for over 10 s, active or in a transaction, from 127.0.0.1 (PgBouncer's server side)
  query="SELECT to_char(clock_timestamp(), 'HH24:MI:SS'),
                count(*),
                count(*) FILTER (WHERE state = 'idle'),
                count(*) FILTER (WHERE state = 'idle' AND state_change < clock_timestamp() - interval '10 seconds'),
                count(*) FILTER (WHERE state <> 'idle'),
                count(*) FILTER (WHERE client_addr = '127.0.0.1')
         FROM pg_stat_activity
         WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()"
  while :; do
    psql -X -At -F, -c "$query" >> "$file" 2>/dev/null || true
    sleep 1
  done
  ;;
report)
  limit=$(psql -X -At -c "SELECT current_setting('max_connections')::int - current_setting('superuser_reserved_connections')::int" 2>/dev/null || echo "?")
  if [ ! -s "$file" ]; then
    echo "No connection samples were recorded."
    exit 0
  fi
  echo "### PostgreSQL client connections during the tests"
  echo
  echo "$(wc -l < "$file") samples, one a second. The server accepts $limit ordinary connections (superusers get the reserved slots too)."
  echo
  echo "| | Peak | At |"
  echo "|---|---|---|"
  awk -F, -v OFS=' | ' '
    { for (c = 2; c <= 6; c++) if ($c + 0 > max[c]) { max[c] = $c + 0; at[c] = $1 } }
    END {
      split("total,idle,idle for over 10 s,active or in a transaction,from 127.0.0.1 (PgBouncer and others)", name, ",")
      for (c = 2; c <= 6; c++) print "| " name[c - 1], max[c], at[c] " |"
    }' "$file"
  echo
  echo "The five busiest seconds (time, total, idle, idle > 10 s, busy, from 127.0.0.1):"
  echo
  echo '```'
  sort -t, -k2,2nr "$file" | head -5
  echo '```'
  ;;
*)
  echo "usage: $0 sample|report <file>" >&2
  exit 2
  ;;
esac
