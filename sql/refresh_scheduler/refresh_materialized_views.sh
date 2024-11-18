#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Wait until TimescaleDB is ready
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME"; do
  echo "Waiting for TimescaleDB to be ready..."
  sleep 2
done

echo "TimescaleDB is ready. Starting refresh scheduler."

# Define the database URI
DATABASE_URI="postgresql://$DB_USER:$DB_PASSWORD@$DB_HOST:$DB_PORT/$DB_NAME"

while true; do
  echo "$(date): Ensuring materialized views exist..."

  # Create materialized views if they don't exist by sourcing the SQL files
  psql "$DATABASE_URI" -v ON_ERROR_STOP=1 <<EOF
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'total_preconf_stats' AND schemaname = 'api') THEN
        \i /scripts/sql/views/total_preconf_stats.sql
        GRANT SELECT, INSERT, UPDATE, DELETE ON api.total_preconf_stats TO postgres;
    END IF;

    IF NOT EXISTS (SELECT FROM pg_matviews WHERE matviewname = 'preconf_txs' AND schemaname = 'api') THEN
        \i /scripts/sql/views/preconf_txs.sql
        GRANT SELECT, INSERT, UPDATE, DELETE ON api.preconf_txs TO postgres;
    END IF;
END
\$\$;
EOF

  echo "$(date): Refreshing materialized view 'total_preconf_stats'..."
  if psql "$DATABASE_URI" -c "REFRESH MATERIALIZED VIEW api.total_preconf_stats;"; then
    echo "Refresh 'total_preconf_stats' successful."
  else
    echo "Refresh 'total_preconf_stats' failed!" >&2
  fi

  echo "$(date): Refreshing materialized view 'preconf_txs'..."
  if psql "$DATABASE_URI" -c "REFRESH MATERIALIZED VIEW api.preconf_txs;"; then
    echo "Refresh 'preconf_txs' successful."
  else
    echo "Refresh 'preconf_txs' failed!" >&2
  fi

  echo "Sleeping for 60 seconds."
  sleep 60
done
