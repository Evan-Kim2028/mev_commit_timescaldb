#!/bin/bash

# Wait until TimescaleDB is ready
until pg_isready -h mev_timescaledb -p 5432 -U postgres; do
  echo "Waiting for TimescaleDB to be ready..."
  sleep 2
done

echo "TimescaleDB is ready. Starting refresh scheduler."

while true; do
  echo "$(date): Refreshing materialized view 'total_preconf_stats'..."
  if psql -h mev_timescaledb -p 5432 -U postgres -d mev_commit_testnet -c "REFRESH MATERIALIZED VIEW api.total_preconf_stats;"; then
    echo "Refresh 'total_preconf_stats' successful."
  else
    echo "Refresh 'total_preconf_stats' failed!" >&2
  fi

  echo "$(date): Refreshing materialized view 'preconf_txs'..."
  if psql -h mev_timescaledb -p 5432 -U postgres -d mev_commit_testnet -c "REFRESH MATERIALIZED VIEW api.preconf_txs;"; then
    echo "Refresh 'preconf_txs' successful."
  else
    echo "Refresh 'preconf_txs' failed!" >&2
  fi

  echo "Sleeping for 60 seconds."
  sleep 60
done
