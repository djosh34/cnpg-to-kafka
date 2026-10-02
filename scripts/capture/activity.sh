#!/usr/bin/env bash
# Runs inside the synthetic client pod; direct TCP sessions to each instance.
set -euo pipefail
export PGDATABASE=app PGCONNECT_TIMEOUT=3
for round in $(seq 1 14); do
  for host in "$@"; do
    for role in included excluded; do
      # A successful psql session closes normally on command completion.
      if ! PGPASSWORD=capture-only psql -h "$host" -U "$role" -c 'SELECT current_user, pg_is_in_recovery();'; then
        # First round must work; later attempts may overlap the deliberate restart.
        [[ $round != 1 ]] || exit 1
      fi
      if PGPASSWORD=deliberately-wrong psql -h "$host" -U "$role" -c 'SELECT 1'; then
        echo 'Unexpected successful bad-password authentication' >&2; exit 1
      fi
    done
    if PGPASSWORD=deliberately-wrong psql -h "$host" -U unknown_role -c 'SELECT 1'; then
      echo 'Unexpected successful unknown-role authentication' >&2; exit 1
    fi
    if [[ $round == 3 || $round == 6 || $round == 11 ]]; then
      # Ordinary PostgreSQL LOG output crosses the kubelet threshold on each pod.
      PGPASSWORD=capture-only psql -h "$host" -U included -v ON_ERROR_STOP=1 <<'SQL'
DO $$ BEGIN FOR i IN 1..35 LOOP RAISE LOG 'capture rotation activity %', repeat('x',4000); END LOOP; END $$;
SQL
    fi
  done
  sleep 18
done
