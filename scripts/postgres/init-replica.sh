#!/bin/bash
set -e

PGDATA="${PGDATA:-/var/lib/postgresql/data}"

# Validate PGDATA is set and safe
if [ -z "$PGDATA" ] || [ "$PGDATA" = "/" ]; then
    echo "ERROR: PGDATA must be set and not root"
    exit 1
fi

# Validate required environment variables
if [ -z "$REPLICATOR_PASSWORD" ]; then
    echo "ERROR: REPLICATOR_PASSWORD environment variable is required"
    exit 1
fi

# Wait for primary to be ready
echo "Waiting for primary database..."
until PGPASSWORD="$POSTGRES_PASSWORD" pg_isready -h postgres -p 5432 -U "$POSTGRES_USER"; do
  echo "Primary not ready, waiting..."
  sleep 2
done

# Check if already initialized as replica
if [ -f "$PGDATA/standby.signal" ]; then
  echo "Replica already initialized, starting postgres..."
  if [ "$(id -u)" = "0" ]; then
    exec gosu postgres postgres
  else
    exec postgres
  fi
fi

# Check if data directory has valid data
if [ -f "$PGDATA/PG_VERSION" ]; then
  echo "Data directory exists but not a replica, reinitializing..."
  # Additional safety check before rm -rf
  if [ -d "$PGDATA" ] && [ -n "$PGDATA" ] && [ "$PGDATA" != "/" ]; then
    rm -rf "$PGDATA"/*
  else
    echo "ERROR: Unsafe PGDATA path detected, aborting"
    exit 1
  fi
fi

echo "Initializing replica from primary..."

# Wait for replication slot to be available
SLOT_FOUND=false
for i in {1..30}; do
  if PGPASSWORD="$POSTGRES_PASSWORD" psql -h postgres -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica_slot'" | grep -q 1; then
    echo "Replication slot ready"
    SLOT_FOUND=true
    break
  fi
  echo "Waiting for replication slot... ($i/30)"
  sleep 2
done

# Fail explicitly if slot was not found
if [ "$SLOT_FOUND" != "true" ]; then
  echo "ERROR: Replication slot 'replica_slot' not found after timeout"
  exit 1
fi

# Backup from primary using replicator user
PGPASSWORD="$REPLICATOR_PASSWORD" pg_basebackup \
  -h postgres \
  -p 5432 \
  -U replicator \
  -D "$PGDATA" \
  -Fp -Xs -P -R

# Configure standby settings
cat >> "$PGDATA/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=postgres port=5432 user=replicator password=$REPLICATOR_PASSWORD application_name=replica1'
primary_slot_name = 'replica_slot'
EOF

# Create standby signal file
touch "$PGDATA/standby.signal"

echo "Replica initialized successfully, starting postgres..."

# Fix ownership and permissions if running as root
if [ "$(id -u)" = "0" ]; then
  chown -R postgres:postgres "$PGDATA"
  chmod 700 "$PGDATA"
  exec gosu postgres postgres
else
  chmod 700 "$PGDATA"
  exec postgres
fi
