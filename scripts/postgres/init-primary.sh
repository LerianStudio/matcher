#!/bin/bash
set -e

# Validate required environment variables
if [ -z "$REPLICATOR_PASSWORD" ]; then
    echo "ERROR: REPLICATOR_PASSWORD environment variable is required"
    exit 1
fi

# Default replication CIDR to Docker bridge network (172.16.0.0/12 covers typical Docker networks)
REPLICATION_CIDR="${REPLICATION_CIDR:-172.16.0.0/12}"

# Create replication user for streaming replication
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'replicator') THEN
            CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
        ELSE
            ALTER USER replicator WITH ENCRYPTED PASSWORD '$REPLICATOR_PASSWORD';
        END IF;
    END
    \$\$;
    
    SELECT pg_create_physical_replication_slot('replica_slot')
    WHERE NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = 'replica_slot');
EOSQL

# Update pg_hba.conf for replication connections with restricted CIDR
if ! grep -q "host replication replicator" "$PGDATA/pg_hba.conf"; then
    echo "host replication replicator $REPLICATION_CIDR md5" >> "$PGDATA/pg_hba.conf"
    pg_ctl reload
fi

echo "Primary database configured for replication"
