// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

package bootstrap

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
)

func TestRunMigrations_DiscoverySlice_ApplyRollbackAndReapply(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pgContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("matcher_test"),
		postgres.WithUsername("matcher"),
		postgres.WithPassword("matcher_test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pgContainer.Terminate(context.Background()))
	}()

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	logger := &libLog.NopLogger{}
	require.NoError(t, RunMigrations(ctx, dsn, "matcher_test", "migrations", logger, false))

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.PingContext(ctx))

	t.Run("applies discovery schema constraints and indexes", func(t *testing.T) {
		require.True(t, tableExists(t, ctx, db, "fetcher_connections"))
		require.True(t, tableExists(t, ctx, db, "discovered_schemas"))
		require.True(t, tableExists(t, ctx, db, "extraction_requests"))

		assert.Contains(t, enumLabels(t, ctx, db, "reconciliation_source_type"), "FETCHER")
		assert.True(t, indexExists(t, ctx, db, "idx_extraction_requests_connection_id"))

		fetcherConnID := mustInsertFetcherConnection(t, ctx, db, "fetcher-conn-1")
		extractionID := mustInsertExtractionRequest(t, ctx, db, fetcherConnID)

		ingestionJobID := mustInsertIngestionJob(t, ctx, db)
		_, err := db.ExecContext(ctx,
			"UPDATE extraction_requests SET ingestion_job_id = $1 WHERE id = $2",
			ingestionJobID,
			extractionID,
		)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx,
			"INSERT INTO extraction_requests (id, connection_id, ingestion_job_id, tables, status) VALUES (gen_random_uuid(), $1, $2, '{}'::jsonb, 'PENDING')",
			fetcherConnID,
			"00000000-0000-0000-0000-000000000999",
		)
		require.Error(t, err, "ingestion job FK must reject arbitrary UUIDs")

		_, err = db.ExecContext(ctx, "DELETE FROM fetcher_connections WHERE id = $1", fetcherConnID)
		require.Error(t, err, "extraction_requests FK must restrict parent deletion")

		cascadeConnID := mustInsertFetcherConnection(t, ctx, db, "fetcher-conn-2")
		mustInsertDiscoveredSchema(t, ctx, db, cascadeConnID, "transactions")
		_, err = db.ExecContext(ctx, "DELETE FROM fetcher_connections WHERE id = $1", cascadeConnID)
		require.NoError(t, err)

		var remaining int
		err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM discovered_schemas WHERE connection_id = $1", cascadeConnID).Scan(&remaining)
		require.NoError(t, err)
		assert.Equal(t, 0, remaining)

		_, err = db.ExecContext(ctx,
			"INSERT INTO extraction_requests (id, connection_id, tables, status) VALUES (gen_random_uuid(), $1, '{}'::jsonb, 'BROKEN')",
			fetcherConnID,
		)
		require.Error(t, err, "status CHECK constraint must reject invalid values")

		_, err = db.ExecContext(ctx,
			"INSERT INTO extraction_requests (id, connection_id, tables, status) VALUES (gen_random_uuid(), $1, '{}'::jsonb, 'SUBMITTED')",
			fetcherConnID,
		)
		require.Error(t, err, "submitted extraction must require fetcher_job_id")

		_, err = db.ExecContext(ctx,
			"INSERT INTO fetcher_connections (fetcher_conn_id, config_name, database_type, host, port, database_name, product_name, status) VALUES ($1, 'cfg', 'POSTGRESQL', 'db.internal', 70000, 'ledger', 'PostgreSQL 17', 'AVAILABLE')",
			"fetcher-conn-bad-port",
		)
		require.Error(t, err, "port CHECK constraint must reject invalid values")

		_, err = db.ExecContext(ctx,
			"INSERT INTO fetcher_connections (fetcher_conn_id, config_name, database_type, host, port, database_name, product_name, status) VALUES ($1, '', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')",
			"fetcher-conn-empty-config",
		)
		require.Error(t, err, "non-empty config_name CHECK constraint must reject blank values")

		_, err = db.ExecContext(ctx,
			"INSERT INTO fetcher_connections (fetcher_conn_id, config_name, database_type, host, port, database_name, product_name, status) VALUES ($1, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')",
			"fetcher-conn-1",
		)
		require.Error(t, err, "fetcher connection id must remain unique")

		_, err = db.ExecContext(ctx,
			"INSERT INTO discovered_schemas (connection_id, table_name, columns) VALUES ($1, 'transactions', '{}'::jsonb)",
			fetcherConnID,
		)
		require.Error(t, err, "columns CHECK constraint must require array JSON")

		mustInsertDiscoveredSchema(t, ctx, db, fetcherConnID, "transactions")
		_, err = db.ExecContext(ctx,
			"INSERT INTO discovered_schemas (connection_id, table_name, columns) VALUES ($1, 'transactions', '[]'::jsonb)",
			fetcherConnID,
		)
		require.Error(t, err, "schema uniqueness must reject duplicate table snapshot")

		_, err = db.ExecContext(ctx,
			"INSERT INTO extraction_requests (id, connection_id, tables, status) VALUES (gen_random_uuid(), $1, '[]'::jsonb, 'PENDING')",
			fetcherConnID,
		)
		require.Error(t, err, "tables CHECK constraint must require object JSON")

		_, err = db.ExecContext(ctx,
			"INSERT INTO extraction_requests (id, connection_id, tables, filters, status) VALUES (gen_random_uuid(), $1, '{}'::jsonb, '[]'::jsonb, 'PENDING')",
			fetcherConnID,
		)
		require.Error(t, err, "filters CHECK constraint must require object-or-null JSON")

		_, err = db.ExecContext(ctx, "DELETE FROM ingestion_jobs WHERE id = $1", ingestionJobID)
		require.NoError(t, err)

		var ingestionJobIDAfterDelete sql.NullString
		err = db.QueryRowContext(ctx, "SELECT ingestion_job_id FROM extraction_requests WHERE id = $1", extractionID).Scan(&ingestionJobIDAfterDelete)
		require.NoError(t, err)
		assert.False(t, ingestionJobIDAfterDelete.Valid, "ingestion job FK must null out on parent deletion")
	})

	t.Run("rolls back enum and table slice, then reapplies cleanly", func(t *testing.T) {
		migrator, err := newMigrator(db, "matcher_test", "migrations")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, closeMigrator(migrator))
		}()

		stepper, ok := migrator.(interface{ Steps(int) error })
		require.True(t, ok, "migrator must support stepping for rollback verification")

		require.NoError(t, stepper.Steps(-1))
		require.NoError(t, stepper.Steps(-1))
		assert.NotContains(t, enumLabels(t, ctx, db, "reconciliation_source_type"), "FETCHER")

		require.NoError(t, stepper.Steps(-1))
		assert.False(t, tableExists(t, ctx, db, "fetcher_connections"))
		assert.False(t, tableExists(t, ctx, db, "discovered_schemas"))
		assert.False(t, tableExists(t, ctx, db, "extraction_requests"))

		require.NoError(t, RunMigrations(ctx, dsn, "matcher_test", "migrations", logger, false))
		assert.True(t, tableExists(t, ctx, db, "fetcher_connections"))
		assert.True(t, tableExists(t, ctx, db, "discovered_schemas"))
		assert.True(t, tableExists(t, ctx, db, "extraction_requests"))
		assert.Contains(t, enumLabels(t, ctx, db, "reconciliation_source_type"), "FETCHER")
	})

	t.Run("rejects enum rollback when FETCHER sources exist", func(t *testing.T) {
		rollbackDB, err := sql.Open("pgx", dsn)
		require.NoError(t, err)
		defer rollbackDB.Close()
		require.NoError(t, rollbackDB.PingContext(ctx))

		contextID := mustInsertReconciliationContext(t, ctx, rollbackDB)
		mustInsertReconciliationSource(t, ctx, rollbackDB, contextID, uniqueName("fetcher-source"), "FETCHER")

		migrator, err := newMigrator(rollbackDB, "matcher_test", "migrations")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, closeMigrator(migrator))
		}()

		stepper, ok := migrator.(interface{ Steps(int) error })
		require.True(t, ok, "migrator must support stepping for rollback verification")

		require.NoError(t, stepper.Steps(-1))
		err = stepper.Steps(-1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "FETCHER")
	})
}

func tableExists(t *testing.T, ctx context.Context, db *sql.DB, tableName string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", tableName).Scan(&exists)
	require.NoError(t, err)

	return exists
}

func indexExists(t *testing.T, ctx context.Context, db *sql.DB, indexName string) bool {
	t.Helper()

	var exists bool
	err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = $1)", indexName).Scan(&exists)
	require.NoError(t, err)

	return exists
}

func enumLabels(t *testing.T, ctx context.Context, db *sql.DB, typeName string) []string {
	t.Helper()

	rows, err := db.QueryContext(ctx, `
		SELECT e.enumlabel
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		WHERE t.typname = $1
		ORDER BY e.enumsortorder`, typeName)
	require.NoError(t, err)
	defer rows.Close()

	labels := make([]string, 0)
	for rows.Next() {
		var label string
		require.NoError(t, rows.Scan(&label))
		labels = append(labels, label)
	}
	require.NoError(t, rows.Err())

	return labels
}

func mustInsertFetcherConnection(t *testing.T, ctx context.Context, db *sql.DB, fetcherConnID string) string {
	t.Helper()

	var id string
	err := db.QueryRowContext(ctx, `
		INSERT INTO fetcher_connections (fetcher_conn_id, config_name, database_type, host, port, database_name, product_name, status)
		VALUES ($1, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')
		RETURNING id`, fetcherConnID).Scan(&id)
	require.NoError(t, err)

	return id
}

func mustInsertDiscoveredSchema(t *testing.T, ctx context.Context, db *sql.DB, connectionID, tableName string) {
	t.Helper()

	_, err := db.ExecContext(ctx, `
		INSERT INTO discovered_schemas (connection_id, table_name, columns)
		VALUES ($1, $2, '[{"name":"id","type":"uuid","nullable":false}]'::jsonb)`, connectionID, tableName)
	require.NoError(t, err)
}

func mustInsertExtractionRequest(t *testing.T, ctx context.Context, db *sql.DB, connectionID string) string {
	t.Helper()

	var id string
	err := db.QueryRowContext(ctx, `
		INSERT INTO extraction_requests (connection_id, tables, status)
		VALUES ($1, '{}'::jsonb, 'PENDING')
		RETURNING id`, connectionID).Scan(&id)
	require.NoError(t, err)

	return id
}

func mustInsertIngestionJob(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()

	contextID := mustInsertReconciliationContext(t, ctx, db)
	sourceID := mustInsertReconciliationSource(t, ctx, db, contextID, uniqueName("source"), "LEDGER")

	var ingestionJobID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO ingestion_jobs (context_id, source_id, status)
		VALUES ($1, $2, 'QUEUED')
		RETURNING id`, contextID, sourceID).Scan(&ingestionJobID)
	require.NoError(t, err)

	return ingestionJobID
}

func mustInsertReconciliationContext(t *testing.T, ctx context.Context, db *sql.DB) string {
	t.Helper()

	var contextID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO reconciliation_contexts (tenant_id, name, type, interval)
		VALUES ('11111111-1111-1111-1111-111111111111', $1, '1:1', 'daily')
		RETURNING id`, uniqueName("ctx")).Scan(&contextID)
	require.NoError(t, err)

	return contextID
}

func mustInsertReconciliationSource(t *testing.T, ctx context.Context, db *sql.DB, contextID, name, sourceType string) string {
	t.Helper()

	var sourceID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO reconciliation_sources (context_id, name, type, config)
		VALUES ($1, $2, $3, '{}'::jsonb)
		RETURNING id`, contextID, name, sourceType).Scan(&sourceID)
	require.NoError(t, err)

	return sourceID
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
