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

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
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

	var contextID string
	err := db.QueryRowContext(ctx, `
		INSERT INTO reconciliation_contexts (tenant_id, name, type, interval)
		VALUES ('11111111-1111-1111-1111-111111111111', $1, '1:1', 'daily')
		RETURNING id`, uniqueName("ctx")).Scan(&contextID)
	require.NoError(t, err)

	var sourceID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO reconciliation_sources (context_id, name, type, config)
		VALUES ($1, $2, 'LEDGER', '{}'::jsonb)
		RETURNING id`, contextID, uniqueName("source")).Scan(&sourceID)
	require.NoError(t, err)

	var ingestionJobID string
	err = db.QueryRowContext(ctx, `
		INSERT INTO ingestion_jobs (context_id, source_id, status)
		VALUES ($1, $2, 'QUEUED')
		RETURNING id`, contextID, sourceID).Scan(&ingestionJobID)
	require.NoError(t, err)

	return ingestionJobID
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
