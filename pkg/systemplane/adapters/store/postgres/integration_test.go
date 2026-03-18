// Copyright 2025 Lerian Studio.

//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // pgx driver registration

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/LerianStudio/matcher/pkg/systemplane/adapters/store/storetest"
	"github.com/LerianStudio/matcher/pkg/systemplane/bootstrap"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// newPostgresFactory builds a storetest.Factory that creates isolated
// Store + HistoryStore instances backed by a real PostgreSQL container.
// Each sub-test receives its own schema, preventing cross-test pollution.
func newPostgresFactory(dsn string) storetest.Factory {
	return func(t *testing.T) (ports.Store, ports.HistoryStore, func()) {
		t.Helper()

		// Use a unique schema per sub-test to guarantee full isolation.
		schema := fmt.Sprintf("test_%d", time.Now().UnixNano())

		cfg := &bootstrap.PostgresBootstrapConfig{
			DSN:           dsn,
			Schema:        schema,
			EntriesTable:  "runtime_entries",
			HistoryTable:  "runtime_history",
			NotifyChannel: "test_changes",
		}

		store, history, closer, err := New(context.Background(), cfg)
		require.NoError(t, err)

		cleanup := func() {
			closer.Close()

			// Drop the per-test schema so we don't leak artefacts.
			db, openErr := sql.Open("pgx", dsn)
			if openErr != nil {
				return
			}
			defer db.Close()

			//nolint:gocritic // dropping test schemas is intentional; no injection risk
			db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", schema))
		}

		return store, history, cleanup
	}
}

// TestPostgresStoreContracts runs the full contract test suite against a real
// PostgreSQL 17 instance managed by testcontainers. The wait strategy uses
// both ForLog (with occurrence=2, because PG logs the "ready" message during
// initdb and again after the server starts) and ForListeningPort to avoid
// the flaky port-mapping race that ForLog alone is susceptible to.
func TestPostgresStoreContracts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	container, err := tcpostgres.Run(ctx,
		"postgres:17-alpine",
		tcpostgres.WithDatabase("systemplane_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithStartupTimeout(90*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(context.Background()))
	})

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	// Run the full contract suite against the live database.
	factory := newPostgresFactory(dsn)
	storetest.RunAll(t, factory)
}
