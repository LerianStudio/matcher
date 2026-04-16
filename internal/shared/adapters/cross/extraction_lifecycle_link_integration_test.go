// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Lives in a separate package (cross_test) so importing the integration
// harness never risks a self-import. The harness owns the testcontainer
// lifecycle; this file only consumes `h.Provider()` and `h.Ctx()`.
package cross_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	extractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryEntities "github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	discoveryVO "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	cross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_ExtractionLifecycleLink_HappyPath verifies Scenario 2 at
// integration scope: seeding a real extraction_requests row with
// ingestion_job_id=NULL, then invoking the adapter against the real
// `*discoveryExtractionRepo.Repository` must persist the linkage so a
// subsequent FindByID returns the supplied ingestion job id.
func TestIntegration_ExtractionLifecycleLink_HappyPath(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		repo := extractionRepo.NewRepository(provider)

		adapter, err := cross.NewExtractionLifecycleLinkWriterAdapter(repo)
		require.NoError(t, err)

		tenantCtx := h.Ctx()
		connectionID := insertFetcherConnection(t, tenantCtx, h, "link-happy-path")

		extraction, err := discoveryEntities.NewExtractionRequest(
			tenantCtx,
			connectionID,
			map[string]any{"transactions": []any{}},
			"",
			"",
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, discoveryVO.ExtractionStatusPending, extraction.Status)
		require.Equal(t, uuid.Nil, extraction.IngestionJobID, "seed row must start unlinked")

		require.NoError(t, repo.Create(tenantCtx, extraction))

		ingestionJobID := insertIngestionJob(t, tenantCtx, h)
		require.NoError(t, adapter.LinkExtractionToIngestion(tenantCtx, extraction.ID, ingestionJobID))

		reloaded, err := repo.FindByID(tenantCtx, extraction.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		require.Equal(t, ingestionJobID, reloaded.IngestionJobID,
			"persisted ingestion_job_id must match the value passed to the adapter")
	})
}

// TestIntegration_ExtractionLifecycleLink_Idempotency proves that a second
// LinkExtractionToIngestion call against the same extraction returns the
// ErrExtractionAlreadyLinked sentinel and does NOT overwrite the existing
// ingestion_job_id. This is the idempotency invariant the adapter enforces
// to protect extractions from being re-associated by replay traffic.
func TestIntegration_ExtractionLifecycleLink_Idempotency(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		provider := h.Provider()
		repo := extractionRepo.NewRepository(provider)

		adapter, err := cross.NewExtractionLifecycleLinkWriterAdapter(repo)
		require.NoError(t, err)

		tenantCtx := h.Ctx()
		connectionID := insertFetcherConnection(t, tenantCtx, h, "link-idempotency")

		extraction, err := discoveryEntities.NewExtractionRequest(
			tenantCtx,
			connectionID,
			map[string]any{"transactions": []any{}},
			"",
			"",
			nil,
		)
		require.NoError(t, err)
		require.NoError(t, repo.Create(tenantCtx, extraction))

		firstIngestionJobID := insertIngestionJob(t, tenantCtx, h)
		require.NoError(t, adapter.LinkExtractionToIngestion(tenantCtx, extraction.ID, firstIngestionJobID))

		// Second call with a different ingestion job id must be rejected and
		// leave the persisted value unchanged. Seed a second row so the FK
		// still resolves even though the idempotency guard will reject the write.
		secondIngestionJobID := insertIngestionJob(t, tenantCtx, h)
		require.NotEqual(t, firstIngestionJobID, secondIngestionJobID)

		err = adapter.LinkExtractionToIngestion(tenantCtx, extraction.ID, secondIngestionJobID)
		require.Error(t, err)
		require.ErrorIs(t, err, sharedPorts.ErrExtractionAlreadyLinked)

		reloaded, err := repo.FindByID(tenantCtx, extraction.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		require.Equal(t, firstIngestionJobID, reloaded.IngestionJobID,
			"idempotency conflict must preserve the original linkage")
	})
}

// insertFetcherConnection seeds the parent row required by the extraction
// FK. It uses the primary DB handle from the harness connection resolver,
// matching the pattern used by the existing migrations_integration_test.go.
// The caller-supplied suffix disambiguates rows across tests so the NOT NULL
// unique fetcher_conn_id constraint stays clean across parallel runs.
func insertFetcherConnection(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	suffix string,
) uuid.UUID {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)

	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries, "harness must expose at least one primary DB")

	primary := primaries[0]
	require.NotNil(t, primary)

	connectionID, err := seedFetcherConnectionRow(ctx, primary, suffix)
	require.NoError(t, err)

	// Row cleanup runs during harness teardown via resetSharedDatabase; we do
	// not add a t.Cleanup hook here to avoid racing the shared-harness reset.
	return connectionID
}

// seedFetcherConnectionRow inserts a minimal fetcher_connections record and
// returns the generated id. Column choices mirror migrations_integration_test.go
// exactly so both test suites exercise the same CHECK constraints.
func seedFetcherConnectionRow(ctx context.Context, db *sql.DB, suffix string) (uuid.UUID, error) {
	var id uuid.UUID
	if err := db.QueryRowContext(ctx, `
		INSERT INTO fetcher_connections (
			fetcher_conn_id, config_name, database_type, host, port,
			database_name, product_name, status
		)
		VALUES ($1, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')
		RETURNING id
	`, suffix+"-"+uuid.New().String()).Scan(&id); err != nil {
		return uuid.Nil, err
	}

	return id, nil
}

// insertIngestionJob seeds a minimal ingestion_jobs row so the
// extraction_requests.ingestion_job_id FK resolves when the adapter writes
// the linkage. Uses the harness seed context_id/source_id (both NOT NULL and
// FK-constrained) — the shared harness truncates all tables before each test,
// so row cleanup is handled by the harness and no t.Cleanup is needed.
// Mirrors the INSERT shape used by tests/chaos/business_chaos_test.go.
func insertIngestionJob(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
) uuid.UUID {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)

	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries, "harness must expose at least one primary DB")

	primary := primaries[0]
	require.NotNil(t, primary)

	jobID := uuid.New()
	_, err = primary.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, context_id, source_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'COMPLETED', NOW(), NOW())
	`, jobID, h.Seed.ContextID, h.Seed.SourceID)
	require.NoError(t, err)

	return jobID
}
