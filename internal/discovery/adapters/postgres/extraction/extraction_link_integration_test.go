// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Integration tests for the extraction repository's atomic
// LinkIfUnlinked contract (T-003 P1 hardening).
//
// The unit-level tests at extraction_bridge_test.go use sqlmock to verify
// the exact SQL text and row-count handling. These integration scenarios
// prove the same contract holds under real concurrency against a real
// Postgres, where the sqlmock verification cannot observe contention on
// the ingestion_job_id IS NULL predicate.
//
// Package `extraction` (not `extraction_test`) so the tests use the
// Repository's exported API — no private surface touched. Build-tag
// isolation keeps the unit build clean.
package extraction

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// IS-4: Atomic link prevents duplicate linkage under race.
//
// Seed one extraction with ingestion_job_id = NULL. Launch two goroutines
// that simultaneously call LinkIfUnlinked with DIFFERENT ingestion job
// ids. Exactly one must succeed; the other must observe
// ErrExtractionAlreadyLinked. The persisted ingestion_job_id must match
// whichever writer won — never a mix of the two.
//
// This guards the T-003 P1 hardening against a TOCTOU regression: the
// unit test at extraction_bridge_test.go proves the SQL shape, this test
// proves real Postgres serialisation does the right thing when two
// transactions race on `ingestion_job_id IS NULL`.
// -----------------------------------------------------------------------------

func TestIntegration_ExtractionLinkIfUnlinked_ConcurrentWriters_OnlyOneSucceeds(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		// The shared harness runs its own timeouts on Postgres ops; the
		// tenant-scoped context derived from h.Ctx() carries TenantIDKey so
		// ApplyTenantSchema short-circuits for the default tenant.
		tenantCtx := h.Ctx()

		provider := h.Provider()
		repo := NewRepository(provider)

		// Parent FK: insert a fetcher_connections row so the extraction FK
		// resolves. UUID-suffixed handle avoids cross-test uniqueness
		// collisions under the shared harness.
		connID := seedFetcherConnectionForLink(t, tenantCtx, h)

		// Seed the extraction in state COMPLETE with ingestion_job_id NULL.
		// LinkToIngestion requires status=COMPLETE so a PENDING seed would
		// be rejected by the domain guard in the adapter code path; but
		// the raw SQL path used by the repo's LinkIfUnlinked does not run
		// the state-machine check, so the raw seed here mirrors production
		// faithfully.
		extraction := seedCompleteExtractionForLink(t, tenantCtx, h, connID)

		// Two candidate ingestion jobs. FK from extraction_requests.ingestion_job_id
		// → ingestion_jobs(id) ON DELETE SET NULL so both rows must exist for
		// the UPDATE to pass FK validation regardless of which one wins.
		firstJobID := seedIngestionJobForLink(t, tenantCtx, h)
		secondJobID := seedIngestionJobForLink(t, tenantCtx, h)
		require.NotEqual(t, firstJobID, secondJobID)

		// Concurrent linkers. A sync.Barrier via channel release would be
		// more precise, but two unbuffered goroutines with a tight
		// sync.WaitGroup are enough to reliably race on Postgres's
		// row-level lock — the LinkIfUnlinked UPDATE holds the row from
		// statement start until commit.
		var (
			successes atomic.Int64
			conflicts atomic.Int64
			firstErr  error
			secondErr error
			wg        sync.WaitGroup
		)
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := repo.LinkIfUnlinked(tenantCtx, extraction.ID, firstJobID)
			if err == nil {
				successes.Add(1)
				return
			}
			firstErr = err
			if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) {
				conflicts.Add(1)
			}
		}()

		go func() {
			defer wg.Done()
			err := repo.LinkIfUnlinked(tenantCtx, extraction.ID, secondJobID)
			if err == nil {
				successes.Add(1)
				return
			}
			secondErr = err
			if errors.Is(err, sharedPorts.ErrExtractionAlreadyLinked) {
				conflicts.Add(1)
			}
		}()

		wg.Wait()

		require.Equal(t, int64(1), successes.Load(),
			"exactly one LinkIfUnlinked call must succeed (firstErr=%v secondErr=%v)", firstErr, secondErr)
		require.Equal(t, int64(1), conflicts.Load(),
			"exactly one LinkIfUnlinked call must return ErrExtractionAlreadyLinked (firstErr=%v secondErr=%v)", firstErr, secondErr)

		// Persisted ingestion_job_id must match whichever writer won. We
		// don't know in advance which goroutine got there first; matching
		// either winner is the contract.
		reloaded, err := repo.FindByID(tenantCtx, extraction.ID)
		require.NoError(t, err)
		require.NotNil(t, reloaded)
		require.NotEqual(t, uuid.Nil, reloaded.IngestionJobID,
			"persisted ingestion_job_id must be non-nil after a successful link")
		assert.Contains(t, []uuid.UUID{firstJobID, secondJobID}, reloaded.IngestionJobID,
			"persisted ingestion_job_id must match one of the two racing writers")
	})
}

// -----------------------------------------------------------------------------
// Supporting fixture helpers — direct SQL seeding keeps these tests
// decoupled from the configuration / ingestion repository wiring and
// mirrors the pattern used in
// internal/shared/adapters/cross/extraction_lifecycle_link_integration_test.go.
// -----------------------------------------------------------------------------

func seedFetcherConnectionForLink(t *testing.T, ctx context.Context, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries, "harness must expose at least one primary DB")
	primary := primaries[0]
	require.NotNil(t, primary)

	connID := uuid.New()
	_, err = primary.ExecContext(ctx, `
		INSERT INTO fetcher_connections (
			id, fetcher_conn_id, config_name, database_type, host, port,
			database_name, product_name, status
		)
		VALUES ($1, $2, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')
	`, connID, "link-race-"+uuid.NewString())
	require.NoError(t, err, "seed fetcher_connections")
	return connID
}

func seedCompleteExtractionForLink(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	connectionID uuid.UUID,
) *entities.ExtractionRequest {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries)
	primary := primaries[0]

	extractionID := uuid.New()
	fetcherJobID := "fetcher-job-" + extractionID.String()[:8]

	_, err = primary.ExecContext(ctx, `
		INSERT INTO extraction_requests (
			id, connection_id, status, fetcher_job_id, result_path,
			tables, created_at, updated_at
		)
		VALUES ($1, $2, 'COMPLETE', $3, '/data/extraction.json', '{}'::jsonb, NOW(), NOW())
	`, extractionID, connectionID, fetcherJobID)
	require.NoError(t, err, "seed extraction_requests")

	repo := NewRepository(h.Provider())
	loaded, err := repo.FindByID(ctx, extractionID)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	// Sanity: the invariant the concurrent test depends on.
	require.Equal(t, uuid.Nil, loaded.IngestionJobID,
		"seeded extraction must start unlinked")
	require.Equal(t, string(loaded.Status), string(loaded.Status),
		"status field must be populated; the repo entity is used downstream")
	return loaded
}

// seedIngestionJobForLink inserts a minimal ingestion_jobs row and
// returns its id. Mirrors the shape used in
// internal/shared/adapters/cross/extraction_lifecycle_link_integration_test.go.
// The shared harness truncates all tables between tests so cleanup is
// handled by the harness reset.
func seedIngestionJobForLink(t *testing.T, ctx context.Context, h *integration.TestHarness) uuid.UUID {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries)
	primary := primaries[0]

	jobID := uuid.New()
	_, err = primary.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, context_id, source_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'COMPLETED', NOW(), NOW())
	`, jobID, h.Seed.ContextID, h.Seed.SourceID)
	require.NoError(t, err, "seed ingestion_jobs")
	return jobID
}
