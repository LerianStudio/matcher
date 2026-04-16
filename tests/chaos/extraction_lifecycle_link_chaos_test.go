//go:build chaos

// Chaos tests for the T-001 extraction-lifecycle linkage seam. These
// mirror the integration tests in
// internal/shared/adapters/cross/extraction_lifecycle_link_integration_test.go
// and drive the same real ExtractionRepository through the Toxiproxy-fronted
// PostgreSQL connection so latency can be injected mid-link.
//
// File placement: the Gate 7 brief proposed
// internal/shared/adapters/cross/extraction_lifecycle_link_chaos_test.go.
// Same rationale as trusted_stream_chaos_test.go: keep all chaos tests
// sharing the harness in tests/chaos/.

package chaos

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	extractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryEntities "github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	cross "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
)

// --------------------------------------------------------------------------
// CHAOS-T001-B: PostgreSQL latency during ExtractionLifecycleLink
// --------------------------------------------------------------------------

// TestIntegration_Chaos_ExtractionLifecycleLink_LatencyDeadlineExceeded
// covers Gate 7 Scenario 2: when PostgreSQL latency exceeds the caller's
// context deadline, LinkExtractionToIngestion must return a wrapped
// deadline-exceeded error and leave ingestion_job_id NULL on the
// extraction_requests row — the one-extraction-to-one-ingestion invariant
// must hold even when the link call times out partway through.
//
// Phases:
//  1. Normal — seed a real extraction_requests row with ingestion_job_id = NULL.
//  2. Inject — 2-second PG latency toxic.
//  3. Verify — link call with 1-second context deadline fails cleanly
//     (context.DeadlineExceeded reachable via errors.Is); DB row unchanged.
//  4. Restore — remove toxics.
//  5. Recovery — fresh link call without the toxic succeeds and persists
//     the ingestion_job_id.
func TestIntegration_Chaos_ExtractionLifecycleLink_LatencyDeadlineExceeded(t *testing.T) {
	requireChaosEnabled(t)

	h := GetSharedChaos()
	require.NotNil(t, h, "chaos harness not initialized")
	h.ResetDatabase(t)

	directDB := h.DirectDB(t)

	// --- Phase 1: Normal -------------------------------------------------
	// Seed via the direct (un-proxied) DB so setup is immune to later
	// injection. The FK into fetcher_connections must be satisfied first.
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer setupCancel()

	connectionID := seedFetcherConnectionForChaos(t, setupCtx, directDB, "chaos-link")
	extractionID := seedUnlinkedExtractionForChaos(t, setupCtx, directDB, connectionID)

	repo := extractionRepo.NewRepository(h.Provider())
	adapter, err := cross.NewExtractionLifecycleLinkWriterAdapter(repo)
	require.NoError(t, err, "build extraction lifecycle link adapter")

	// Baseline sanity: the adapter can read the seed row through the proxy
	// before any chaos is injected. (We use a disposable second extraction
	// for this, then a fresh one for the chaos phase; seeding both up front
	// keeps Phase 3's timing tight and avoids setup noise under latency.)
	baselineExtractionID := seedUnlinkedExtractionForChaos(t, setupCtx, directDB, connectionID)

	// The adapter writes extraction_requests.ingestion_job_id, which has a FK
	// to ingestion_jobs(id). Seed a real ingestion_jobs row up front via the
	// direct (un-proxied) DB so the FK resolves when the baseline adapter call
	// runs. Mirrors insertIngestionJob() in
	// internal/shared/adapters/cross/extraction_lifecycle_link_integration_test.go.
	baselineIngestionJobID := seedIngestionJobForChaos(t, setupCtx, directDB, h.Seed.ContextID, h.Seed.SourceID)

	baselineCtx, baselineCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer baselineCancel()
	require.NoError(t, adapter.LinkExtractionToIngestion(
		baselineCtx, baselineExtractionID, baselineIngestionJobID,
	), "baseline link must succeed before chaos injection")

	// Seed the FK target for the Phase 3 chaos call BEFORE injecting latency,
	// using the direct DB so setup is immune to the latency we are about to add.
	// Without this, the chaos-phase UPDATE could fail with a FK-violation sentinel
	// instead of the deadline-exceeded error the test is trying to assert.
	intendedIngestionID := seedIngestionJobForChaos(t, setupCtx, directDB, h.Seed.ContextID, h.Seed.SourceID)

	// --- Phase 2: Inject -------------------------------------------------
	// 2 seconds on every PG response. Every single SELECT/UPDATE that
	// LinkExtractionToIngestion issues will slip past the caller's deadline.
	h.InjectPGLatency(t, 2000, 0)

	// --- Phase 3: Verify -------------------------------------------------
	chaosCtx, chaosCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer chaosCancel()

	start := time.Now()
	linkErr := adapter.LinkExtractionToIngestion(chaosCtx, extractionID, intendedIngestionID)
	elapsed := time.Since(start)

	require.Error(t, linkErr, "link call must fail when PG latency exceeds deadline")
	// The wrapped error must surface DeadlineExceeded so upstream callers
	// (Fetcher bridge, retry wrappers) can errors.Is it. PROJECT_RULES §8
	// requires %w wrapping; this is where we verify that in practice.
	assert.True(t,
		errors.Is(linkErr, context.DeadlineExceeded) || errors.Is(linkErr, context.Canceled),
		"wrapped error must expose context.DeadlineExceeded/Canceled (got %v)", linkErr,
	)
	// Belt-and-braces: respect the caller's deadline rather than hanging
	// for the full latency budget. The client library aborts on deadline;
	// the 2s latency would only surface if the context was ignored.
	assert.Less(t, elapsed, 3*time.Second,
		"link must respect context deadline (%v elapsed) rather than wait for full PG latency", elapsed)

	// The DB row must still report ingestion_job_id = NULL. Direct DB
	// bypasses the poisoned proxy so the assertion is reliable even under
	// ongoing latency injection.
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()

	var linked sql.NullString
	require.NoError(t, directDB.QueryRowContext(verifyCtx, `
		SELECT ingestion_job_id FROM extraction_requests WHERE id = $1
	`, extractionID).Scan(&linked), "read extraction row directly")
	assert.False(t, linked.Valid,
		"extraction_requests.ingestion_job_id must remain NULL after a deadline-aborted link (got %q)", linked.String)

	// --- Phase 4: Restore ------------------------------------------------
	h.RemoveAllToxics(t)

	// --- Phase 5: Recovery -----------------------------------------------
	// Under the cleared proxy the link must succeed and the row must carry
	// the ingestion_job_id supplied on the recovery call. Use a fresh ID so
	// the idempotency sentinel (ErrExtractionAlreadyLinked) cannot fire —
	// the Phase 3 write was rolled back so the record is still unlinked.
	// The FK into ingestion_jobs still needs a real parent row; seed one now.
	recoveryIngestionID := seedIngestionJobForChaos(t, setupCtx, directDB, h.Seed.ContextID, h.Seed.SourceID)
	require.NotEqual(t, intendedIngestionID, recoveryIngestionID)

	// Eventually tolerates any brief pool-replacement window right after
	// toxic removal — matching the pattern used by existing chaos tests.
	require.Eventually(t, func() bool {
		rCtx, rCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer rCancel()
		return adapter.LinkExtractionToIngestion(rCtx, extractionID, recoveryIngestionID) == nil
	}, 15*time.Second, 500*time.Millisecond,
		"link must succeed after PG latency toxic is cleared")

	// Confirm the persisted ingestion_job_id matches what Phase 5 supplied.
	var persisted uuid.UUID
	require.NoError(t, directDB.QueryRowContext(verifyCtx, `
		SELECT ingestion_job_id FROM extraction_requests WHERE id = $1
	`, extractionID).Scan(&persisted), "read extraction row after recovery")
	assert.Equal(t, recoveryIngestionID, persisted,
		"recovery link must persist the recovery ingestion_job_id")

	AssertNoDataCorruption(t, directDB)
}

// --------------------------------------------------------------------------
// Seeding helpers (scoped to chaos tests)
// --------------------------------------------------------------------------

// seedFetcherConnectionForChaos inserts a minimal fetcher_connections row
// directly (bypassing the proxy). It returns the generated id. The column
// list mirrors the integration-test seed helper in
// extraction_lifecycle_link_integration_test.go so both suites exercise
// the same CHECK constraints.
func seedFetcherConnectionForChaos(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	suffix string,
) uuid.UUID {
	t.Helper()

	var id uuid.UUID
	err := db.QueryRowContext(ctx, `
		INSERT INTO fetcher_connections (
			fetcher_conn_id, config_name, database_type, host, port,
			database_name, product_name, status
		)
		VALUES ($1, 'cfg', 'POSTGRESQL', 'db.internal', 5432, 'ledger', 'PostgreSQL 17', 'AVAILABLE')
		RETURNING id
	`, suffix+"-"+uuid.New().String()).Scan(&id)
	require.NoError(t, err, "seed fetcher_connections row")
	return id
}

// seedUnlinkedExtractionForChaos inserts a real extraction_requests row
// with ingestion_job_id = NULL via the extraction repository's own Create
// path, then returns the generated id. Using NewExtractionRequest ensures
// the seed respects the entity invariants the adapter reads back.
func seedUnlinkedExtractionForChaos(
	t *testing.T,
	ctx context.Context,
	_ *sql.DB,
	connectionID uuid.UUID,
) uuid.UUID {
	t.Helper()

	extraction, err := discoveryEntities.NewExtractionRequest(
		ctx,
		connectionID,
		map[string]any{"transactions": []any{}},
		"",
		"",
		nil,
	)
	require.NoError(t, err, "construct ExtractionRequest entity")
	require.Equal(t, uuid.Nil, extraction.IngestionJobID,
		"seed row must start unlinked; test precondition")

	// Use the repo bound to the chaos harness's proxied PG so the write is
	// visible to the adapter under test. The write itself happens before
	// any chaos injection and is expected to succeed.
	repo := extractionRepo.NewRepository(GetSharedChaos().Provider())
	require.NoError(t, repo.Create(ctx, extraction), "create unlinked extraction")

	return extraction.ID
}

// seedIngestionJobForChaos inserts a minimal ingestion_jobs row directly
// (bypassing the proxy) so the extraction_requests.ingestion_job_id FK
// resolves when the adapter writes the linkage. Mirrors insertIngestionJob()
// in extraction_lifecycle_link_integration_test.go — same column shape and
// NOT NULL context_id/source_id values sourced from the harness seed so the
// parent FKs into contexts/sources are satisfied.
func seedIngestionJobForChaos(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	contextID uuid.UUID,
	sourceID uuid.UUID,
) uuid.UUID {
	t.Helper()

	jobID := uuid.New()
	_, err := db.ExecContext(ctx, `
		INSERT INTO ingestion_jobs (id, context_id, source_id, status, created_at, updated_at)
		VALUES ($1, $2, $3, 'COMPLETED', NOW(), NOW())
	`, jobID, contextID, sourceID)
	require.NoError(t, err, "seed ingestion_jobs row")
	return jobID
}
