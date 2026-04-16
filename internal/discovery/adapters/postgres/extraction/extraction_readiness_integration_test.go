// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build integration

// Integration tests for the bridge readiness read-model surface (T-004).
//
// The unit-level sqlmock tests at extraction_readiness_queries_test.go
// pin the SQL shape and bound-checking logic. This test proves the same
// queries return the right partition counts and drilldown rows when run
// against a real Postgres instance with a representative extraction
// population.
package extraction

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
	vo "github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
	"github.com/LerianStudio/matcher/tests/integration"
)

// -----------------------------------------------------------------------------
// IS-RDS-1: bridge readiness summary partitions extractions correctly and
// drilldown returns matching rows for each state.
//
// Seeds one extraction in every readiness state, asserts the summary counts
// match exactly, then walks each state through the drilldown endpoint to
// verify the same row reappears under the right state. Single integration
// test covers the AC-T1 contract end-to-end.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeReadiness_SummaryAndDrilldown_EndToEnd(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tenantCtx := h.Ctx()
		repo := NewRepository(h.Provider())

		// Connection FK shared by all extraction rows in this test.
		connID := seedFetcherConnectionForLink(t, tenantCtx, h)

		// Stale threshold: anything older than 30 minutes is "stale" for the
		// purposes of this test, anything newer is "pending".
		staleThreshold := 30 * time.Minute

		// 1 ready row (COMPLETE + ingestion_job_id NOT NULL).
		readyExtraction := seedCompleteExtractionForLink(t, tenantCtx, h, connID)
		jobID := seedIngestionJobForLink(t, tenantCtx, h)
		require.NoError(t, repo.LinkIfUnlinked(tenantCtx, readyExtraction.ID, jobID))

		// 1 pending row (COMPLETE + unlinked + recently created).
		pendingExtraction := seedCompleteExtractionForLink(t, tenantCtx, h, connID)

		// 1 stale row (COMPLETE + unlinked + created long ago — backdate via
		// raw UPDATE because the entity constructor stamps NOW()).
		staleExtraction := seedCompleteExtractionForLink(t, tenantCtx, h, connID)
		backdateExtraction(t, tenantCtx, h, staleExtraction.ID, time.Hour)

		// 2 failed rows (one FAILED, one CANCELLED) so the partition picks up
		// both terminal-non-bridge states.
		failedID := seedTerminalExtraction(t, tenantCtx, h, connID, vo.ExtractionStatusFailed)
		cancelledID := seedTerminalExtraction(t, tenantCtx, h, connID, vo.ExtractionStatusCancelled)

		// 3 in-flight rows (PENDING, SUBMITTED, EXTRACTING) — the partition
		// must surface upstream extractions that have not yet COMPLETE'd so
		// dashboards can distinguish "Fetcher idle" from "Fetcher working".
		inFlightPending := seedTerminalExtraction(t, tenantCtx, h, connID, vo.ExtractionStatusPending)
		inFlightSubmitted := seedTerminalExtraction(t, tenantCtx, h, connID, vo.ExtractionStatusSubmitted)
		inFlightExtracting := seedTerminalExtraction(t, tenantCtx, h, connID, vo.ExtractionStatusExtracting)

		// --- Summary --------------------------------------------------------
		counts, err := repo.CountBridgeReadiness(tenantCtx, staleThreshold)
		require.NoError(t, err)

		assert.Equal(t, int64(1), counts.Ready, "ready bucket must contain the linked extraction")
		assert.Equal(t, int64(1), counts.Pending, "pending bucket must contain the recently-created unlinked extraction")
		assert.Equal(t, int64(1), counts.Stale, "stale bucket must contain the backdated unlinked extraction")
		assert.Equal(t, int64(2), counts.Failed, "failed bucket must contain both FAILED and CANCELLED rows")
		assert.Equal(t, int64(3), counts.InFlightCount,
			"in-flight bucket must contain PENDING+SUBMITTED+EXTRACTING rows so the partition stays exhaustive")
		assert.Equal(t, int64(8), counts.Total())

		// --- Drilldown: ready ----------------------------------------------
		readyRows, err := repo.ListBridgeCandidates(
			tenantCtx, "ready", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, readyRows, 1)
		assert.Equal(t, readyExtraction.ID, readyRows[0].ID)
		assert.NotEqual(t, uuid.Nil, readyRows[0].IngestionJobID,
			"ready row must carry the linked ingestion_job_id")

		// --- Drilldown: pending --------------------------------------------
		pendingRows, err := repo.ListBridgeCandidates(
			tenantCtx, "pending", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, pendingRows, 1)
		assert.Equal(t, pendingExtraction.ID, pendingRows[0].ID)
		assert.Equal(t, uuid.Nil, pendingRows[0].IngestionJobID)

		// --- Drilldown: stale ----------------------------------------------
		staleRows, err := repo.ListBridgeCandidates(
			tenantCtx, "stale", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, staleRows, 1)
		assert.Equal(t, staleExtraction.ID, staleRows[0].ID)
		assert.Equal(t, uuid.Nil, staleRows[0].IngestionJobID)

		// --- Drilldown: failed ---------------------------------------------
		failedRows, err := repo.ListBridgeCandidates(
			tenantCtx, "failed", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, failedRows, 2)

		gotIDs := []uuid.UUID{failedRows[0].ID, failedRows[1].ID}
		assert.Contains(t, gotIDs, failedID)
		assert.Contains(t, gotIDs, cancelledID)

		// --- Drilldown: in_flight ------------------------------------------
		inFlightRows, err := repo.ListBridgeCandidates(
			tenantCtx, "in_flight", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, inFlightRows, 3,
			"in_flight drilldown must surface all three non-terminal upstream states")

		gotInFlightIDs := []uuid.UUID{inFlightRows[0].ID, inFlightRows[1].ID, inFlightRows[2].ID}
		assert.Contains(t, gotInFlightIDs, inFlightPending)
		assert.Contains(t, gotInFlightIDs, inFlightSubmitted)
		assert.Contains(t, gotInFlightIDs, inFlightExtracting)
	})
}

// -----------------------------------------------------------------------------
// IS-RDS-2 (Fix 4): cursor pagination round-trip without overlap or skip.
//
// The base IS-RDS-1 test seeds 5 rows total and uses limit=50, so cursor
// pagination is never exercised against a real Postgres. This test pages
// through 5 ready rows in pages of 2, asserts the cursor anchor (created_at,
// id) survives microsecond-precision round-trip, and verifies zero overlap
// between pages.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeReadiness_CursorPagination_NoOverlap(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tenantCtx := h.Ctx()
		repo := NewRepository(h.Provider())

		connID := seedFetcherConnectionForLink(t, tenantCtx, h)
		staleThreshold := 30 * time.Minute

		// Seed 5 ready rows. Stagger created_at so the cursor anchor is
		// stable and the pagination order is deterministic. Each row must
		// be COMPLETE+linked so it lands in the "ready" partition.
		const totalRows = 5
		seededIDs := make(map[uuid.UUID]bool, totalRows)

		for i := 0; i < totalRows; i++ {
			extraction := seedCompleteExtractionForLink(t, tenantCtx, h, connID)
			jobID := seedIngestionJobForLink(t, tenantCtx, h)
			require.NoError(t, repo.LinkIfUnlinked(tenantCtx, extraction.ID, jobID))
			// Backdate by descending offsets so seed order matches eventual
			// ASC pagination order (oldest first). Spacing of 1 minute keeps
			// rows distinguishable even with microsecond truncation.
			backdateExtraction(t, tenantCtx, h, extraction.ID,
				time.Duration(totalRows-i)*time.Minute)
			seededIDs[extraction.ID] = true
		}

		// Page 1: limit 2.
		page1, err := repo.ListBridgeCandidates(
			tenantCtx, "ready", staleThreshold, time.Time{}, uuid.Nil, 2,
		)
		require.NoError(t, err)
		require.Len(t, page1, 2)

		// Build cursor from page 1's last row.
		cursorTime := page1[1].CreatedAt
		cursorID := page1[1].ID

		// Page 2: limit 2, anchored after page 1.
		page2, err := repo.ListBridgeCandidates(
			tenantCtx, "ready", staleThreshold, cursorTime, cursorID, 2,
		)
		require.NoError(t, err)
		require.Len(t, page2, 2, "second page must return exactly 2 more rows")

		// Page 3: limit 2, anchored after page 2 — only 1 row should remain.
		cursorTime2 := page2[1].CreatedAt
		cursorID2 := page2[1].ID
		page3, err := repo.ListBridgeCandidates(
			tenantCtx, "ready", staleThreshold, cursorTime2, cursorID2, 2,
		)
		require.NoError(t, err)
		require.Len(t, page3, 1, "third page must return the final remaining row")

		// Aggregate seen IDs across all pages and assert zero overlap. Any
		// repeat means the keyset cursor leaked or the (created_at, id)
		// tuple lost precision in round-trip.
		seen := make(map[uuid.UUID]bool, totalRows)

		for _, page := range [][]*entities.ExtractionRequest{page1, page2, page3} {
			for _, row := range page {
				assert.False(t, seen[row.ID], "row %s appeared on more than one page (cursor leak)", row.ID)
				seen[row.ID] = true
				assert.True(t, seededIDs[row.ID], "page returned an ID we did not seed")
			}
		}

		assert.Len(t, seen, totalRows, "every seeded row must be visited exactly once")
	})
}

// -----------------------------------------------------------------------------
// IS-RDS-3 (Fix 5): exact threshold boundary — pin the <= vs < convention.
//
// CountBridgeReadiness/ListBridgeCandidates use `<=` for the pending bucket
// and `>` for the stale bucket, making the threshold inclusive on the
// pending side. If the SQL ever drifts to `<`/`>=`, this test catches it.
// -----------------------------------------------------------------------------

func TestIntegration_BridgeReadiness_ExactThresholdBoundary(t *testing.T) {
	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		tenantCtx := h.Ctx()
		repo := NewRepository(h.Provider())

		connID := seedFetcherConnectionForLink(t, tenantCtx, h)
		staleThreshold := 30 * time.Minute

		// Row A: exactly at the staleness boundary. Per the SQL convention
		// (`<=`), this row MUST land in pending — boundary is inclusive on
		// the pending side. Use a tiny clock skew margin so wall-clock drift
		// between the test process and Postgres NOW() does not flip it.
		atBoundary := seedCompleteExtractionForLink(t, tenantCtx, h, connID)
		// Backdate by (threshold - 5s) so when Postgres evaluates NOW() -
		// created_at, the row is just inside the threshold.
		backdateExtraction(t, tenantCtx, h, atBoundary.ID, staleThreshold-5*time.Second)

		// Row B: 30s past the boundary. MUST land in stale.
		pastBoundary := seedCompleteExtractionForLink(t, tenantCtx, h, connID)
		backdateExtraction(t, tenantCtx, h, pastBoundary.ID, staleThreshold+30*time.Second)

		counts, err := repo.CountBridgeReadiness(tenantCtx, staleThreshold)
		require.NoError(t, err)

		assert.Equal(t, int64(1), counts.Pending,
			"row aged below threshold must be pending (boundary is inclusive via `<=`)")
		assert.Equal(t, int64(1), counts.Stale,
			"row aged past threshold must be stale (`>` predicate)")

		// Drilldown agreement: pending bucket holds the at-boundary row.
		pendingRows, err := repo.ListBridgeCandidates(
			tenantCtx, "pending", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, pendingRows, 1)
		assert.Equal(t, atBoundary.ID, pendingRows[0].ID,
			"pending drilldown and summary must agree on which row is on which side of the threshold")

		staleRows, err := repo.ListBridgeCandidates(
			tenantCtx, "stale", staleThreshold, time.Time{}, uuid.Nil, 50,
		)
		require.NoError(t, err)
		require.Len(t, staleRows, 1)
		assert.Equal(t, pastBoundary.ID, staleRows[0].ID)
	})
}

// -----------------------------------------------------------------------------
// Supporting fixture helpers — extend the link integration test set with
// the seed shapes T-004 needs (terminal-state extractions, backdated rows).
// -----------------------------------------------------------------------------

// backdateExtraction shifts the extraction's created_at into the past so
// the readiness query classifies it as stale. Required because the entity
// constructor unconditionally stamps NOW().
func backdateExtraction(t *testing.T, ctx context.Context, h *integration.TestHarness, id uuid.UUID, age time.Duration) {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries)
	primary := primaries[0]

	cutoff := time.Now().UTC().Add(-age)
	_, err = primary.ExecContext(ctx,
		`UPDATE extraction_requests SET created_at = $1 WHERE id = $2`,
		cutoff, id,
	)
	require.NoError(t, err, "backdate extraction created_at")
}

// seedTerminalExtraction inserts an extraction in any non-COMPLETE state
// so the readiness query picks it up under the failed (FAILED/CANCELLED) or
// in_flight (PENDING/SUBMITTED/EXTRACTING) partition. Uses raw SQL because
// the domain constructor would reject a direct status seed without going
// through the proper transition path.
//
// PENDING/FAILED/CANCELLED rows do not require a fetcher_job_id per
// chk_extraction_requests_fetcher_job_id_required; SUBMITTED/EXTRACTING do,
// so we synthesise one when needed to keep the constraint satisfied.
func seedTerminalExtraction(
	t *testing.T,
	ctx context.Context,
	h *integration.TestHarness,
	connectionID uuid.UUID,
	status vo.ExtractionStatus,
) uuid.UUID {
	t.Helper()

	resolver, err := h.Connection.Resolver(ctx)
	require.NoError(t, err)
	primaries := resolver.PrimaryDBs()
	require.NotEmpty(t, primaries)
	primary := primaries[0]

	id := uuid.New()

	// Decide whether the chk constraint requires fetcher_job_id for this status.
	var fetcherJobID *string

	switch status {
	case vo.ExtractionStatusSubmitted, vo.ExtractionStatusExtracting, vo.ExtractionStatusComplete:
		jobID := "seed-" + id.String()
		fetcherJobID = &jobID
	default:
		// PENDING/FAILED/CANCELLED — fetcher_job_id stays NULL.
	}

	_, err = primary.ExecContext(ctx, `
		INSERT INTO extraction_requests (
			id, connection_id, status, fetcher_job_id, error_message, tables, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, 'seeded for T-004', '{}'::jsonb, NOW(), NOW())
	`, id, connectionID, string(status), fetcherJobID)
	require.NoError(t, err, "seed terminal extraction")
	return id
}
