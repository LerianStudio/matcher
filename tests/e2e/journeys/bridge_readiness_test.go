//go:build e2e

package journeys

import (
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
	"github.com/LerianStudio/matcher/tests/e2e/mock"
)

// bridgeStaleThresholdKey is the single systemplane config key this journey
// mutates to prove the runtime config round-trip works end-to-end.
const bridgeStaleThresholdKey = "fetcher.bridge_stale_threshold_sec"

// readBridgeStaleThresholdSnapshot captures the current value (if any) of
// fetcher.bridge_stale_threshold_sec so the subtest can restore it regardless
// of success or failure.
func readBridgeStaleThresholdSnapshot(appBaseURL string) (map[string]any, error) {
	value, found, err := readSystemplaneKeyValue(appBaseURL, bridgeStaleThresholdKey)
	if err != nil {
		return nil, err
	}

	snap := make(map[string]any, 1)
	if found {
		snap[bridgeStaleThresholdKey] = value
	}

	return snap, nil
}

// restoreBridgeStaleThresholdSnapshot PUTs the captured value back. A nil
// or empty snapshot is a no-op so absent-key restores don't shadow registry
// defaults.
func restoreBridgeStaleThresholdSnapshot(appBaseURL string, snapshot map[string]any) error {
	if len(snapshot) == 0 {
		return nil
	}

	return putSystemplaneValues(appBaseURL, snapshot)
}

// TestBridgeReadiness_Journey exercises GET /v1/discovery/extractions/bridge/summary
// and GET /v1/discovery/extractions/bridge/candidates end-to-end against a mock
// Fetcher. The test seeds extractions in known discovery states (COMPLETE,
// FAILED) and then asserts that the HTTP endpoints surface them in the
// expected readiness buckets with the correct DTO shape.
//
// Scope and deliberate omissions:
//   - The "ready" bucket requires the full verified-artifact bridge pipeline
//     (APP_ENC_KEY + object storage + ingestion) to link a COMPLETE extraction
//     to an ingestion job. The local E2E stack does not wire that pipeline, so
//     this journey does not assert ready_count > 0. Unit + integration tests
//     cover that flow; this journey guards HTTP routing, DTO serialisation,
//     SQL partitioning for the remaining buckets, and cursor pagination.
//   - A COMPLETE extraction in the local stack remains unlinked forever, which
//     is exactly the "pending" bucket the summary surfaces. Pushing its age
//     past a low stale-threshold moves it into "stale".
//   - A FAILED extraction (mock job status=failed, polled) lands in the
//     "failed" bucket, with bridge_last_error empty because the failure came
//     from the extraction layer, not the bridge pipeline.
func TestBridgeReadiness_Journey(t *testing.T) {
	e2e.RunE2EWithTimeout(t, 3*time.Minute, func(t *testing.T, tc *e2e.TestContext, apiClient *e2e.Client) {
		mockServer := getMockFetcher()
		if mockServer == nil {
			if os.Getenv(requireFetcherMockEnv) != "" {
				t.Fatal("mock Fetcher server is required for this bridge readiness run")
			}

			t.Skip("Mock Fetcher server not available")
		}

		ctx := context.Background()
		discovery := apiClient.Discovery

		// -----------------------------------------------------------------
		// Seed the mock with one postgres connection. We keep this journey's
		// connections distinct from discovery_test.go by using different
		// config names so both tests can coexist in the same run.
		// -----------------------------------------------------------------
		mockServer.Reset()
		mockServer.SetHealthy(true)

		const (
			mockPendingConnID = "conn-bridge-pending-001"
			mockFailedConnID  = "conn-bridge-failed-001"
		)

		mockServer.AddConnection(mock.MockConnection{
			ID:           mockPendingConnID,
			ConfigName:   "bridge-pending",
			Type:         "postgresql",
			Host:         "db.bridge-test.local",
			Port:         5432,
			DatabaseName: "bridge_pending_db",
			ProductName:  "PostgreSQL 15",
		})
		mockServer.AddConnection(mock.MockConnection{
			ID:           mockFailedConnID,
			ConfigName:   "bridge-failed",
			Type:         "postgresql",
			Host:         "db.bridge-test.local",
			Port:         5432,
			DatabaseName: "bridge_failed_db",
			ProductName:  "PostgreSQL 15",
		})

		mockServer.SetSchema(mockPendingConnID, &mock.MockSchema{
			ID:           mockPendingConnID,
			ConfigName:   "bridge-pending",
			DatabaseName: "bridge_pending_db",
			Type:         "postgresql",
			Tables: []mock.MockTable{
				{Name: "orders", Fields: []string{"id", "amount", "currency"}},
			},
		})
		mockServer.SetSchema(mockFailedConnID, &mock.MockSchema{
			ID:           mockFailedConnID,
			ConfigName:   "bridge-failed",
			DatabaseName: "bridge_failed_db",
			Type:         "postgresql",
			Tables: []mock.MockTable{
				{Name: "orders", Fields: []string{"id", "amount", "currency"}},
			},
		})

		mockServer.SetTestResult(mockPendingConnID, &mock.MockTestResult{
			Status:    "success",
			Message:   "connection established",
			LatencyMs: 42,
		})
		mockServer.SetTestResult(mockFailedConnID, &mock.MockTestResult{
			Status:    "success",
			Message:   "connection established",
			LatencyMs: 42,
		})

		// Shared state populated across ordered subtests.
		var (
			pendingConnectionID string
			failedConnectionID  string
			pendingExtractionID string
			failedExtractionID  string
			pendingFetcherJobID string
			failedFetcherJobID  string
		)

		// =================================================================
		// 01 — Refresh to sync the two connections from the mock
		// =================================================================
		t.Run("01_refresh_syncs_connections", func(t *testing.T) {
			resp, err := discovery.RefreshDiscovery(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, resp.ConnectionsSynced, 2,
				"should sync at least the 2 bridge-journey connections")
		})

		// =================================================================
		// 02 — List and capture Matcher UUIDs for the two connections
		// =================================================================
		t.Run("02_list_connections_resolves_uuids", func(t *testing.T) {
			list, err := discovery.ListConnections(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(list.Connections), 2,
				"should list at least the 2 bridge-journey connections")

			for i := range list.Connections {
				conn := list.Connections[i]
				switch conn.ConfigName {
				case "bridge-pending":
					pendingConnectionID = conn.ID
				case "bridge-failed":
					failedConnectionID = conn.ID
				}
			}

			require.NotEmpty(t, pendingConnectionID,
				"bridge-pending connection must exist")
			require.NotEmpty(t, failedConnectionID,
				"bridge-failed connection must exist")
		})

		// =================================================================
		// 03 — Seed a COMPLETE extraction (lands in "pending" bucket)
		// =================================================================
		t.Run("03_seed_complete_extraction", func(t *testing.T) {
			if pendingConnectionID == "" {
				t.Skip("skipping: depends on 02_list_connections_resolves_uuids")
			}

			extraction, err := discovery.StartExtraction(ctx, pendingConnectionID, client.DiscoveryStartExtractionRequest{
				Tables: map[string]client.DiscoveryExtractionTableRequest{
					"orders": {Columns: []string{"id", "amount", "currency"}},
				},
				StartDate: "2025-01-01",
				EndDate:   "2025-12-31",
			})
			require.NoError(t, err)
			require.NotEmpty(t, extraction.ID)

			pendingExtractionID = extraction.ID
			pendingFetcherJobID = mockServer.GetLastJobID()
			require.NotEmpty(t, pendingFetcherJobID)

			// Drive the mock job to COMPLETE, then poll so Matcher transitions
			// the extraction to ExtractionStatusComplete. An unlinked COMPLETE
			// extraction is exactly the "pending" bucket from the dashboard's
			// perspective (no ingestion_job_id, bridge_last_error IS NULL).
			mockServer.AddJob(mock.MockExtractionJob{
				ID:         pendingFetcherJobID,
				Status:     "completed",
				ResultPath: "/data/extractions/bridge-pending-orders.csv",
			})

			polled, err := discovery.PollExtraction(ctx, pendingExtractionID)
			require.NoError(t, err)
			require.Equal(t, "COMPLETE", polled.Status,
				"polled extraction should be COMPLETE")
			require.Nil(t, polled.IngestionJobID,
				"COMPLETE extraction should stay unlinked without the bridge worker")
		})

		// =================================================================
		// 04 — Seed a FAILED extraction (lands in "failed" bucket)
		// =================================================================
		t.Run("04_seed_failed_extraction", func(t *testing.T) {
			if failedConnectionID == "" {
				t.Skip("skipping: depends on 02_list_connections_resolves_uuids")
			}

			extraction, err := discovery.StartExtraction(ctx, failedConnectionID, client.DiscoveryStartExtractionRequest{
				Tables: map[string]client.DiscoveryExtractionTableRequest{
					"orders": {Columns: []string{"id", "amount"}},
				},
			})
			require.NoError(t, err)
			require.NotEmpty(t, extraction.ID)

			failedExtractionID = extraction.ID
			failedFetcherJobID = mockServer.GetLastJobID()
			require.NotEmpty(t, failedFetcherJobID)

			// Force a terminal failure at the extraction layer. The mock
			// status transition is observable to the Matcher poller, which
			// calls MarkFailed and persists ExtractionStatusFailed. This
			// lands the row in the "failed" readiness bucket.
			mockServer.SetJobStatus(failedFetcherJobID, "failed")

			polled, err := discovery.PollExtraction(ctx, failedExtractionID)
			require.NoError(t, err)
			require.Equal(t, "FAILED", polled.Status,
				"polled extraction should be FAILED")
		})

		// =================================================================
		// 05 — Summary reflects pending + failed buckets
		// =================================================================
		t.Run("05_summary_counts_pending_and_failed", func(t *testing.T) {
			if pendingExtractionID == "" || failedExtractionID == "" {
				t.Skip("skipping: depends on seed subtests")
			}

			summary, err := discovery.GetBridgeReadinessSummary(ctx)
			require.NoError(t, err)
			require.NotNil(t, summary)

			// The COMPLETE+unlinked extraction lives in either "pending" or
			// "stale" depending on the systemplane-configured threshold. The
			// default threshold is 1 hour, so a freshly-seeded extraction
			// lands in "pending". Other runs in the same E2E stack may have
			// added rows to these buckets, hence the >= asserts.
			require.GreaterOrEqual(t, summary.PendingCount, int64(1),
				"pending bucket should include the seeded COMPLETE extraction")
			require.GreaterOrEqual(t, summary.FailedCount, int64(1),
				"failed bucket should include the seeded FAILED extraction")

			// Total must match the sum of the individual buckets — mutual
			// exclusion is part of the dashboard contract.
			require.Equal(t,
				summary.ReadyCount+summary.PendingCount+summary.StaleCount+
					summary.FailedCount+summary.InFlightCount,
				summary.TotalCount,
				"total must equal the sum of mutually-exclusive buckets")

			require.Greater(t, summary.StaleThresholdSec, int64(0),
				"staleThresholdSec must be echoed back and positive")
			require.False(t, summary.GeneratedAt.IsZero(),
				"generatedAt must be populated")
		})

		// =================================================================
		// 06 — Candidates drilldown for "pending" includes the COMPLETE row
		// =================================================================
		t.Run("06_candidates_pending_includes_extraction", func(t *testing.T) {
			if pendingExtractionID == "" {
				t.Skip("skipping: depends on 03_seed_complete_extraction")
			}

			page, err := discovery.ListBridgeCandidates(ctx, "pending", "", 200)
			require.NoError(t, err)
			require.NotNil(t, page)
			require.Equal(t, "pending", page.State)
			require.Equal(t, 200, page.Limit)

			found := false

			for i := range page.Items {
				item := page.Items[i]
				if item.ExtractionID == pendingExtractionID {
					found = true

					require.Equal(t, "COMPLETE", item.Status,
						"pending candidate must carry the upstream COMPLETE status")
					require.Equal(t, "pending", item.ReadinessState,
						"candidate readinessState must echo the requested bucket")
					require.Equal(t, pendingConnectionID, item.ConnectionID,
						"candidate connectionId must match the seeded connection")
					require.Nil(t, item.IngestionJobID,
						"pending candidate must be unlinked")
					require.Equal(t, pendingFetcherJobID, item.FetcherJobID,
						"candidate fetcherJobId must match the mock-generated id")
					require.GreaterOrEqual(t, item.AgeSeconds, int64(0),
						"candidate ageSeconds must be non-negative")
					require.Empty(t, item.BridgeLastError,
						"pending candidate must not carry a bridge_last_error")
					require.False(t, item.CreatedAt.IsZero())
					require.False(t, item.UpdatedAt.IsZero())

					break
				}
			}

			require.True(t, found,
				"seeded COMPLETE extraction %s must be present in pending drilldown",
				pendingExtractionID)
		})

		// =================================================================
		// 07 — Candidates drilldown for "failed" includes the FAILED row
		// =================================================================
		t.Run("07_candidates_failed_includes_extraction", func(t *testing.T) {
			if failedExtractionID == "" {
				t.Skip("skipping: depends on 04_seed_failed_extraction")
			}

			page, err := discovery.ListBridgeCandidates(ctx, "failed", "", 200)
			require.NoError(t, err)
			require.NotNil(t, page)
			require.Equal(t, "failed", page.State)

			found := false

			for i := range page.Items {
				item := page.Items[i]
				if item.ExtractionID == failedExtractionID {
					found = true

					require.Equal(t, "FAILED", item.Status,
						"failed candidate must carry the upstream FAILED status")
					require.Equal(t, "failed", item.ReadinessState,
						"candidate readinessState must echo the requested bucket")
					require.Equal(t, failedConnectionID, item.ConnectionID)
					require.Nil(t, item.IngestionJobID,
						"failed candidate is unlinked")
					// bridge_last_error is populated only for bridge-pipeline
					// failures (T-005). An extraction-layer FAILED row leaves
					// it empty, which the DTO omits via omitempty.
					require.Empty(t, item.BridgeLastError,
						"extraction-layer FAILED rows must not carry a bridge_last_error")

					break
				}
			}

			require.True(t, found,
				"seeded FAILED extraction %s must be present in failed drilldown",
				failedExtractionID)
		})

		// =================================================================
		// 08 — Invalid state returns 400
		// =================================================================
		t.Run("08_invalid_state_returns_400", func(t *testing.T) {
			_, err := discovery.ListBridgeCandidates(ctx, "not-a-state", "", 0)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr),
				"error should be APIError")
			require.Equal(t, http.StatusBadRequest, apiErr.StatusCode,
				"unknown state must yield 400")
		})

		// =================================================================
		// 09 — Missing state returns 400
		// =================================================================
		t.Run("09_missing_state_returns_400", func(t *testing.T) {
			_, err := discovery.ListBridgeCandidates(ctx, "", "", 0)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr),
				"error should be APIError")
			require.Equal(t, http.StatusBadRequest, apiErr.StatusCode,
				"missing state must yield 400")
		})

		// =================================================================
		// 10 — Limit above the cap returns 400
		// =================================================================
		t.Run("10_limit_too_large_returns_400", func(t *testing.T) {
			_, err := discovery.ListBridgeCandidates(ctx, "pending", "", 1000)
			require.Error(t, err)

			var apiErr *client.APIError
			require.True(t, errors.As(err, &apiErr),
				"error should be APIError")
			require.Equal(t, http.StatusBadRequest, apiErr.StatusCode,
				"limit exceeding the cap must yield 400")
		})

		// =================================================================
		// 11 — Stale threshold override surfaces the COMPLETE row in "stale"
		//       (runtime config round-trip proves systemplane wiring works)
		// =================================================================
		t.Run("11_stale_threshold_moves_pending_to_stale", func(t *testing.T) {
			if pendingExtractionID == "" {
				t.Skip("skipping: depends on 03_seed_complete_extraction")
			}

			// Capture the current threshold so we can restore it after the
			// subtest, independent of success or failure.
			snapshot, err := readBridgeStaleThresholdSnapshot(tc.Config().AppBaseURL)
			require.NoError(t, err, "snapshot stale threshold")

			defer func() {
				if restoreErr := restoreBridgeStaleThresholdSnapshot(tc.Config().AppBaseURL, snapshot); restoreErr != nil {
					t.Logf("warning: failed to restore bridge stale threshold: %v", restoreErr)
				}
			}()

			// Minimum allowed by validateBridgeStaleThresholdSec is 60s.
			// Sixty seconds after the seed extraction was created it is
			// guaranteed to be past the threshold.
			const lowThresholdSec = 60

			require.NoError(t,
				putSystemplaneValues(tc.Config().AppBaseURL, map[string]any{
					"fetcher.bridge_stale_threshold_sec": lowThresholdSec,
				}),
				"patching bridge stale threshold to %d seconds", lowThresholdSec)

			// Eventually poll the summary / candidates — the threshold is a
			// live-read config (ApplyLiveRead), so the very next request
			// should see the new value. We still wrap in Eventually to absorb
			// any in-flight write lag and to clock the row past 60s if needed.
			opts := e2e.DefaultPollOptions(tc.Config())
			opts.Timeout = 2 * time.Minute

			require.NoError(t,
				e2e.Eventually(ctx, opts, func() (bool, error) {
					page, err := discovery.ListBridgeCandidates(ctx, "stale", "", 200)
					if err != nil {
						return false, err
					}
					for i := range page.Items {
						if page.Items[i].ExtractionID == pendingExtractionID {
							return true, nil
						}
					}
					return false, nil
				}),
				"seeded COMPLETE extraction should move into the stale bucket once past the 60s threshold",
			)

			summary, err := discovery.GetBridgeReadinessSummary(ctx)
			require.NoError(t, err)
			require.GreaterOrEqual(t, summary.StaleCount, int64(1),
				"summary stale_count must include at least the seeded extraction")
			require.EqualValues(t, lowThresholdSec, summary.StaleThresholdSec,
				"summary must echo back the patched threshold")
		})
	})
}
