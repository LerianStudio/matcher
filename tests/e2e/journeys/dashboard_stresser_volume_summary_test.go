//go:build e2e

package journeys

import (
	"fmt"
	"strings"
	"testing"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/client"
)

type dashboardStresserHighVolumeStats struct {
	enrichedEndpoints    int
	scheduleCreated      bool
	clonedContextID      string
	feeScheduleSimulated bool
	exceptionsFound      int
	commentsAdded        int
	commentsDeleted      int
	disputesOpened       int
	disputesClosed       int
	bulkAssigned         int
	bulkResolved         int
	bulkDispatched       int
	forceMatched         int
	adjustedEntries      int
	dispatched           int
	syncReports          int
	asyncJobs            int
	auditLogsFound       int
	searchResultsFound   int
}

func finalizeDashboardStresserHighVolumeEnrichment(
	t *testing.T,
	tc *e2e.TestContext,
	reconciliationContextID string,
	reconciliationContextName string,
	dashboard *client.DashboardAggregates,
	matchGroups []client.MatchGroup,
	endpointFailures map[string]string,
	stats dashboardStresserHighVolumeStats,
) {
	// ── Endpoint Failure Evaluation ──────────────────────────────────────
	tc.Logf("\n" + repeatStr("─", 60))
	tc.Logf("ENDPOINT FAILURE ANALYSIS")
	tc.Logf(repeatStr("─", 60))

	var unexpectedFailures []string
	var knownHits []string
	var fixedKnown []string

	for name, errMsg := range endpointFailures {
		if reason, known := knownFailures[name]; known {
			knownHits = append(knownHits, fmt.Sprintf("  KNOWN  %-30s → %s", name, reason))
		} else {
			unexpectedFailures = append(unexpectedFailures, fmt.Sprintf("  FAIL   %-30s → %s", name, errMsg))
		}
	}

	// Detect fixed known failures (were in allowlist but now pass)
	for name := range knownFailures {
		if _, failed := endpointFailures[name]; !failed {
			fixedKnown = append(fixedKnown, name)
		}
	}

	if len(knownHits) > 0 {
		tc.Logf("\nKnown failures (%d):", len(knownHits))
		for _, line := range knownHits {
			tc.Logf("%s", line)
		}
	}

	if len(fixedKnown) > 0 {
		tc.Logf("\nPreviously known failures now PASSING (%d) — remove from knownFailures:", len(fixedKnown))
		for _, name := range fixedKnown {
			tc.Logf("  FIXED  %s", name)
		}
	}

	if len(unexpectedFailures) > 0 {
		tc.Logf("\nUnexpected failures (%d):", len(unexpectedFailures))
		for _, line := range unexpectedFailures {
			tc.Logf("%s", line)
		}
	}

	tc.Logf("\nEndpoint results: %d passed, %d known failures, %d fixed, %d unexpected",
		stats.enrichedEndpoints, len(knownHits), len(fixedKnown), len(unexpectedFailures))
	tc.Logf(repeatStr("─", 60))

	// Fail the test if any unexpected failures occurred
	if len(unexpectedFailures) > 0 {
		t.Errorf("%d unexpected endpoint failure(s) detected:\n%s",
			len(unexpectedFailures), strings.Join(unexpectedFailures, "\n"))
	}

	// ============================================================
	// ENRICHED SUMMARY
	// ============================================================
	tc.Logf("\n" + repeatStr("=", 60))
	tc.Logf("ENRICHED HIGH VOLUME DASHBOARD SUMMARY")
	tc.Logf(repeatStr("=", 60))
	tc.Logf("Context Name: %s", reconciliationContextName)
	tc.Logf("Context ID:   %s", reconciliationContextID)

	if dashboard.Volume != nil {
		tc.Logf("\nVolume Stats:")
		tc.Logf("  Total Transactions:   %d", dashboard.Volume.TotalTransactions)
		tc.Logf("  Matched Transactions: %d", dashboard.Volume.MatchedTransactions)
		tc.Logf("  Unmatched Count:      %d", dashboard.Volume.UnmatchedCount)
		tc.Logf("  Total Amount:         %s", dashboard.Volume.TotalAmount)
		tc.Logf("  Matched Amount:       %s", dashboard.Volume.MatchedAmount)
		tc.Logf("  Unmatched Amount:     %s", dashboard.Volume.UnmatchedAmount)
	}

	if dashboard.MatchRate != nil {
		tc.Logf("\nMatch Rate Stats:")
		tc.Logf("  Match Rate:           %.2f%%", dashboard.MatchRate.MatchRate)
		tc.Logf("  Match Rate by Amount: %.2f%%", dashboard.MatchRate.MatchRateAmount)
	}

	tc.Logf("\nEnriched API Coverage:")
	tc.Logf("  Enriched endpoints exercised: %d", stats.enrichedEndpoints)
	tc.Logf("  Match groups:                 %d", len(matchGroups))
	tc.Logf("  Schedule created:             %v", stats.scheduleCreated)
	tc.Logf("  Exceptions found:             %d", stats.exceptionsFound)
	tc.Logf("  Force matched:                %d", stats.forceMatched)
	tc.Logf("  Adjusted entries:             %d", stats.adjustedEntries)
	tc.Logf("  Dispatched:                   %d", stats.dispatched)
	tc.Logf("  Comments added/deleted:       %d/%d", stats.commentsAdded, stats.commentsDeleted)
	tc.Logf("  Bulk assigned/resolved/dispatched: %d/%d/%d", stats.bulkAssigned, stats.bulkResolved, stats.bulkDispatched)
	tc.Logf("  Disputes opened/closed:       %d/%d", stats.disputesOpened, stats.disputesClosed)
	tc.Logf("  Sync reports:                 %d", stats.syncReports)
	tc.Logf("  Async export jobs:            %d", stats.asyncJobs)
	tc.Logf("  Audit logs found:             %d", stats.auditLogsFound)
	tc.Logf("  Search results:               %d", stats.searchResultsFound)
	tc.Logf("  Fee schedule simulated:       %v", stats.feeScheduleSimulated)
	if stats.clonedContextID != "" {
		tc.Logf("  Cloned context:               %s", stats.clonedContextID)
	}

	tc.Logf("\n" + repeatStr("=", 60))
	if shouldSkipCleanup() {
		tc.Logf("Data preserved! View dashboard at context: %s", reconciliationContextID)
		tc.Logf(repeatStr("=", 60))
	} else {
		tc.Logf("Test completed (data will be cleaned up)")
		tc.Logf(repeatStr("=", 60))
	}
}
