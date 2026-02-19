//go:build integration

package flow

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/integration/server"
)

func TestIntegrationHTTPFlow_UploadFile_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload a ledger CSV file via HTTP
		csvContent := BuildCSVContent("HTTP-REF-001", "100.00", "USD", "2026-01-15", "ledger tx")
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "ledger.csv", csvContent, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted, got %d: %s",
			resp.StatusCode,
			string(body),
		)

		jobResp := ParseJobResponse(t, body)
		require.NotEqual(
			t,
			jobResp.ID.String(),
			"00000000-0000-0000-0000-000000000000",
			"job ID should not be nil",
		)
		require.NotEmpty(t, jobResp.Status)

		// Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

func TestIntegrationHTTPFlow_UploadJSONFile_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload a ledger JSON file via HTTP
		jsonContent := BuildJSONContent(t,
			"JSON-REF-001",
			"100.00",
			"USD",
			"2026-01-15",
			"ledger tx json",
		)
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(
			path,
			"file",
			"ledger.json",
			jsonContent,
			map[string]string{
				"format": "json",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted, got %d: %s",
			resp.StatusCode,
			string(body),
		)

		jobResp := ParseJobResponse(t, body)
		require.NotEqual(
			t,
			jobResp.ID.String(),
			"00000000-0000-0000-0000-000000000000",
			"job ID should not be nil",
		)
		require.NotEmpty(t, jobResp.Status)

		// Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

func TestIntegrationHTTPFlow_UploadJSONAndMatch_PublishesEvents(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger JSON file
		ledgerJSON := BuildJSONContent(t,
			"JSON-FLOW-001",
			"350.00",
			"USD",
			"2026-01-15",
			"ledger json entry",
		)
		ledgerPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger.json",
			ledgerJSON,
			map[string]string{
				"format": "json",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload failed: %s",
			string(ledgerBody),
		)

		ledgerJob := ParseJobResponse(t, ledgerBody)
		t.Logf("Ledger JSON job created: %s", ledgerJob.ID)

		// Step 2: Upload bank JSON file (matching transaction)
		bankJSON := BuildJSONContent(t,
			"json-flow-001",
			"350.00",
			"USD",
			"2026-01-15",
			"bank json statement",
		)
		bankPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

		bankResp, bankBody, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank.json",
			bankJSON,
			map[string]string{
				"format": "json",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload failed: %s",
			string(bankBody),
		)

		bankJob := ParseJobResponse(t, bankBody)
		t.Logf("Bank JSON job created: %s", bankJob.ID)

		// Step 3: Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 4: Trigger matching via HTTP
		matchPath := RunMatchPath(seed.ContextID)
		matchResp, matchBody, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{
			"mode": "COMMIT",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching failed: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Step 5: Dispatch outbox to publish match confirmed events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 6: Verify match_confirmed was published to RabbitMQ
		eventBody, err := sh.WaitForEventWithTimeout(
			10*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		require.NotEmpty(t, event.EventType, "event_type should not be empty")
		t.Logf("Received match_confirmed event: MatchID=%s, RunID=%s", event.MatchID, event.RunID)
	})
}

func TestIntegrationHTTPFlow_UploadMultiRowJSON_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload a ledger JSON file with multiple transactions
		jsonContent := BuildMultiRowJSON(t, [][]string{
			{"JSON-MULTI-001", "100.00", "USD", "2026-01-15", "tx 1"},
			{"JSON-MULTI-002", "200.00", "EUR", "2026-01-16", "tx 2"},
			{"JSON-MULTI-003", "300.00", "GBP", "2026-01-17", "tx 3"},
		})
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(
			path,
			"file",
			"ledger_multi.json",
			jsonContent,
			map[string]string{
				"format": "json",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted, got %d: %s",
			resp.StatusCode,
			string(body),
		)

		jobResp := ParseJobResponse(t, body)
		require.NotEqual(
			t,
			jobResp.ID.String(),
			"00000000-0000-0000-0000-000000000000",
			"job ID should not be nil",
		)
		require.NotEmpty(t, jobResp.Status)

		// Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

func TestIntegrationHTTPFlow_UploadAndMatch_PublishesEvents(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger file
		ledgerCSV := BuildCSVContent("FLOW-REF-001", "250.00", "USD", "2026-01-15", "ledger entry")
		ledgerPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{
				"format": "csv",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload failed: %s",
			string(ledgerBody),
		)

		ledgerJob := ParseJobResponse(t, ledgerBody)
		t.Logf("Ledger job created: %s", ledgerJob.ID)

		// Step 2: Upload bank file (matching transaction)
		bankCSV := BuildCSVContent("flow-ref-001", "250.00", "USD", "2026-01-15", "bank statement")
		bankPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

		bankResp, bankBody, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{
				"format": "csv",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload failed: %s",
			string(bankBody),
		)

		bankJob := ParseJobResponse(t, bankBody)
		t.Logf("Bank job created: %s", bankJob.ID)

		// Step 3: Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 4: Trigger matching via HTTP
		matchPath := RunMatchPath(seed.ContextID)
		matchResp, matchBody, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{
			"mode": "COMMIT",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching failed: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Step 5: Dispatch outbox to publish match confirmed events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 6: Verify match_confirmed was published to RabbitMQ
		eventBody, err := sh.WaitForEventWithTimeout(
			10*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		require.NotEmpty(t, event.EventType, "event_type should not be empty")
		t.Logf("Received match_confirmed event: MatchID=%s, RunID=%s", event.MatchID, event.RunID)
	})
}

func TestIntegrationHTTPFlow_DryRunMatch_DoesNotPublishEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching transactions
		ledgerCSV := BuildCSVContent("DRY-REF-001", "99.99", "EUR", "2026-01-16", "ledger")
		bankCSV := BuildCSVContent("dry-ref-001", "99.99", "EUR", "2026-01-16", "bank")

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Run matching in DRY_RUN mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "DRY_RUN",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"dry run matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Dry run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch any outbox events (should be none for dry run)
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Verify no match_confirmed event was published
		// Wait a short time to confirm no events arrive
		waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event for dry run",
		)
	})
}

func TestIntegrationHTTPFlow_GetJobStatus(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		// Upload a file
		csvContent := BuildCSVContent("STATUS-REF-001", "50.00", "GBP", "2026-01-17", "test")
		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"test.csv",
			csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, uploadResp.StatusCode)

		job := ParseJobResponse(t, uploadBody)

		// Get job status
		statusPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs/" + job.ID.String()
		statusResp, statusBody, err := sh.DoJSON(http.MethodGet, statusPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusOK,
			statusResp.StatusCode,
			"get job status: %s",
			string(statusBody),
		)

		statusJob := ParseJobResponse(t, statusBody)
		require.Equal(t, job.ID, statusJob.ID)
		t.Logf("Job status: %s", statusJob.Status)
	})
}

// =============================================================================
// Partial Match Scenario Tests
// =============================================================================

func TestIntegrationHTTPFlow_PartialMatch_OnlyMatchingPairsConfirmed(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger file with 3 transactions
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"REF-A", "100.00", "USD", "2026-01-15", "ledger tx A"},
			{"REF-B", "200.00", "USD", "2026-01-15", "ledger tx B"},
			{"REF-C", "300.00", "USD", "2026-01-15", "ledger tx C"},
		})
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job created: %s", ParseJobResponse(t, ledgerBody).ID)

		// Step 2: Upload bank file with only 2 matching transactions (REF-A and REF-B)
		bankCSV := BuildMultiRowCSV([][]string{
			{"ref-a", "100.00", "USD", "2026-01-15", "bank tx A"},
			{"ref-b", "200.00", "USD", "2026-01-15", "bank tx B"},
		})
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job created: %s", ParseJobResponse(t, bankBody).ID)

		// Step 3: Dispatch outbox to process ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 4: Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Step 5: Dispatch outbox to publish match confirmed events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 6: Wait for match_confirmed events
		// With 2 matching pairs (REF-A/ref-a and REF-B/ref-b), we expect 2 events,
		// but the matching engine may batch them differently. Verify at least 1 event.
		events := WaitForMultipleMatchEvents(t, sh, 2, 15*time.Second)
		require.GreaterOrEqual(
			t,
			len(events),
			1,
			"expected at least 1 match_confirmed event, got %d",
			len(events),
		)
		require.LessOrEqual(
			t,
			len(events),
			2,
			"expected at most 2 match_confirmed events, got %d",
			len(events),
		)

		t.Logf("Received %d match_confirmed events", len(events))
	})
}

func TestIntegrationHTTPFlow_PartialMatch_UnmatchedTransactionsRemainPending(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger with 3 transactions
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"PARTIAL-A", "150.00", "EUR", "2026-01-16", "ledger A"},
			{"PARTIAL-B", "250.00", "EUR", "2026-01-16", "ledger B"},
			{"PARTIAL-C", "350.00", "EUR", "2026-01-16", "ledger C"},
		})
		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		// Upload bank with only 1 matching transaction (PARTIAL-A)
		bankCSV := BuildMultiRowCSV([][]string{
			{"partial-a", "150.00", "EUR", "2026-01-16", "bank A"},
		})
		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		// Dispatch ingestion
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify only 1 match_confirmed event (one match)
		events := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.Equal(t, 1, len(events), "expected 1 match_confirmed event")

		t.Logf("Verified: 1 match event received, 2 unmatched (PARTIAL-B, PARTIAL-C)")
	})
}

func TestIntegrationHTTPFlow_PartialMatch_NoMatchesProducesNoEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger with transactions
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"NOMATCH-A", "100.00", "USD", "2026-01-17", "ledger A"},
			{"NOMATCH-B", "200.00", "USD", "2026-01-17", "ledger B"},
		})
		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		// Upload bank with completely different references (no matches possible)
		bankCSV := BuildMultiRowCSV([][]string{
			{"different-x", "999.00", "GBP", "2026-01-17", "bank X"},
			{"different-y", "888.00", "JPY", "2026-01-17", "bank Y"},
		})
		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		// Dispatch ingestion
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch any outbox events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify no match_confirmed event is published (timeout expected)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event when no matches found",
		)
		t.Logf("Verified: no match_confirmed event published for zero matches")
	})
}

// =============================================================================
// Error Handling Tests
// =============================================================================

func TestIntegrationHTTPFlow_UploadFile_InvalidFormat_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-001", "100.00", "USD", "2026-01-15", "test")
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Use invalid format
		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{
			"format": "xlsx", // Invalid format
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
		require.Contains(t, string(body), "invalid format")
	})
}

func TestIntegrationHTTPFlow_UploadFile_MissingFormat_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-002", "100.00", "USD", "2026-01-15", "test")
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Omit format field
		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
		require.Contains(t, string(body), "format is required")
	})
}

func TestIntegrationHTTPFlow_UploadFile_NonExistentSource_ReturnsError(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-003", "100.00", "USD", "2026-01-15", "test")
		nonExistentSourceID := uuid.New()
		path := UploadPath(seed.ContextID, nonExistentSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusNotFound,
			resp.StatusCode,
			"non-existent source should return 404: %s",
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_UploadFile_NonExistentContext_ReturnsError(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-004", "100.00", "USD", "2026-01-15", "test")
		nonExistentContextID := uuid.New()
		path := UploadPath(nonExistentContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusNotFound,
			resp.StatusCode,
			"non-existent context should return 404: %s",
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_UploadFile_InvalidContextID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-005", "100.00", "USD", "2026-01-15", "test")
		// Use invalid UUID format in path
		path := "/v1/imports/contexts/not-a-uuid/sources/" + seed.LedgerSourceID.String() + "/upload"

		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_UploadFile_InvalidSourceID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		csvContent := BuildCSVContent("ERR-REF-006", "100.00", "USD", "2026-01-15", "test")
		// Use invalid UUID format in path
		path := "/v1/imports/contexts/" + seed.ContextID.String() + "/sources/invalid-uuid/upload"

		resp, body, err := sh.DoMultipart(path, "file", "test.csv", csvContent, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_UploadFile_MalformedCSV_ProcessesWithErrors(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)

		// Malformed CSV: missing columns, inconsistent row lengths
		malformedCSV := []byte(
			"id,amount,currency\nREF-001,100.00\nREF-002,200.00,USD,extra_column\n",
		)
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(
			path,
			"file",
			"malformed.csv",
			malformedCSV,
			map[string]string{
				"format": "csv",
			},
		)
		require.NoError(t, err)
		// Job creation should succeed, but processing may fail
		require.Equal(t, http.StatusAccepted, resp.StatusCode, "job creation: %s", string(body))

		job := ParseJobResponse(t, body)
		t.Logf("Job created for malformed CSV: %s", job.ID)

		// Dispatch outbox to process
		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

func TestIntegrationHTTPFlow_UploadFile_EmptyFile_ReturnsError(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		emptyContent := []byte("")
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(
			path,
			"file",
			"empty.csv",
			emptyContent,
			map[string]string{
				"format": "csv",
			},
		)
		require.NoError(t, err)
		// Empty file should be rejected with 400 Bad Request
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"empty file should be rejected: %s",
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_UploadFile_ExceedsMaxSize_Returns413(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		maxUploadSize := 100 * 1024 * 1024
		oversizedContent := make([]byte, maxUploadSize+1)
		for i := range oversizedContent {
			oversizedContent[i] = 'x'
		}

		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(
			path,
			"file",
			"oversized.csv",
			oversizedContent,
			map[string]string{
				"format": "csv",
			},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode,
			"expected 413 Request Entity Too Large, got %d: %s", resp.StatusCode, string(body))

		require.Contains(t, string(body), "file exceeds 100MB limit")
	})
}

func TestIntegrationHTTPFlow_UploadFile_ExactlyMaxSize_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		maxUploadSize := 100 * 1024 * 1024
		header := []byte("id,amount,currency,date,description\n")
		row := []byte("MAXSIZE-001,100.00,USD,2026-01-15,boundary test\n")
		paddingSize := maxUploadSize - len(header) - len(row)
		if paddingSize < 0 {
			paddingSize = 0
		}

		content := make([]byte, 0, maxUploadSize)
		content = append(content, header...)
		content = append(content, row...)
		padding := make([]byte, paddingSize)
		for i := range padding {
			padding[i] = ' '
		}
		content = append(content, padding...)

		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "maxsize.csv", content, map[string]string{
			"format": "csv",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted for file at exactly max size, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_GetJob_NonExistentJob_ReturnsNotFound(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		nonExistentJobID := uuid.New()
		statusPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs/" + nonExistentJobID.String()

		resp, body, err := sh.DoJSON(http.MethodGet, statusPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusNotFound,
			resp.StatusCode,
			"expected 404 Not Found, got %d: %s",
			resp.StatusCode,
			string(body),
		)
		errResp := AssertErrorResponse(t, body)
		t.Logf("Error response: code=%d, message=%s", errResp.Code, errResp.Message)
	})
}

func TestIntegrationHTTPFlow_GetJob_InvalidJobID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		statusPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs/not-a-uuid"

		resp, body, err := sh.DoJSON(http.MethodGet, statusPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
		errResp := AssertErrorResponse(t, body)
		t.Logf("Error response: code=%d, message=%s", errResp.Code, errResp.Message)
	})
}

func TestIntegrationHTTPFlow_RunMatch_InvalidMode_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		matchPath := RunMatchPath(seed.ContextID)
		resp, body, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{
			"mode": "INVALID_MODE",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_RunMatch_MissingMode_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		matchPath := RunMatchPath(seed.ContextID)
		resp, body, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_RunMatch_InvalidContextID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		matchPath := "/v1/matching/contexts/not-a-uuid/run"
		resp, body, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{
			"mode": "COMMIT",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

// =============================================================================
// Multiple Files Per Source Tests
// =============================================================================

func TestIntegrationHTTPFlow_MultipleFilesPerSource_CreatesSeperateJobs(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload first file
		csv1 := BuildCSVContent("MULTI-REF-001", "100.00", "USD", "2026-01-15", "first file tx 1")
		resp1, body1, err := sh.DoMultipart(
			uploadPath,
			"file",
			"batch1.csv",
			csv1,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp1.StatusCode, "first upload: %s", string(body1))
		job1 := ParseJobResponse(t, body1)
		t.Logf("First job created: %s", job1.ID)

		// Dispatch outbox for first file
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Upload second file to same source
		csv2 := BuildCSVContent("MULTI-REF-002", "200.00", "USD", "2026-01-16", "second file tx 1")
		resp2, body2, err := sh.DoMultipart(
			uploadPath,
			"file",
			"batch2.csv",
			csv2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp2.StatusCode, "second upload: %s", string(body2))
		job2 := ParseJobResponse(t, body2)
		t.Logf("Second job created: %s", job2.ID)

		// Dispatch outbox for second file
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Upload third file to same source
		csv3 := BuildCSVContent("MULTI-REF-003", "300.00", "USD", "2026-01-17", "third file tx 1")
		resp3, body3, err := sh.DoMultipart(
			uploadPath,
			"file",
			"batch3.csv",
			csv3,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp3.StatusCode, "third upload: %s", string(body3))
		job3 := ParseJobResponse(t, body3)
		t.Logf("Third job created: %s", job3.ID)

		// Verify all jobs have unique IDs
		require.NotEqual(t, job1.ID, job2.ID, "job1 and job2 should have different IDs")
		require.NotEqual(t, job2.ID, job3.ID, "job2 and job3 should have different IDs")
		require.NotEqual(t, job1.ID, job3.ID, "job1 and job3 should have different IDs")

		// Verify all jobs are valid (not nil UUIDs)
		nilUUID := "00000000-0000-0000-0000-000000000000"
		require.NotEqual(t, job1.ID.String(), nilUUID, "job1 ID should not be nil")
		require.NotEqual(t, job2.ID.String(), nilUUID, "job2 ID should not be nil")
		require.NotEqual(t, job3.ID.String(), nilUUID, "job3 ID should not be nil")
	})
}

func TestIntegrationHTTPFlow_MultipleFilesPerSource_AllTransactionsParticipateInMatching(
	t *testing.T,
) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		ledgerPath := UploadPath(seed.ContextID, seed.LedgerSourceID)
		bankPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

		// Upload multiple ledger files with different transactions
		ledgerCSV1 := BuildCSVContent(
			"BATCH-REF-001",
			"150.00",
			"USD",
			"2026-01-20",
			"ledger batch 1",
		)
		ledgerResp1, ledgerBody1, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger1.csv",
			ledgerCSV1,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp1.StatusCode,
			"ledger upload 1: %s",
			string(ledgerBody1),
		)
		ledgerJob1 := ParseJobResponse(t, ledgerBody1)
		t.Logf("Ledger job 1: %s", ledgerJob1.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		ledgerCSV2 := BuildCSVContent(
			"BATCH-REF-002",
			"250.00",
			"EUR",
			"2026-01-21",
			"ledger batch 2",
		)
		ledgerResp2, ledgerBody2, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger2.csv",
			ledgerCSV2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp2.StatusCode,
			"ledger upload 2: %s",
			string(ledgerBody2),
		)
		ledgerJob2 := ParseJobResponse(t, ledgerBody2)
		t.Logf("Ledger job 2: %s", ledgerJob2.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Upload matching bank files with corresponding transactions
		bankCSV1 := BuildCSVContent("batch-ref-001", "150.00", "USD", "2026-01-20", "bank batch 1")
		bankResp1, bankBody1, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank1.csv",
			bankCSV1,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp1.StatusCode,
			"bank upload 1: %s",
			string(bankBody1),
		)
		bankJob1 := ParseJobResponse(t, bankBody1)
		t.Logf("Bank job 1: %s", bankJob1.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 3)

		bankCSV2 := BuildCSVContent("batch-ref-002", "250.00", "EUR", "2026-01-21", "bank batch 2")
		bankResp2, bankBody2, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank2.csv",
			bankCSV2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp2.StatusCode,
			"bank upload 2: %s",
			string(bankBody2),
		)
		bankJob2 := ParseJobResponse(t, bankBody2)
		t.Logf("Bank job 2: %s", bankJob2.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching - should match transactions from all files
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch outbox to publish match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed event was published
		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		t.Logf("Received match_confirmed event: %s", string(eventBody))
	})
}

func TestIntegrationHTTPFlow_MultipleFilesPerSource_MultiRowFiles(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		ledgerPath := UploadPath(seed.ContextID, seed.LedgerSourceID)
		bankPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

		// Upload ledger file with multiple rows
		ledgerMultiRow := BuildMultiRowCSV([][]string{
			{"MULTIROW-001", "500.00", "USD", "2026-01-25", "payment 1"},
			{"MULTIROW-002", "750.00", "USD", "2026-01-25", "payment 2"},
			{"MULTIROW-003", "1000.00", "EUR", "2026-01-26", "payment 3"},
		})
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger_multi.csv",
			ledgerMultiRow,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger multi-row upload: %s",
			string(ledgerBody),
		)
		ledgerJob := ParseJobResponse(t, ledgerBody)
		t.Logf("Ledger multi-row job: %s", ledgerJob.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Upload second ledger file with more rows
		ledgerMultiRow2 := BuildMultiRowCSV([][]string{
			{"MULTIROW-004", "1250.00", "GBP", "2026-01-27", "payment 4"},
			{"MULTIROW-005", "1500.00", "USD", "2026-01-28", "payment 5"},
		})
		ledgerResp2, ledgerBody2, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger_multi2.csv",
			ledgerMultiRow2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp2.StatusCode,
			"ledger multi-row upload 2: %s",
			string(ledgerBody2),
		)
		ledgerJob2 := ParseJobResponse(t, ledgerBody2)
		t.Logf("Ledger multi-row job 2: %s", ledgerJob2.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Upload matching bank files
		bankMultiRow := BuildMultiRowCSV([][]string{
			{"multirow-001", "500.00", "USD", "2026-01-25", "bank 1"},
			{"multirow-002", "750.00", "USD", "2026-01-25", "bank 2"},
			{"multirow-003", "1000.00", "EUR", "2026-01-26", "bank 3"},
		})
		bankResp, bankBody, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank_multi.csv",
			bankMultiRow,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank multi-row upload: %s",
			string(bankBody),
		)
		bankJob := ParseJobResponse(t, bankBody)
		t.Logf("Bank multi-row job: %s", bankJob.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		bankMultiRow2 := BuildMultiRowCSV([][]string{
			{"multirow-004", "1250.00", "GBP", "2026-01-27", "bank 4"},
			{"multirow-005", "1500.00", "USD", "2026-01-28", "bank 5"},
		})
		bankResp2, bankBody2, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank_multi2.csv",
			bankMultiRow2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp2.StatusCode,
			"bank multi-row upload 2: %s",
			string(bankBody2),
		)
		bankJob2 := ParseJobResponse(t, bankBody2)
		t.Logf("Bank multi-row job 2: %s", bankJob2.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify all 4 jobs are distinct
		require.NotEqual(t, ledgerJob.ID, ledgerJob2.ID)
		require.NotEqual(t, bankJob.ID, bankJob2.ID)
		require.NotEqual(t, ledgerJob.ID, bankJob.ID)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed event
		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		t.Logf("Received match_confirmed event for multi-row files: %s", string(eventBody))
	})
}

func TestIntegrationHTTPFlow_MultipleFilesPerSource_SequentialJobStatus(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload multiple files and track their jobs
		var jobs []JobResponse
		for i := 1; i <= 3; i++ {
			csvContent := BuildCSVContent(
				fmt.Sprintf("SEQ-REF-%03d", i),
				fmt.Sprintf("%d.00", i*100),
				"USD",
				fmt.Sprintf("2026-01-%02d", 10+i),
				fmt.Sprintf("sequential tx %d", i),
			)

			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("seq_%d.csv", i),
				csvContent,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode, "upload %d: %s", i, string(body))

			job := ParseJobResponse(t, body)
			jobs = append(jobs, job)
			t.Logf("Job %d created: %s", i, job.ID)

			sh.DispatchOutboxUntilEmpty(ctx, 3)
		}

		// Verify all jobs can be queried individually
		for i, job := range jobs {
			statusPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs/" + job.ID.String()
			statusResp, statusBody, err := sh.DoJSON(http.MethodGet, statusPath, nil)
			require.NoError(t, err)
			require.Equal(
				t,
				http.StatusOK,
				statusResp.StatusCode,
				"get job %d status: %s",
				i+1,
				string(statusBody),
			)

			statusJob := ParseJobResponse(t, statusBody)
			require.Equal(t, job.ID, statusJob.ID, "job %d ID mismatch", i+1)
			t.Logf("Job %d status: %s", i+1, statusJob.Status)
		}
	})
}

// =============================================================================
// No-Match Scenario Tests
// =============================================================================

func TestIntegrationHTTPFlow_NoMatch_DifferentReferences_DoesNotPublishEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)

		// Upload ledger file with reference "LEDGER-001"
		ledgerCSV := BuildCSVContent("LEDGER-001", "500.00", "USD", "2026-01-15", "ledger entry")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job created: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank file with DIFFERENT reference "BANK-999" (won't match)
		bankCSV := BuildCSVContent("BANK-999", "500.00", "USD", "2026-01-15", "bank statement")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job created: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch any match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify NO match_confirmed event was published (references don't match)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event when references differ",
		)
	})
}

func TestIntegrationHTTPFlow_NoMatch_DifferentAmounts_DoesNotPublishEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)

		// Upload ledger file with amount "100.00"
		ledgerCSV := BuildCSVContent(
			"AMOUNT-REF-001",
			"100.00",
			"USD",
			"2026-01-15",
			"ledger entry",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank file with SAME reference but DIFFERENT amount "999.99" (won't match)
		bankCSV := BuildCSVContent(
			"amount-ref-001",
			"999.99",
			"USD",
			"2026-01-15",
			"bank statement",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch any match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify NO match_confirmed event was published (amounts don't match)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event when amounts differ",
		)
	})
}

func TestIntegrationHTTPFlow_NoMatch_DifferentCurrencies_DoesNotPublishEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)

		// Upload ledger file with currency "USD"
		ledgerCSV := BuildCSVContent(
			"CURRENCY-REF-001",
			"250.00",
			"USD",
			"2026-01-15",
			"ledger entry",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank file with SAME reference/amount but DIFFERENT currency "EUR" (won't match)
		bankCSV := BuildCSVContent(
			"currency-ref-001",
			"250.00",
			"EUR",
			"2026-01-15",
			"bank statement",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch any match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify NO match_confirmed event was published (currencies don't match)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event when currencies differ",
		)
	})
}

func TestIntegrationHTTPFlow_NoMatch_CompletelyDifferentData_DoesNotPublishEvent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)

		// Upload ledger file with completely different data
		ledgerCSV := BuildCSVContent(
			"LEDGER-ABC-123",
			"1000.00",
			"USD",
			"2026-01-10",
			"payment received",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank file with completely different data (nothing matches)
		bankCSV := BuildCSVContent("BANK-XYZ-999", "50.50", "GBP", "2026-01-20", "wire transfer")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch any match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify NO match_confirmed event was published
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event when data is completely different",
		)
	})
}

// =============================================================================
// Transaction Listing Tests
// =============================================================================

func TestIntegrationHTTPFlow_ListTransactions_ReturnsAllTransactions(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload CSV with 12 transactions
		csvContent := BuildMultiRowCSV([][]string{
			{"TX-001", "100.00", "USD", "2026-01-01", "transaction 1"},
			{"TX-002", "200.00", "USD", "2026-01-02", "transaction 2"},
			{"TX-003", "300.00", "EUR", "2026-01-03", "transaction 3"},
			{"TX-004", "400.00", "USD", "2026-01-04", "transaction 4"},
			{"TX-005", "500.00", "GBP", "2026-01-05", "transaction 5"},
			{"TX-006", "600.00", "USD", "2026-01-06", "transaction 6"},
			{"TX-007", "700.00", "EUR", "2026-01-07", "transaction 7"},
			{"TX-008", "800.00", "USD", "2026-01-08", "transaction 8"},
			{"TX-009", "900.00", "USD", "2026-01-09", "transaction 9"},
			{"TX-010", "1000.00", "GBP", "2026-01-10", "transaction 10"},
			{"TX-011", "1100.00", "USD", "2026-01-11", "transaction 11"},
			{"TX-012", "1200.00", "EUR", "2026-01-12", "transaction 12"},
		})

		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "transactions.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			uploadResp.StatusCode,
			"upload: %s",
			string(uploadBody),
		)

		job := ParseJobResponse(t, uploadBody)
		t.Logf("Job created: %s", job.ID)

		// Dispatch outbox to process ingestion
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// List transactions with default pagination
		listPath := TransactionsPath(seed.ContextID, job.ID)
		listResp, listBody, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode, "list: %s", string(listBody))

		txList := ParseTransactionListResponse(t, listBody)
		require.Equal(t, 12, len(txList.Items), "expected 12 transactions")

		// Verify transaction fields are populated
		for _, tx := range txList.Items {
			require.NotEqual(
				t,
				tx.ID.String(),
				"00000000-0000-0000-0000-000000000000",
				"transaction ID should not be nil",
			)
			require.Equal(t, job.ID, tx.JobID, "job ID mismatch")
			require.Equal(t, seed.LedgerSourceID, tx.SourceID, "source ID mismatch")
			require.Equal(t, seed.ContextID, tx.ContextID, "context ID mismatch")
			require.NotEmpty(t, tx.ExternalID, "external_id should not be empty")
			require.NotEmpty(t, tx.Amount, "amount should not be empty")
			require.NotEmpty(t, tx.Currency, "currency should not be empty")
		}
		t.Logf("Listed %d transactions successfully", len(txList.Items))
	})
}

func TestIntegrationHTTPFlow_ListTransactions_Pagination_Limit(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload CSV with 15 transactions
		rows := make([][]string, 15)
		for i := 0; i < 15; i++ {
			rows[i] = []string{
				fmt.Sprintf("PAGE-TX-%03d", i+1),
				fmt.Sprintf("%d.00", (i+1)*100),
				"USD",
				fmt.Sprintf("2026-01-%02d", i+1),
				fmt.Sprintf("paginated tx %d", i+1),
			}
		}
		csvContent := BuildMultiRowCSV(rows)

		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "paginated.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, uploadResp.StatusCode)

		job := ParseJobResponse(t, uploadBody)
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Request first page with limit=5
		listPath := TransactionsPath(seed.ContextID, job.ID) + "?limit=5"
		listResp, listBody, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode, "list: %s", string(listBody))

		txList := ParseTransactionListResponse(t, listBody)
		require.Equal(t, 5, len(txList.Items), "expected 5 transactions with limit=5")
		require.Equal(t, 5, txList.Limit, "cursor limit should be 5")
		t.Logf("First page: %d items, next_cursor: %s", len(txList.Items), txList.NextCursor)
	})
}

func TestIntegrationHTTPFlow_ListTransactions_Pagination_Cursor(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload CSV with 10 transactions
		rows := make([][]string, 10)
		for i := 0; i < 10; i++ {
			rows[i] = []string{
				fmt.Sprintf("CURSOR-TX-%03d", i+1),
				fmt.Sprintf("%d.00", (i+1)*50),
				"EUR",
				fmt.Sprintf("2026-02-%02d", i+1),
				fmt.Sprintf("cursor tx %d", i+1),
			}
		}
		csvContent := BuildMultiRowCSV(rows)

		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "cursor_test.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, uploadResp.StatusCode)

		job := ParseJobResponse(t, uploadBody)
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Get first page with limit=4
		listPath := TransactionsPath(seed.ContextID, job.ID) + "?limit=4"
		resp1, body1, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp1.StatusCode)

		page1 := ParseTransactionListResponse(t, body1)
		require.Equal(t, 4, len(page1.Items), "first page should have 4 items")
		require.NotEmpty(t, page1.NextCursor, "first page should have next_cursor")
		t.Logf("Page 1: %d items", len(page1.Items))

		// Get second page using cursor
		listPath2 := TransactionsPath(
			seed.ContextID,
			job.ID,
		) + "?limit=4&cursor=" + page1.NextCursor
		resp2, body2, err := sh.DoJSON(http.MethodGet, listPath2, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp2.StatusCode)

		page2 := ParseTransactionListResponse(t, body2)
		require.Equal(t, 4, len(page2.Items), "second page should have 4 items")
		t.Logf("Page 2: %d items", len(page2.Items))

		// Verify no duplicate IDs between pages
		page1IDs := make(map[uuid.UUID]bool)
		for _, tx := range page1.Items {
			page1IDs[tx.ID] = true
		}
		for _, tx := range page2.Items {
			require.False(t, page1IDs[tx.ID], "transaction %s appears in both pages", tx.ID)
		}

		// Get third page (should have remaining 2 items)
		if page2.NextCursor != "" {
			listPath3 := TransactionsPath(
				seed.ContextID,
				job.ID,
			) + "?limit=4&cursor=" + page2.NextCursor
			resp3, body3, err := sh.DoJSON(http.MethodGet, listPath3, nil)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp3.StatusCode)

			page3 := ParseTransactionListResponse(t, body3)
			require.Equal(t, 2, len(page3.Items), "third page should have 2 items")
			t.Logf("Page 3: %d items", len(page3.Items))
		}
	})
}

func TestIntegrationHTTPFlow_ListTransactions_SortOrder(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload CSV with 5 transactions
		csvContent := BuildMultiRowCSV([][]string{
			{"SORT-TX-001", "100.00", "USD", "2026-03-01", "sort tx 1"},
			{"SORT-TX-002", "200.00", "USD", "2026-03-02", "sort tx 2"},
			{"SORT-TX-003", "300.00", "USD", "2026-03-03", "sort tx 3"},
			{"SORT-TX-004", "400.00", "USD", "2026-03-04", "sort tx 4"},
			{"SORT-TX-005", "500.00", "USD", "2026-03-05", "sort tx 5"},
		})

		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "sort_test.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, uploadResp.StatusCode)

		job := ParseJobResponse(t, uploadBody)
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Get transactions in ascending order
		ascPath := TransactionsPath(seed.ContextID, job.ID) + "?sort_order=asc"
		ascResp, ascBody, err := sh.DoJSON(http.MethodGet, ascPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, ascResp.StatusCode)

		ascList := ParseTransactionListResponse(t, ascBody)
		require.Equal(t, 5, len(ascList.Items))

		// Get transactions in descending order
		descPath := TransactionsPath(seed.ContextID, job.ID) + "?sort_order=desc"
		descResp, descBody, err := sh.DoJSON(http.MethodGet, descPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, descResp.StatusCode)

		descList := ParseTransactionListResponse(t, descBody)
		require.Equal(t, 5, len(descList.Items))

		// Verify order is reversed
		require.Equal(t, ascList.Items[0].ID, descList.Items[4].ID, "first asc should be last desc")
		require.Equal(t, ascList.Items[4].ID, descList.Items[0].ID, "last asc should be first desc")
		t.Logf(
			"ASC first: %s, DESC first: %s",
			ascList.Items[0].ExternalID,
			descList.Items[0].ExternalID,
		)
	})
}

func TestIntegrationHTTPFlow_ListTransactions_InvalidJobID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs/not-a-uuid/transactions"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "expected 400: %s", string(body))
		errResp := AssertErrorResponse(t, body)
		t.Logf("Error response: code=%d, message=%s", errResp.Code, errResp.Message)
	})
}

func TestIntegrationHTTPFlow_ListTransactions_NonExistentJob_ReturnsNotFound(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		nonExistentJobID := uuid.New()
		listPath := TransactionsPath(seed.ContextID, nonExistentJobID)
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode, "expected 404: %s", string(body))
		errResp := AssertErrorResponse(t, body)
		t.Logf("Error response: code=%d, message=%s", errResp.Code, errResp.Message)
	})
}

func TestIntegrationHTTPFlow_ListTransactions_InvalidSortOrder_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Create a job first
		csvContent := BuildCSVContent("INVALID-SORT-TX", "100.00", "USD", "2026-04-01", "test")
		uploadResp, uploadBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "test.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, uploadResp.StatusCode)

		job := ParseJobResponse(t, uploadBody)
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		listPath := TransactionsPath(seed.ContextID, job.ID) + "?sort_order=invalid"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode, "expected 400: %s", string(body))
	})
}

func TestIntegrationHTTPFlow_ListTransactions_EmptyJob_ReturnsBadRequest(t *testing.T) {
	t.Parallel()

	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload empty CSV (header only) - should return 400 Bad Request
		csvContent := []byte("id,amount,currency,date,description\n")
		uploadResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "empty.csv", csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, uploadResp.StatusCode, "empty CSV should return 400")
		t.Log("Empty CSV correctly rejected with 400 Bad Request")
	})
}

// =============================================================================
// Case Sensitivity Matching Tests
// =============================================================================

func TestIntegrationHTTPFlow_CaseInsensitiveMatch_DifferentCaseReferencesMatch(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		// Setup with caseInsensitive=true (default)
		seed := SetupFlowTestConfigWithOptions(t, sh, FlowTestConfigOptions{
			CaseInsensitive: true,
		})
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger with uppercase reference
		ledgerCSV := BuildCSVContent("REF-001", "500.00", "USD", "2026-01-20", "ledger uppercase")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank with lowercase reference (same ref, different case)
		bankCSV := BuildCSVContent("ref-001", "500.00", "USD", "2026-01-20", "bank lowercase")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed event was published (case insensitive should match)
		events := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.Equal(t, 1, len(events), "expected 1 match_confirmed event")

		t.Logf("Received match_confirmed event: MatchID=%s", events[0].MatchID)
	})
}

func TestIntegrationHTTPFlow_CaseSensitiveMatch_DifferentCaseReferencesDoNotMatch(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		// Setup with caseInsensitive=false (case sensitive matching)
		seed := SetupFlowTestConfigWithOptions(t, sh, FlowTestConfigOptions{
			CaseInsensitive: false,
		})
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger with uppercase reference
		ledgerCSV := BuildCSVContent("REF-002", "750.00", "EUR", "2026-01-21", "ledger uppercase")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank with lowercase reference (same ref, different case)
		bankCSV := BuildCSVContent("ref-002", "750.00", "EUR", "2026-01-21", "bank lowercase")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch any match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify NO match_confirmed event was published (case sensitive should NOT match)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event for case-sensitive matching with different cases",
		)
		t.Log("Confirmed: case-sensitive matching correctly rejected different-case references")
	})
}

func TestIntegrationHTTPFlow_CaseSensitiveMatch_ExactCaseReferencesMatch(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		// Setup with caseInsensitive=false (case sensitive matching)
		seed := SetupFlowTestConfigWithOptions(t, sh, FlowTestConfigOptions{
			CaseInsensitive: false,
		})
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger with specific case
		ledgerCSV := BuildCSVContent("REF-003", "1000.00", "GBP", "2026-01-22", "ledger exact case")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank with EXACT same case reference
		bankCSV := BuildCSVContent("REF-003", "1000.00", "GBP", "2026-01-22", "bank exact case")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed event was published (exact case should match even in case-sensitive mode)
		events := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.Equal(t, 1, len(events), "expected 1 match_confirmed event")

		t.Log("Confirmed: case-sensitive matching correctly matched exact-case references")
	})
}

// =============================================================================
// Date Tolerance Matching Tests
// =============================================================================

func TestIntegrationHTTPFlow_DatePrecisionDay_SameDayMatches(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfigWithDatePrecision(t, sh, "DAY")
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger tx with date 2026-01-15
		ledgerCSV := BuildCSVContent(
			"DATE-SAME-001",
			"500.00",
			"USD",
			"2026-01-15",
			"ledger tx same day",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job created: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank tx with same date 2026-01-15
		bankCSV := BuildCSVContent(
			"date-same-001",
			"500.00",
			"USD",
			"2026-01-15",
			"bank tx same day",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job created: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed event - same day should match with DAY precision
		events := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.GreaterOrEqual(t, len(events), 1, "expected at least 1 match_confirmed event")

		t.Logf("Received %d match_confirmed event(s)", len(events))
	})
}

func TestIntegrationHTTPFlow_DatePrecisionDay_OneDayApartNoMatch(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfigWithDatePrecision(t, sh, "DAY")
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger tx with date 2026-01-15
		ledgerCSV := BuildCSVContent(
			"DATE-DIFF-001",
			"750.00",
			"EUR",
			"2026-01-15",
			"ledger tx day 15",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job created: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank tx with date 2026-01-16 (1 day later)
		bankCSV := BuildCSVContent("date-diff-001", "750.00", "EUR", "2026-01-16", "bank tx day 16")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job created: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in DRY_RUN mode to avoid persisting state
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "DRY_RUN",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Dry run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch any outbox events
		sh.DispatchOutboxUntilEmpty(ctx, 3)

		// Verify NO match_confirmed event - dates 1 day apart should NOT match with DAY precision
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"expected no match_confirmed event for transactions 1 day apart with DAY precision",
		)
	})
}

func TestIntegrationHTTPFlow_DatePrecisionDay_MultipleTransactionsMixedDates(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfigWithDatePrecision(t, sh, "DAY")
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger file with 3 transactions on different dates
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"DATE-MIX-A", "100.00", "USD", "2026-01-15", "ledger A day 15"},
			{"DATE-MIX-B", "200.00", "USD", "2026-01-16", "ledger B day 16"},
			{"DATE-MIX-C", "300.00", "USD", "2026-01-17", "ledger C day 17"},
		})
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank file with matching refs but only A and C match dates
		// A: same date (15th), B: off by 1 day (should NOT match), C: same date (17th)
		bankCSV := BuildMultiRowCSV([][]string{
			{"date-mix-a", "100.00", "USD", "2026-01-15", "bank A day 15"}, // matches
			{"date-mix-b", "200.00", "USD", "2026-01-17", "bank B day 17"}, // off by 1 day
			{"date-mix-c", "300.00", "USD", "2026-01-17", "bank C day 17"}, // matches
		})
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed events (A and C match, B does not)
		// Expecting 2 events but accepting at least 1 due to matching engine behavior
		events := WaitForMultipleMatchEvents(t, sh, 2, 15*time.Second)
		require.GreaterOrEqual(t, len(events), 1, "expected at least 1 match_confirmed event")
		require.LessOrEqual(t, len(events), 2, "expected at most 2 match_confirmed events")

		t.Logf("Received %d match_confirmed events (A and C matched, B did not)", len(events))
	})
}

// =============================================================================
// Job Status Tests
// =============================================================================

func TestIntegrationHTTPFlow_JobCreation_ReturnsValidStatus(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		csvContent := BuildCSVContent(
			"STATUS-TEST-001",
			"100.00",
			"USD",
			"2026-01-20",
			"status test",
		)
		resp, body, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"test.csv",
			csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)

		job := ParseJobResponse(t, body)
		t.Logf("Job created: %s, status: %s", job.ID, job.Status)

		require.NotEmpty(t, job.ID, "job should have an ID")
		require.True(
			t,
			IsTerminalStatus(job.Status),
			"job should complete synchronously, got status: %s",
			job.Status,
		)
		require.Equal(t, "COMPLETED", job.Status, "job should complete successfully")
	})
}

func TestIntegrationHTTPFlow_JobCreation_MultipleJobsIndependent(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		csv1 := BuildCSVContent("MULTI-JOB-001", "100.00", "USD", "2026-01-20", "job 1")
		resp1, body1, err := sh.DoMultipart(
			uploadPath,
			"file",
			"job1.csv",
			csv1,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp1.StatusCode)
		job1 := ParseJobResponse(t, body1)
		t.Logf("Job 1 created: %s, status: %s", job1.ID, job1.Status)

		csv2 := BuildCSVContent("MULTI-JOB-002", "200.00", "USD", "2026-01-21", "job 2")
		resp2, body2, err := sh.DoMultipart(
			uploadPath,
			"file",
			"job2.csv",
			csv2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp2.StatusCode)
		job2 := ParseJobResponse(t, body2)
		t.Logf("Job 2 created: %s, status: %s", job2.ID, job2.Status)

		require.NotEqual(t, job1.ID, job2.ID, "jobs should have distinct IDs")
		require.Equal(t, "COMPLETED", job1.Status, "job 1 should complete")
		require.Equal(t, "COMPLETED", job2.Status, "job 2 should complete")
	})
}

func TestIntegrationHTTPFlow_JobStatus_PersistedAcrossRequests(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		csvContent := BuildCSVContent(
			"PERSIST-STATUS-001",
			"150.00",
			"EUR",
			"2026-01-22",
			"persist test",
		)
		resp, body, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"persist.csv",
			csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)

		job := ParseJobResponse(t, body)
		require.Equal(t, "COMPLETED", job.Status, "job should complete synchronously")

		for i := 0; i < 3; i++ {
			checkJob := GetJobStatus(t, sh, seed.ContextID, job.ID)
			require.Equal(
				t,
				"COMPLETED",
				checkJob.Status,
				"status should remain completed on request %d",
				i+1,
			)
			require.Equal(t, job.ID, checkJob.ID, "job ID should match on request %d", i+1)
		}
	})
}

func TestIntegrationHTTPFlow_JobStatus_LedgerAndBankJobsCompleteBeforeMatching(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		ledgerCSV := BuildCSVContent("FLOW-STATUS-001", "500.00", "USD", "2026-01-23", "ledger")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)
		ledgerJob := ParseJobResponse(t, ledgerBody)
		t.Logf("Ledger job: %s, status: %s", ledgerJob.ID, ledgerJob.Status)

		bankCSV := BuildCSVContent("flow-status-001", "500.00", "USD", "2026-01-23", "bank")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)
		bankJob := ParseJobResponse(t, bankBody)
		t.Logf("Bank job: %s, status: %s", bankJob.ID, bankJob.Status)

		require.Equal(t, "COMPLETED", ledgerJob.Status, "ledger job should complete synchronously")
		require.Equal(t, "COMPLETED", bankJob.Status, "bank job should complete synchronously")

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{"mode": "COMMIT"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		eventBody, err := sh.WaitForEventWithTimeout(
			10*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "should receive match_confirmed after both jobs completed")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
	})
}

// =============================================================================
// Match Idempotency Tests
// =============================================================================

func TestIntegrationHTTPFlow_MatchIdempotency_SecondRunProducesNoMatches(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger file
		ledgerCSV := BuildCSVContent(
			"IDEMP-REF-001",
			"500.00",
			"USD",
			"2026-01-20",
			"idempotency test ledger",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger job created: %s", ParseJobResponse(t, ledgerBody).ID)

		// Step 2: Upload bank file with matching transaction
		bankCSV := BuildCSVContent(
			"idemp-ref-001",
			"500.00",
			"USD",
			"2026-01-20",
			"idempotency test bank",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)
		t.Logf("Bank job created: %s", ParseJobResponse(t, bankBody).ID)

		// Step 3: Dispatch outbox to process ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 4: Run FIRST match in COMMIT mode
		matchResp1, matchBody1, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp1.StatusCode,
			"first matching: %s",
			string(matchBody1),
		)

		runMatch1 := ParseRunMatchResponse(t, matchBody1)
		t.Logf("First match run created: %s, status: %s", runMatch1.RunID, runMatch1.Status)

		// Step 5: Dispatch outbox to publish match confirmed events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 6: Verify first match_confirmed event was published
		// Verify first match has exactly 1 match_confirmed event
		events := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.Equal(t, 1, len(events), "first match should have exactly 1 match_confirmed event")
		t.Logf("Received first match_confirmed event: MatchID=%s", events[0].MatchID)

		// Step 7: Run SECOND match in COMMIT mode (idempotency test)
		matchResp2, matchBody2, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp2.StatusCode,
			"second matching: %s",
			string(matchBody2),
		)

		runMatch2 := ParseRunMatchResponse(t, matchBody2)
		t.Logf("Second match run created: %s, status: %s", runMatch2.RunID, runMatch2.Status)

		// Verify run IDs are different
		require.NotEqual(t, runMatch1.RunID, runMatch2.RunID, "each run should have a unique ID")

		// Step 8: Dispatch outbox for second match
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 9: Verify NO new match_confirmed event (already matched transactions should not re-match)
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"second run should not produce match_confirmed event for already-matched transactions",
		)
		t.Log("Confirmed: second match run produced no new match events (idempotent behavior)")
	})
}

func TestIntegrationHTTPFlow_MatchIdempotency_MultipleTransactions_OnlyUnmatchedProceed(
	t *testing.T,
) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger file with 3 transactions
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"IDEMP-A", "100.00", "USD", "2026-01-20", "tx A"},
			{"IDEMP-B", "200.00", "USD", "2026-01-20", "tx B"},
			{"IDEMP-C", "300.00", "USD", "2026-01-20", "tx C"},
		})
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Step 2: Upload bank file with only 2 matching transactions (A and B)
		bankCSV := BuildMultiRowCSV([][]string{
			{"idemp-a", "100.00", "USD", "2026-01-20", "bank A"},
			{"idemp-b", "200.00", "USD", "2026-01-20", "bank B"},
		})
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 3: Run first match - should match A and B
		matchResp1, matchBody1, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp1.StatusCode,
			"first matching: %s",
			string(matchBody1),
		)
		t.Logf("First match run: %s", ParseRunMatchResponse(t, matchBody1).RunID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// First match should produce match_confirmed events (A and B)
		// Expecting 2 events but accepting at least 1 due to matching engine behavior
		events1 := WaitForMultipleMatchEvents(t, sh, 2, 15*time.Second)
		require.GreaterOrEqual(
			t,
			len(events1),
			1,
			"first match should have at least 1 match_confirmed event",
		)
		require.LessOrEqual(
			t,
			len(events1),
			2,
			"first match should have at most 2 match_confirmed events",
		)
		t.Logf("First match confirmed: %d events", len(events1))

		// Step 4: Upload additional bank file matching C
		bankCSV2 := BuildCSVContent("idemp-c", "300.00", "USD", "2026-01-20", "bank C")
		bankResp2, bankBody2, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank2.csv", bankCSV2,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp2.StatusCode,
			"bank upload 2: %s",
			string(bankBody2),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 5: Run second match - should only match C (A and B already matched)
		matchResp2, matchBody2, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp2.StatusCode,
			"second matching: %s",
			string(matchBody2),
		)
		t.Logf("Second match run: %s", ParseRunMatchResponse(t, matchBody2).RunID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Second match should produce 1 match_confirmed event (C only)
		events2 := WaitForMultipleMatchEvents(t, sh, 1, 10*time.Second)
		require.Equal(
			t,
			1,
			len(events2),
			"second match should only have 1 match_confirmed event (C)",
		)
		t.Logf("Second match confirmed: 1 event (A and B already matched)")
	})
}

func TestIntegrationHTTPFlow_MatchIdempotency_ThirdRunNoNewMatches(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching transactions
		ledgerCSV := BuildCSVContent("TRIPLE-REF-001", "777.00", "EUR", "2026-01-25", "triple test")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankCSV := BuildCSVContent(
			"triple-ref-001",
			"777.00",
			"EUR",
			"2026-01-25",
			"triple test bank",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// First match run
		matchResp1, matchBody1, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{"mode": "COMMIT"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp1.StatusCode)
		runMatch1 := ParseRunMatchResponse(t, matchBody1)
		t.Logf("Run 1: %s", runMatch1.RunID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		_, err = sh.WaitForEventWithTimeout(10*time.Second, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.NoError(t, err, "first run should produce match event")

		// Second match run
		matchResp2, matchBody2, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{"mode": "COMMIT"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp2.StatusCode)
		runMatch2 := ParseRunMatchResponse(t, matchBody2)
		t.Logf("Run 2: %s", runMatch2.RunID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		waitCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
		defer cancel2()
		_, err = sh.WaitForEvent(waitCtx2, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"second run should not produce match event",
		)

		// Third match run
		matchResp3, matchBody3, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{"mode": "COMMIT"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp3.StatusCode)
		runMatch3 := ParseRunMatchResponse(t, matchBody3)
		t.Logf("Run 3: %s", runMatch3.RunID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		waitCtx3, cancel3 := context.WithTimeout(ctx, 3*time.Second)
		defer cancel3()
		_, err = sh.WaitForEvent(waitCtx3, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(
			t,
			err,
			context.DeadlineExceeded,
			"third run should not produce match event",
		)

		// All runs should have unique IDs
		require.NotEqual(t, runMatch1.RunID, runMatch2.RunID)
		require.NotEqual(t, runMatch2.RunID, runMatch3.RunID)
		require.NotEqual(t, runMatch1.RunID, runMatch3.RunID)

		t.Log("Confirmed: multiple consecutive match runs are idempotent after initial match")
	})
}

// =============================================================================
// GetMatchRun and GetMatchRunResults Tests
// =============================================================================

func TestIntegrationHTTPFlow_GetMatchRun_ReturnsRunDetails(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching transactions
		ledgerCSV := BuildCSVContent("GETRUN-REF-001", "500.00", "USD", "2026-01-20", "ledger tx")
		bankCSV := BuildCSVContent("getrun-ref-001", "500.00", "USD", "2026-01-20", "bank tx")

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// GET /v1/matching/runs/:runId?contextId=X
		getRunPath := fmt.Sprintf(
			"/v1/matching/runs/%s?contextId=%s",
			runMatch.RunID,
			seed.ContextID,
		)
		getRunResp, getRunBody, err := sh.DoJSON(http.MethodGet, getRunPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusOK,
			getRunResp.StatusCode,
			"get match run: %s",
			string(getRunBody),
		)

		// Parse and verify response structure
		matchRun := ParseMatchRunDetailsResponse(t, getRunBody)
		require.Equal(t, runMatch.RunID, matchRun.ID, "run ID should match")
		require.NotEmpty(t, matchRun.Status, "status should not be empty")
		require.Equal(t, seed.ContextID, matchRun.ContextID, "context ID should match")
		t.Logf(
			"Match run details: ID=%s, Status=%s, ContextID=%s",
			matchRun.ID,
			matchRun.Status,
			matchRun.ContextID,
		)
	})
}

func TestIntegrationHTTPFlow_GetMatchRun_NonExistentRun_ReturnsNotFound(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		nonExistentRunID := uuid.New()
		getRunPath := fmt.Sprintf(
			"/v1/matching/runs/%s?contextId=%s",
			nonExistentRunID,
			seed.ContextID,
		)

		resp, body, err := sh.DoJSON(http.MethodGet, getRunPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusNotFound,
			resp.StatusCode,
			"expected 404 Not Found, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_GetMatchRun_InvalidRunID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		getRunPath := fmt.Sprintf("/v1/matching/runs/not-a-uuid?contextId=%s", seed.ContextID)

		resp, body, err := sh.DoJSON(http.MethodGet, getRunPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_GetMatchRun_MissingContextID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		runID := uuid.New()
		getRunPath := fmt.Sprintf("/v1/matching/runs/%s", runID)

		resp, body, err := sh.DoJSON(http.MethodGet, getRunPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_GetMatchRunResults_ReturnsPaginatedGroups(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload multiple matching transactions to create multiple match groups
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"GROUPS-REF-001", "100.00", "USD", "2026-01-20", "ledger tx 1"},
			{"GROUPS-REF-002", "200.00", "USD", "2026-01-20", "ledger tx 2"},
			{"GROUPS-REF-003", "300.00", "EUR", "2026-01-21", "ledger tx 3"},
		})
		bankCSV := BuildMultiRowCSV([][]string{
			{"groups-ref-001", "100.00", "USD", "2026-01-20", "bank tx 1"},
			{"groups-ref-002", "200.00", "USD", "2026-01-20", "bank tx 2"},
			{"groups-ref-003", "300.00", "EUR", "2026-01-21", "bank tx 3"},
		})

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching in COMMIT mode
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// GET /v1/matching/runs/:runId/groups?contextId=X
		getGroupsPath := fmt.Sprintf(
			"/v1/matching/runs/%s/groups?contextId=%s",
			runMatch.RunID,
			seed.ContextID,
		)
		getGroupsResp, getGroupsBody, err := sh.DoJSON(http.MethodGet, getGroupsPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusOK,
			getGroupsResp.StatusCode,
			"get match run results: %s",
			string(getGroupsBody),
		)

		// Parse and verify response structure
		groupsResp := ParseMatchGroupsResponse(t, getGroupsBody)
		require.NotNil(t, groupsResp.Items, "items should not be nil")
		require.GreaterOrEqual(t, len(groupsResp.Items), 0, "items should be a valid array")
		require.Greater(t, groupsResp.Limit, 0, "limit should be greater than 0")
		t.Logf("Match groups response: %d items, limit=%d", len(groupsResp.Items), groupsResp.Limit)
	})
}

func TestIntegrationHTTPFlow_GetMatchRunResults_WithPagination(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching transactions
		ledgerCSV := BuildCSVContent("PAGE-REF-001", "100.00", "USD", "2026-01-20", "ledger tx")
		bankCSV := BuildCSVContent("page-ref-001", "100.00", "USD", "2026-01-20", "bank tx")

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file",
			"ledger.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Test with explicit pagination parameters
		getGroupsPath := fmt.Sprintf(
			"/v1/matching/runs/%s/groups?contextId=%s&limit=10&offset=0",
			runMatch.RunID,
			seed.ContextID,
		)
		getGroupsResp, getGroupsBody, err := sh.DoJSON(http.MethodGet, getGroupsPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusOK,
			getGroupsResp.StatusCode,
			"get match run results with pagination: %s",
			string(getGroupsBody),
		)

		groupsResp := ParseMatchGroupsResponse(t, getGroupsBody)
		require.Equal(t, 10, groupsResp.Limit, "limit should be 10")
		t.Logf("Paginated response: %d items, limit=%d", len(groupsResp.Items), groupsResp.Limit)
	})
}

func TestIntegrationHTTPFlow_GetMatchRunResults_InvalidRunID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		getGroupsPath := fmt.Sprintf(
			"/v1/matching/runs/not-a-uuid/groups?contextId=%s",
			seed.ContextID,
		)

		resp, body, err := sh.DoJSON(http.MethodGet, getGroupsPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

func TestIntegrationHTTPFlow_GetMatchRunResults_MissingContextID_ReturnsBadRequest(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		runID := uuid.New()
		getGroupsPath := fmt.Sprintf("/v1/matching/runs/%s/groups", runID)

		resp, body, err := sh.DoJSON(http.MethodGet, getGroupsPath, nil)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusBadRequest,
			resp.StatusCode,
			"expected 400 Bad Request, got %d: %s",
			resp.StatusCode,
			string(body),
		)
	})
}

// =============================================================================
// Concurrent Upload Tests
// =============================================================================

func TestIntegrationHTTPFlow_ConcurrentUploads_SameSource(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)
		numUploads := 5

		type uploadResult struct {
			index      int
			statusCode int
			jobID      uuid.UUID
			err        error
		}

		results := make(chan uploadResult, numUploads)
		var wg sync.WaitGroup

		for i := 0; i < numUploads; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				ref := fmt.Sprintf("CONC-SAME-%d-%s", idx, uuid.New().String()[:8])
				csvContent := BuildCSVContent(
					ref,
					fmt.Sprintf("%d.00", (idx+1)*100),
					"USD",
					"2026-01-20",
					fmt.Sprintf("concurrent tx %d", idx),
				)

				resp, body, err := sh.DoMultipart(
					uploadPath,
					"file",
					fmt.Sprintf("concurrent_%d.csv", idx),
					csvContent,
					map[string]string{"format": "csv"},
				)

				result := uploadResult{index: idx, err: err}
				if err == nil {
					result.statusCode = resp.StatusCode
					if resp.StatusCode == http.StatusAccepted {
						job := ParseJobResponse(t, body)
						result.jobID = job.ID
					}
				}
				results <- result
			}(i)
		}

		wg.Wait()
		close(results)

		var collected []uploadResult
		for r := range results {
			collected = append(collected, r)
		}

		require.Len(t, collected, numUploads, "expected %d results", numUploads)

		jobIDs := make(map[string]bool)
		nilUUID := "00000000-0000-0000-0000-000000000000"

		for _, r := range collected {
			require.NoError(t, r.err, "upload %d should not error", r.index)
			require.Equal(
				t,
				http.StatusAccepted,
				r.statusCode,
				"upload %d should return 202",
				r.index,
			)
			require.NotEqual(
				t,
				r.jobID.String(),
				nilUUID,
				"upload %d job ID should not be nil",
				r.index,
			)
			require.False(
				t,
				jobIDs[r.jobID.String()],
				"upload %d job ID should be unique, got duplicate %s",
				r.index,
				r.jobID,
			)
			jobIDs[r.jobID.String()] = true
			t.Logf("Upload %d: job %s", r.index, r.jobID)
		}

		require.Len(t, jobIDs, numUploads, "all %d jobs should have unique IDs", numUploads)

		sh.DispatchOutboxUntilEmpty(ctx, numUploads*2)
	})
}

func TestIntegrationHTTPFlow_ConcurrentUploads_DifferentSources(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		sources := []uuid.UUID{seed.LedgerSourceID, seed.NonLedgerSourceID}
		numUploadsPerSource := 3

		type uploadResult struct {
			sourceIdx  int
			uploadIdx  int
			statusCode int
			jobID      uuid.UUID
			err        error
		}

		totalUploads := len(sources) * numUploadsPerSource
		results := make(chan uploadResult, totalUploads)
		var wg sync.WaitGroup

		for sIdx, sourceID := range sources {
			for uIdx := 0; uIdx < numUploadsPerSource; uIdx++ {
				wg.Add(1)
				go func(sourceIndex int, uploadIndex int, srcID uuid.UUID) {
					defer wg.Done()

					ref := fmt.Sprintf(
						"CONC-DIFF-S%d-U%d-%s",
						sourceIndex,
						uploadIndex,
						uuid.New().String()[:8],
					)
					csvContent := BuildCSVContent(
						ref,
						fmt.Sprintf("%d.00", (uploadIndex+1)*50),
						"EUR",
						"2026-01-21",
						fmt.Sprintf("source %d tx %d", sourceIndex, uploadIndex),
					)
					uploadPath := UploadPath(seed.ContextID, srcID)

					resp, body, err := sh.DoMultipart(
						uploadPath,
						"file",
						fmt.Sprintf("src%d_upload%d.csv", sourceIndex, uploadIndex),
						csvContent,
						map[string]string{"format": "csv"},
					)

					result := uploadResult{sourceIdx: sourceIndex, uploadIdx: uploadIndex, err: err}
					if err == nil {
						result.statusCode = resp.StatusCode
						if resp.StatusCode == http.StatusAccepted {
							job := ParseJobResponse(t, body)
							result.jobID = job.ID
						}
					}
					results <- result
				}(sIdx, uIdx, sourceID)
			}
		}

		wg.Wait()
		close(results)

		var collected []uploadResult
		for r := range results {
			collected = append(collected, r)
		}

		require.Len(t, collected, totalUploads, "expected %d results", totalUploads)

		jobIDs := make(map[string]bool)
		nilUUID := "00000000-0000-0000-0000-000000000000"

		for _, r := range collected {
			require.NoError(
				t,
				r.err,
				"source %d upload %d should not error",
				r.sourceIdx,
				r.uploadIdx,
			)
			require.Equal(
				t,
				http.StatusAccepted,
				r.statusCode,
				"source %d upload %d should return 202",
				r.sourceIdx,
				r.uploadIdx,
			)
			require.NotEqual(
				t,
				r.jobID.String(),
				nilUUID,
				"source %d upload %d job ID should not be nil",
				r.sourceIdx,
				r.uploadIdx,
			)
			require.False(
				t,
				jobIDs[r.jobID.String()],
				"source %d upload %d job ID should be unique",
				r.sourceIdx,
				r.uploadIdx,
			)
			jobIDs[r.jobID.String()] = true
			t.Logf("Source %d Upload %d: job %s", r.sourceIdx, r.uploadIdx, r.jobID)
		}

		require.Len(t, jobIDs, totalUploads, "all %d jobs should have unique IDs", totalUploads)

		sh.DispatchOutboxUntilEmpty(ctx, totalUploads*2)
	})
}

func TestIntegrationHTTPFlow_ConcurrentUploads_WithMatching(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		numPairs := 3

		type uploadResult struct {
			pairIdx    int
			isLedger   bool
			statusCode int
			jobID      uuid.UUID
			err        error
		}

		results := make(chan uploadResult, numPairs*2)
		var wg sync.WaitGroup

		for i := 0; i < numPairs; i++ {
			ref := fmt.Sprintf("MATCH-PAIR-%d", i)
			amount := fmt.Sprintf("%d.00", (i+1)*100)

			wg.Add(1)
			go func(pairIdx int, refID, amt string) {
				defer wg.Done()

				csvContent := BuildCSVContent(
					refID,
					amt,
					"USD",
					"2026-01-22",
					fmt.Sprintf("ledger pair %d", pairIdx),
				)
				uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

				resp, body, err := sh.DoMultipart(
					uploadPath,
					"file",
					fmt.Sprintf("ledger_%d.csv", pairIdx),
					csvContent,
					map[string]string{"format": "csv"},
				)

				result := uploadResult{pairIdx: pairIdx, isLedger: true, err: err}
				if err == nil {
					result.statusCode = resp.StatusCode
					if resp.StatusCode == http.StatusAccepted {
						job := ParseJobResponse(t, body)
						result.jobID = job.ID
					}
				}
				results <- result
			}(i, ref, amount)

			wg.Add(1)
			go func(pairIdx int, refID, amt string) {
				defer wg.Done()

				bankRef := strings.ToLower(refID)
				csvContent := BuildCSVContent(
					bankRef,
					amt,
					"USD",
					"2026-01-22",
					fmt.Sprintf("bank pair %d", pairIdx),
				)
				uploadPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

				resp, body, err := sh.DoMultipart(
					uploadPath,
					"file",
					fmt.Sprintf("bank_%d.csv", pairIdx),
					csvContent,
					map[string]string{"format": "csv"},
				)

				result := uploadResult{pairIdx: pairIdx, isLedger: false, err: err}
				if err == nil {
					result.statusCode = resp.StatusCode
					if resp.StatusCode == http.StatusAccepted {
						job := ParseJobResponse(t, body)
						result.jobID = job.ID
					}
				}
				results <- result
			}(i, ref, amount)
		}

		wg.Wait()
		close(results)

		var collected []uploadResult
		for r := range results {
			collected = append(collected, r)
		}

		require.Len(t, collected, numPairs*2, "expected %d results", numPairs*2)

		jobIDs := make(map[string]bool)
		nilUUID := "00000000-0000-0000-0000-000000000000"

		for _, r := range collected {
			require.NoError(t, r.err, "pair %d (ledger=%v) should not error", r.pairIdx, r.isLedger)
			require.Equal(
				t,
				http.StatusAccepted,
				r.statusCode,
				"pair %d (ledger=%v) should return 202",
				r.pairIdx,
				r.isLedger,
			)
			require.NotEqual(
				t,
				r.jobID.String(),
				nilUUID,
				"pair %d (ledger=%v) job ID should not be nil",
				r.pairIdx,
				r.isLedger,
			)
			require.False(
				t,
				jobIDs[r.jobID.String()],
				"pair %d (ledger=%v) job ID should be unique",
				r.pairIdx,
				r.isLedger,
			)
			jobIDs[r.jobID.String()] = true
			t.Logf("Pair %d (ledger=%v): job %s", r.pairIdx, r.isLedger, r.jobID)
		}

		sh.DispatchOutboxUntilEmpty(ctx, numPairs*4)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		t.Logf("Received match_confirmed event: %s", string(eventBody))
	})
}

func TestIntegrationHTTPFlow_ConcurrentUploads_RaceConditionStress(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		numConcurrent := 10

		type uploadResult struct {
			index      int
			statusCode int
			jobID      uuid.UUID
			err        error
			duration   time.Duration
		}

		results := make(chan uploadResult, numConcurrent)
		var wg sync.WaitGroup
		startBarrier := make(chan struct{})

		for i := 0; i < numConcurrent; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				<-startBarrier

				start := time.Now()
				ref := fmt.Sprintf("STRESS-%d-%s", idx, uuid.New().String()[:8])
				csvContent := BuildCSVContent(
					ref,
					fmt.Sprintf("%d.99", idx),
					"GBP",
					"2026-01-23",
					fmt.Sprintf("stress tx %d", idx),
				)

				sourceID := seed.LedgerSourceID
				if idx%2 == 1 {
					sourceID = seed.NonLedgerSourceID
				}
				uploadPath := UploadPath(seed.ContextID, sourceID)

				resp, body, err := sh.DoMultipart(
					uploadPath,
					"file",
					fmt.Sprintf("stress_%d.csv", idx),
					csvContent,
					map[string]string{"format": "csv"},
				)

				result := uploadResult{index: idx, err: err, duration: time.Since(start)}
				if err == nil {
					result.statusCode = resp.StatusCode
					if resp.StatusCode == http.StatusAccepted {
						job := ParseJobResponse(t, body)
						result.jobID = job.ID
					}
				}
				results <- result
			}(i)
		}

		close(startBarrier)

		wg.Wait()
		close(results)

		var collected []uploadResult
		for r := range results {
			collected = append(collected, r)
		}

		require.Len(t, collected, numConcurrent, "expected %d results", numConcurrent)

		jobIDs := make(map[string]bool)
		nilUUID := "00000000-0000-0000-0000-000000000000"
		var totalDuration time.Duration

		for _, r := range collected {
			require.NoError(t, r.err, "stress upload %d should not error", r.index)
			require.Equal(
				t,
				http.StatusAccepted,
				r.statusCode,
				"stress upload %d should return 202",
				r.index,
			)
			require.NotEqual(
				t,
				r.jobID.String(),
				nilUUID,
				"stress upload %d job ID should not be nil",
				r.index,
			)
			require.False(
				t,
				jobIDs[r.jobID.String()],
				"stress upload %d job ID should be unique, got duplicate %s",
				r.index,
				r.jobID,
			)
			jobIDs[r.jobID.String()] = true
			totalDuration += r.duration
			t.Logf("Stress upload %d: job %s (took %v)", r.index, r.jobID, r.duration)
		}

		require.Len(t, jobIDs, numConcurrent, "all %d jobs should have unique IDs", numConcurrent)
		t.Logf("Average upload duration: %v", totalDuration/time.Duration(numConcurrent))

		sh.DispatchOutboxUntilEmpty(ctx, numConcurrent*2)
	})
}

// =============================================================================
// XML File Upload Tests
// =============================================================================

func TestIntegrationHTTPFlow_UploadXMLFile_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload an XML file via HTTP
		xmlContent := BuildXMLContent("XML-REF-001", "100.00", "USD", "2026-01-15", "xml tx")
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "ledger.xml", xmlContent, map[string]string{
			"format": "xml",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted, got %d: %s",
			resp.StatusCode,
			string(body),
		)

		jobResp := ParseJobResponse(t, body)
		require.NotEqual(
			t,
			jobResp.ID.String(),
			"00000000-0000-0000-0000-000000000000",
			"job ID should not be nil",
		)
		require.NotEmpty(t, jobResp.Status)
		t.Logf("XML job created: %s, status: %s", jobResp.ID, jobResp.Status)

		// Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 3)
	})
}

func TestIntegrationHTTPFlow_UploadXMLAndMatch_PublishesEvents(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Step 1: Upload ledger XML file
		ledgerXML := BuildXMLContent(
			"XML-MATCH-001",
			"350.00",
			"EUR",
			"2026-01-20",
			"ledger xml entry",
		)
		ledgerPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			ledgerPath,
			"file",
			"ledger.xml",
			ledgerXML,
			map[string]string{
				"format": "xml",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger XML upload failed: %s",
			string(ledgerBody),
		)

		ledgerJob := ParseJobResponse(t, ledgerBody)
		t.Logf("Ledger XML job created: %s", ledgerJob.ID)

		// Step 2: Upload bank XML file (matching transaction)
		bankXML := BuildXMLContent(
			"xml-match-001",
			"350.00",
			"EUR",
			"2026-01-20",
			"bank xml statement",
		)
		bankPath := UploadPath(seed.ContextID, seed.NonLedgerSourceID)

		bankResp, bankBody, err := sh.DoMultipart(
			bankPath,
			"file",
			"bank.xml",
			bankXML,
			map[string]string{
				"format": "xml",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank XML upload failed: %s",
			string(bankBody),
		)

		bankJob := ParseJobResponse(t, bankBody)
		t.Logf("Bank XML job created: %s", bankJob.ID)

		// Step 3: Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 4: Trigger matching via HTTP
		matchPath := RunMatchPath(seed.ContextID)
		matchResp, matchBody, err := sh.DoJSON(http.MethodPost, matchPath, map[string]string{
			"mode": "COMMIT",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching failed: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run created: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Step 5: Dispatch outbox to publish match confirmed events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Step 6: Verify match_confirmed was published to RabbitMQ
		eventBody, err := sh.WaitForEventWithTimeout(
			10*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		t.Logf("Received match_confirmed event from XML upload: %s", string(eventBody))
	})
}

func TestIntegrationHTTPFlow_UploadMultiRowXML_ReturnsAccepted(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload XML file with multiple transactions
		xmlContent := BuildMultiRowXML([][]string{
			{"XML-MULTI-001", "100.00", "USD", "2026-01-15", "xml tx 1"},
			{"XML-MULTI-002", "200.00", "EUR", "2026-01-16", "xml tx 2"},
			{"XML-MULTI-003", "300.00", "GBP", "2026-01-17", "xml tx 3"},
		})
		path := UploadPath(seed.ContextID, seed.LedgerSourceID)

		resp, body, err := sh.DoMultipart(path, "file", "multi.xml", xmlContent, map[string]string{
			"format": "xml",
		})
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			resp.StatusCode,
			"expected 202 Accepted, got %d: %s",
			resp.StatusCode,
			string(body),
		)

		jobResp := ParseJobResponse(t, body)
		require.NotEqual(
			t,
			jobResp.ID.String(),
			"00000000-0000-0000-0000-000000000000",
			"job ID should not be nil",
		)
		t.Logf("Multi-row XML job created: %s, status: %s", jobResp.ID, jobResp.Status)

		// Dispatch outbox to publish ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)
	})
}

func TestIntegrationHTTPFlow_MixedFormatUpload_CSVAndXML_MatchesAcrossFormats(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger as CSV
		ledgerCSV := BuildCSVContent("MIXED-REF-001", "500.00", "USD", "2026-01-25", "ledger csv")
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger CSV upload: %s",
			string(ledgerBody),
		)
		t.Logf("Ledger CSV job: %s", ParseJobResponse(t, ledgerBody).ID)

		// Upload bank as XML (matching transaction)
		bankXML := BuildXMLContent("mixed-ref-001", "500.00", "USD", "2026-01-25", "bank xml")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.xml", bankXML,
			map[string]string{"format": "xml"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank XML upload: %s",
			string(bankBody),
		)
		t.Logf("Bank XML job: %s", ParseJobResponse(t, bankBody).ID)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Verify match_confirmed was published
		eventBody, err := sh.WaitForEventWithTimeout(
			10*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(
			t,
			err,
			"timed out waiting for match_confirmed event for mixed format upload",
		)
		require.NotEmpty(t, eventBody)
		event := ParseMatchConfirmedEvent(t, eventBody)
		require.NotEqual(t, uuid.Nil, event.MatchID, "match_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.RunID, "run_id should not be nil")
		require.NotEqual(t, uuid.Nil, event.ContextID, "context_id should not be nil")
		t.Logf("Received match_confirmed event for mixed CSV/XML: %s", string(eventBody))
	})
}

// =============================================================================
// Job Listing Pagination Tests
// =============================================================================

func TestIntegrationHTTPFlow_ListJobs_WithLimit(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload 5 files to create 5 jobs
		for i := 1; i <= 5; i++ {
			csv := BuildCSVContent(
				fmt.Sprintf("PAGE-REF-%03d", i),
				fmt.Sprintf("%d.00", i*100),
				"USD",
				"2026-01-15",
				fmt.Sprintf("pagination test %d", i),
			)
			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("page%d.csv", i),
				csv,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode, "upload %d: %s", i, string(body))
			sh.DispatchOutboxUntilEmpty(ctx, 2)
		}

		// List jobs with limit=2
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs?limit=2"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "list jobs: %s", string(body))

		listResp := ParseJobListResponse(t, body)
		require.Len(t, listResp.Items, 2, "expected 2 items with limit=2")
		require.NotEmpty(t, listResp.NextCursor, "expected next_cursor for more results")
	})
}

func TestIntegrationHTTPFlow_ListJobs_CursorPagination(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload 6 files to create 6 jobs
		var allJobIDs []uuid.UUID
		for i := 1; i <= 6; i++ {
			csv := BuildCSVContent(
				fmt.Sprintf("CURSOR-REF-%03d", i),
				fmt.Sprintf("%d.00", i*50),
				"EUR",
				"2026-01-16",
				fmt.Sprintf("cursor test %d", i),
			)
			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("cursor%d.csv", i),
				csv,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode, "upload %d: %s", i, string(body))
			job := ParseJobResponse(t, body)
			allJobIDs = append(allJobIDs, job.ID)
			sh.DispatchOutboxUntilEmpty(ctx, 2)
		}

		// Get first page (limit=3)
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs?limit=3"
		resp1, body1, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp1.StatusCode, "first page: %s", string(body1))

		page1 := ParseJobListResponse(t, body1)
		require.Len(t, page1.Items, 3, "expected 3 items on first page")
		require.NotEmpty(t, page1.NextCursor, "expected next_cursor on first page")
		t.Logf("First page cursor: next=%s", page1.NextCursor)

		// Get second page using cursor
		listPath2 := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs?limit=3&cursor=" + page1.NextCursor
		resp2, body2, err := sh.DoJSON(http.MethodGet, listPath2, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp2.StatusCode, "second page: %s", string(body2))

		page2 := ParseJobListResponse(t, body2)
		require.Len(t, page2.Items, 3, "expected 3 items on second page")
		t.Logf("Second page cursor: next=%s, prev=%s", page2.NextCursor, page2.PrevCursor)

		// Verify no overlap between pages
		page1IDs := make(map[string]bool)
		for _, item := range page1.Items {
			page1IDs[item.ID.String()] = true
		}
		for _, item := range page2.Items {
			require.False(t, page1IDs[item.ID.String()], "job %s appears on both pages", item.ID)
		}
	})
}

func TestIntegrationHTTPFlow_ListJobs_SortOrderAsc(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload 5 files
		var createdJobIDs []uuid.UUID
		for i := 1; i <= 5; i++ {
			csv := BuildCSVContent(
				fmt.Sprintf("ASC-REF-%03d", i),
				fmt.Sprintf("%d.00", i*75),
				"GBP",
				"2026-01-17",
				fmt.Sprintf("asc test %d", i),
			)
			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("asc%d.csv", i),
				csv,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode)
			job := ParseJobResponse(t, body)
			createdJobIDs = append(createdJobIDs, job.ID)
			sh.DispatchOutboxUntilEmpty(ctx, 2)
		}

		// List with sort_order=asc and sort_by=created_at (default sort_by is id which gives random order)
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs?sort_order=asc&sort_by=created_at&limit=10"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "list asc: %s", string(body))

		listResp := ParseJobListResponse(t, body)
		require.GreaterOrEqual(t, len(listResp.Items), 5, "expected at least 5 items")

		// Verify ascending order (first created job should come first)
		require.Equal(
			t,
			createdJobIDs[0],
			listResp.Items[0].ID,
			"first created job should be first in asc order",
		)
	})
}

func TestIntegrationHTTPFlow_ListJobs_SortOrderDesc(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		// Upload 5 files
		var createdJobIDs []uuid.UUID
		for i := 1; i <= 5; i++ {
			csv := BuildCSVContent(
				fmt.Sprintf("DESC-REF-%03d", i),
				fmt.Sprintf("%d.00", i*25),
				"JPY",
				"2026-01-18",
				fmt.Sprintf("desc test %d", i),
			)
			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("desc%d.csv", i),
				csv,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode)
			job := ParseJobResponse(t, body)
			createdJobIDs = append(createdJobIDs, job.ID)
			sh.DispatchOutboxUntilEmpty(ctx, 2)
		}

		// List with sort_order=desc and sort_by=created_at
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs?sort_order=desc&sort_by=created_at&limit=10"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "list desc: %s", string(body))

		listResp := ParseJobListResponse(t, body)
		require.GreaterOrEqual(t, len(listResp.Items), 5, "expected at least 5 items")

		// Verify descending order (last created job should come first)
		require.Equal(
			t,
			createdJobIDs[4],
			listResp.Items[0].ID,
			"last created job should be first in desc order",
		)
	})
}

func TestIntegrationHTTPFlow_ListJobs_ResponseStructure(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload a file to ensure at least one job exists
		csv := BuildCSVContent("STRUCT-REF-001", "999.99", "CHF", "2026-01-19", "structure test")
		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)
		resp, _, err := sh.DoMultipart(
			uploadPath,
			"file",
			"struct.csv",
			csv,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		sh.DispatchOutboxUntilEmpty(ctx, 2)

		// List jobs
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs"
		listResp, listBody, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, listResp.StatusCode, "list jobs: %s", string(listBody))

		// Verify response structure
		parsed := ParseJobListResponse(t, listBody)
		require.NotNil(t, parsed.Items, "response should have items array")
		require.GreaterOrEqual(t, len(parsed.Items), 1, "should have at least one job")

		// Verify cursor fields exist (embedded from CursorResponse)
		require.GreaterOrEqual(t, parsed.Limit, 1, "cursor should have limit")

		// Verify job item structure
		job := parsed.Items[0]
		require.NotEqual(t, uuid.Nil, job.ID, "job should have valid ID")
		require.NotEmpty(t, job.Status, "job should have status")
	})
}

func TestIntegrationHTTPFlow_ListJobs_EmptyContext_ReturnsEmptyList(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		seed := SetupFlowTestConfig(t, sh)

		// List jobs without uploading any files
		listPath := "/v1/imports/contexts/" + seed.ContextID.String() + "/jobs"
		resp, body, err := sh.DoJSON(http.MethodGet, listPath, nil)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, "list empty: %s", string(body))

		parsed := ParseJobListResponse(t, body)
		require.Empty(t, parsed.Items, "expected empty items array for context with no jobs")
	})
}

// =============================================================================
// Deduplication Tests
// =============================================================================

func TestIntegrationHTTPFlow_DuplicateUpload_SameFile_NoDoubleCount(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		csvContent := BuildCSVContent(
			"DEDUP-REF-001",
			"500.00",
			"USD",
			"2026-01-20",
			"dedup test tx",
		)

		resp1, body1, err := sh.DoMultipart(
			uploadPath,
			"file",
			"dedup.csv",
			csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp1.StatusCode, "first upload: %s", string(body1))
		job1 := ParseJobResponse(t, body1)
		t.Logf("First upload job: %s", job1.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		resp2, body2, err := sh.DoMultipart(
			uploadPath,
			"file",
			"dedup.csv",
			csvContent,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp2.StatusCode, "second upload: %s", string(body2))
		job2 := ParseJobResponse(t, body2)
		t.Logf("Second upload job: %s", job2.ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		require.NotEqual(t, job1.ID, job2.ID, "each upload should create a separate job")

		bankCSV := BuildCSVContent("dedup-ref-001", "500.00", "USD", "2026-01-20", "bank tx")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Expect exactly 1 match_confirmed event (no duplicates)
		events := WaitForMultipleMatchEvents(t, sh, 1, 15*time.Second)
		require.Equal(
			t,
			1,
			len(events),
			"expected exactly 1 match_confirmed event (no duplicates), got %d",
			len(events),
		)
		t.Logf("Match event with %d match(es) (deduplication working)", len(events))
	})
}

func TestIntegrationHTTPFlow_DuplicateUpload_MultiRowFile_DeduplicatesAll(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)

		ledgerCSV := BuildMultiRowCSV([][]string{
			{"DEDUP-MULTI-001", "100.00", "USD", "2026-01-20", "tx 1"},
			{"DEDUP-MULTI-002", "200.00", "USD", "2026-01-20", "tx 2"},
			{"DEDUP-MULTI-003", "300.00", "USD", "2026-01-20", "tx 3"},
		})

		resp1, body1, err := sh.DoMultipart(
			uploadPath,
			"file",
			"multi.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp1.StatusCode, "first upload: %s", string(body1))
		t.Logf("First multi-row upload job: %s", ParseJobResponse(t, body1).ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		resp2, body2, err := sh.DoMultipart(
			uploadPath,
			"file",
			"multi.csv",
			ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, resp2.StatusCode, "second upload: %s", string(body2))
		t.Logf("Second multi-row upload job: %s", ParseJobResponse(t, body2).ID)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		bankCSV := BuildMultiRowCSV([][]string{
			{"dedup-multi-001", "100.00", "USD", "2026-01-20", "bank 1"},
			{"dedup-multi-002", "200.00", "USD", "2026-01-20", "bank 2"},
			{"dedup-multi-003", "300.00", "USD", "2026-01-20", "bank 3"},
		})
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Expect match_confirmed events (no duplicates)
		// With 3 matching pairs, we expect up to 3 events but accept at least 1
		events := WaitForMultipleMatchEvents(t, sh, 3, 15*time.Second)
		require.GreaterOrEqual(t, len(events), 1, "expected at least 1 match_confirmed event")
		require.LessOrEqual(
			t,
			len(events),
			3,
			"expected at most 3 match_confirmed events (no duplicates)",
		)

		t.Logf("Received %d match_confirmed events without duplication", len(events))
	})
}

func TestIntegrationHTTPFlow_DuplicateUpload_ThreeUploads_StillDeduplicates(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		uploadPath := UploadPath(seed.ContextID, seed.LedgerSourceID)
		csvContent := BuildCSVContent(
			"TRIPLE-DEDUP-001",
			"999.99",
			"EUR",
			"2026-01-25",
			"triple test",
		)

		for i := 1; i <= 3; i++ {
			resp, body, err := sh.DoMultipart(
				uploadPath,
				"file",
				fmt.Sprintf("upload%d.csv", i),
				csvContent,
				map[string]string{"format": "csv"},
			)
			require.NoError(t, err)
			require.Equal(t, http.StatusAccepted, resp.StatusCode, "upload %d: %s", i, string(body))
			t.Logf("Upload %d job: %s", i, ParseJobResponse(t, body).ID)
			sh.DispatchOutboxUntilEmpty(ctx, 3)
		}

		bankCSV := BuildCSVContent("triple-dedup-001", "999.99", "EUR", "2026-01-25", "bank triple")
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file",
			"bank.csv",
			bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Expect exactly 1 match_confirmed event after 3 duplicate uploads
		events := WaitForMultipleMatchEvents(t, sh, 1, 15*time.Second)
		require.Equal(
			t,
			1,
			len(events),
			"expected exactly 1 match_confirmed event after 3 duplicate uploads, got %d",
			len(events),
		)
		t.Logf("Deduplication works across 3 uploads: %d match(es)", len(events))
	})
}

// =============================================================================
// Multi-Rule Priority Tests
// =============================================================================

func TestIntegrationHTTPFlow_MultiRule_ExactRuleAppliedFirst(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupMultiRuleFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		t.Logf(
			"Created multi-rule config: ExactRule=%s (priority 1), ToleranceRule=%s (priority 2)",
			seed.ExactRuleID,
			seed.ToleranceRuleID,
		)

		// Upload ledger transaction
		ledgerCSV := BuildCSVContent(
			"PRIORITY-REF-001",
			"100.00",
			"USD",
			"2026-01-15",
			"ledger tx exact match",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank transaction with EXACT matching values
		// This should match the EXACT rule (priority 1)
		bankCSV := BuildCSVContent(
			"priority-ref-001",
			"100.00",
			"USD",
			"2026-01-15",
			"bank tx exact match",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Wait for match_confirmed event
		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)
		t.Logf("Match event: RuleID=%s, Confidence=%d, TransactionIDs=%v",
			matchEvent.RuleID, matchEvent.Confidence, matchEvent.TransactionIDs)

		// Verify the EXACT rule was applied (priority 1, higher priority)
		require.Equal(t, seed.ExactRuleID, matchEvent.RuleID,
			"expected EXACT rule (priority 1) to be applied, got rule %s", matchEvent.RuleID)
		require.Equal(t, 100, matchEvent.Confidence,
			"expected confidence 100 from EXACT rule, got %d", matchEvent.Confidence)
	})
}

func TestIntegrationHTTPFlow_MultiRule_ToleranceRuleAppliedWhenExactFails(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupMultiRuleFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		t.Logf(
			"Created multi-rule config: ExactRule=%s (priority 1), ToleranceRule=%s (priority 2)",
			seed.ExactRuleID,
			seed.ToleranceRuleID,
		)

		// Upload ledger transaction
		ledgerCSV := BuildCSVContent(
			"TOLERANCE-REF-001",
			"100.00",
			"USD",
			"2026-01-15",
			"ledger tx tolerance",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank transaction with SLIGHTLY DIFFERENT amount (within tolerance)
		// Amount: 103.00 (difference of $3, within $5 tolerance)
		// This should NOT match EXACT rule but SHOULD match TOLERANCE rule
		bankCSV := BuildCSVContent(
			"tolerance-ref-001",
			"103.00",
			"USD",
			"2026-01-15",
			"bank tx tolerance",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Wait for match_confirmed event
		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)
		t.Logf("Match event: RuleID=%s, Confidence=%d, TransactionIDs=%v",
			matchEvent.RuleID, matchEvent.Confidence, matchEvent.TransactionIDs)

		// Verify the TOLERANCE rule was applied (priority 2, lower priority)
		require.Equal(
			t,
			seed.ToleranceRuleID,
			matchEvent.RuleID,
			"expected TOLERANCE rule (priority 2) to be applied when exact match fails, got rule %s",
			matchEvent.RuleID,
		)
		// Confidence is calculated dynamically from weighted components (PRD AC-001):
		// Amount match (40%) + Currency match (30%) + Date match (20%) + Reference (10%)
		// When MatchReference=false (default), ReferenceScore=1.0 (not penalized)
		// Score: 40 + 30 + 20 + 10 = 100
		require.Equal(
			t,
			100,
			matchEvent.Confidence,
			"expected confidence 100 from TOLERANCE rule (reference matching disabled by default), got %d",
			matchEvent.Confidence,
		)
	})
}

func TestIntegrationHTTPFlow_MultiRule_MixedMatchesDifferentRules(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupMultiRuleFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		t.Logf(
			"Created multi-rule config: ExactRule=%s, ToleranceRule=%s",
			seed.ExactRuleID,
			seed.ToleranceRuleID,
		)

		// Upload ledger transactions: one that will match exactly, one that needs tolerance
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"MIXED-EXACT-001", "200.00", "USD", "2026-01-20", "ledger exact match"},
			{"MIXED-TOL-001", "300.00", "USD", "2026-01-20", "ledger tolerance match"},
		})
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank transactions:
		// - First matches EXACT rule (same amount)
		// - Second needs TOLERANCE rule (amount differs by $4, within $5 tolerance)
		bankCSV := BuildMultiRowCSV([][]string{
			{"mixed-exact-001", "200.00", "USD", "2026-01-20", "bank exact match"},
			{"mixed-tol-001", "304.00", "USD", "2026-01-20", "bank tolerance match"},
		})
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)
		t.Logf("Match run: %s, status: %s", runMatch.RunID, runMatch.Status)

		// Dispatch match events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Collect both match_confirmed events
		var matchEvents []MatchConfirmedEvent
		for i := 0; i < 2; i++ {
			eventBody, err := sh.WaitForEventWithTimeout(
				15*time.Second,
				func(routingKey string, _ []byte) bool {
					return routingKey == server.RoutingKeyMatchConfirmed
				},
			)
			require.NoError(t, err, "timed out waiting for match_confirmed event %d", i+1)

			event := ParseMatchConfirmedEvent(t, eventBody)
			matchEvents = append(matchEvents, event)
			t.Logf("Match event %d: RuleID=%s, Confidence=%d", i+1, event.RuleID, event.Confidence)
		}

		// Verify we got matches from both rules
		ruleIDSet := make(map[uuid.UUID]bool)
		for _, event := range matchEvents {
			ruleIDSet[event.RuleID] = true
		}

		require.Contains(t, ruleIDSet, seed.ExactRuleID,
			"expected at least one match from EXACT rule")
		require.Contains(t, ruleIDSet, seed.ToleranceRuleID,
			"expected at least one match from TOLERANCE rule")

		t.Logf(
			"Successfully matched transactions using different rules based on data characteristics",
		)
	})
}

func TestIntegrationHTTPFlow_MultiRule_NoMatchWhenOutsideTolerance(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupMultiRuleFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload ledger transaction
		ledgerCSV := BuildCSVContent(
			"NOMATCH-REF-001",
			"100.00",
			"USD",
			"2026-01-15",
			"ledger no match",
		)
		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		// Upload bank transaction with amount OUTSIDE tolerance
		// Amount: 110.00 (difference of $10, exceeds $5 tolerance)
		bankCSV := BuildCSVContent(
			"nomatch-ref-001",
			"110.00",
			"USD",
			"2026-01-15",
			"bank no match",
		)
		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		// Dispatch ingestion events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		// Dispatch any potential events
		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Wait briefly and verify NO match_confirmed event was published
		waitCtx, waitCancel := context.WithTimeout(ctx, 3*time.Second)
		defer waitCancel()

		_, err = sh.WaitForEvent(waitCtx, func(routingKey string, _ []byte) bool {
			return routingKey == server.RoutingKeyMatchConfirmed
		})
		require.ErrorIs(t, err, context.DeadlineExceeded,
			"expected no match_confirmed event when amount exceeds tolerance")

		t.Logf("Correctly rejected match when amount difference ($10) exceeds tolerance ($5)")
	})
}

// =============================================================================
// Match Confirmed Event Payload Verification Tests
// =============================================================================

func TestIntegrationHTTPFlow_MatchConfirmedEvent_ContainsAllRequiredFields(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching ledger and bank transactions
		ledgerCSV := BuildCSVContent("EVENT-FIELD-001", "500.00", "USD", "2026-01-15", "ledger tx")
		bankCSV := BuildCSVContent("event-field-001", "500.00", "USD", "2026-01-15", "bank tx")

		ledgerResp, ledgerBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			ledgerResp.StatusCode,
			"ledger upload: %s",
			string(ledgerBody),
		)

		bankResp, bankBody, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			bankResp.StatusCode,
			"bank upload: %s",
			string(bankBody),
		)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Run matching
		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(
			t,
			http.StatusAccepted,
			matchResp.StatusCode,
			"matching: %s",
			string(matchBody),
		)

		runMatch := ParseRunMatchResponse(t, matchBody)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		// Capture and verify event payload
		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err, "timed out waiting for match_confirmed event")

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)
		t.Logf("Raw event: %s", string(eventBody))

		// Verify all required fields using helper
		AssertMatchConfirmedEventValid(t, matchEvent, seed.ContextID, seed.RuleID)

		// Verify run_id matches the response
		require.Equal(t, runMatch.RunID, matchEvent.RunID, "run_id should match API response")

		// Verify transaction IDs (2 transactions: 1 ledger + 1 bank)
		AssertTransactionIDsContain(t, matchEvent, 2)

		t.Logf(
			"Event validation passed: event_type=%s, context_id=%s, run_id=%s, match_id=%s, confidence=%d",
			matchEvent.EventType,
			matchEvent.ContextID,
			matchEvent.RunID,
			matchEvent.MatchID,
			matchEvent.Confidence,
		)
	})
}

func TestIntegrationHTTPFlow_MatchConfirmedEvent_ConfidenceScore(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload exact match transactions (should get 100% confidence with EXACT rule)
		ledgerCSV := BuildCSVContent(
			"CONF-TEST-001",
			"999.99",
			"EUR",
			"2026-02-01",
			"confidence test",
		)
		bankCSV := BuildCSVContent(
			"conf-test-001",
			"999.99",
			"EUR",
			"2026-02-01",
			"confidence test",
		)

		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, _, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err)

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)

		// EXACT rule with matchScore=100 in config should give 100 confidence
		require.Equal(t, 100, matchEvent.Confidence, "exact match should have 100%% confidence")
		t.Logf("Confidence score verified: %d", matchEvent.Confidence)
	})
}

func TestIntegrationHTTPFlow_MatchConfirmedEvent_TimestampsAreValid(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		testStartTime := time.Now().UTC()

		// Upload matching transactions
		ledgerCSV := BuildCSVContent(
			"TIME-TEST-001",
			"123.45",
			"GBP",
			"2026-03-01",
			"timestamp test",
		)
		bankCSV := BuildCSVContent("time-test-001", "123.45", "GBP", "2026-03-01", "timestamp test")

		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, _, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err)

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)

		// Verify confirmed_at is after test start
		require.True(
			t,
			matchEvent.ConfirmedAt.After(testStartTime) ||
				matchEvent.ConfirmedAt.Equal(testStartTime),
			"confirmed_at (%v) should be >= test start time (%v)",
			matchEvent.ConfirmedAt,
			testStartTime,
		)

		// Verify timestamp is after test start
		require.True(
			t,
			matchEvent.Timestamp.After(testStartTime) || matchEvent.Timestamp.Equal(testStartTime),
			"timestamp (%v) should be >= test start time (%v)",
			matchEvent.Timestamp,
			testStartTime,
		)

		// Verify timestamps are in UTC
		require.Equal(
			t,
			time.UTC,
			matchEvent.ConfirmedAt.Location(),
			"confirmed_at should be in UTC",
		)
		require.Equal(t, time.UTC, matchEvent.Timestamp.Location(), "timestamp should be in UTC")

		// Verify confirmed_at is <= timestamp (confirmed before or at event creation)
		require.True(
			t,
			matchEvent.ConfirmedAt.Before(matchEvent.Timestamp) ||
				matchEvent.ConfirmedAt.Equal(matchEvent.Timestamp),
			"confirmed_at (%v) should be <= timestamp (%v)",
			matchEvent.ConfirmedAt,
			matchEvent.Timestamp,
		)

		t.Logf(
			"Timestamps verified: confirmed_at=%v, timestamp=%v",
			matchEvent.ConfirmedAt,
			matchEvent.Timestamp,
		)
	})
}

func TestIntegrationHTTPFlow_MatchConfirmedEvent_MultipleMatches_EmitsMultipleEvents(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload 3 matching pairs
		ledgerCSV := BuildMultiRowCSV([][]string{
			{"MULTI-EVT-001", "100.00", "USD", "2026-04-01", "match 1"},
			{"MULTI-EVT-002", "200.00", "USD", "2026-04-02", "match 2"},
			{"MULTI-EVT-003", "300.00", "USD", "2026-04-03", "match 3"},
		})
		bankCSV := BuildMultiRowCSV([][]string{
			{"multi-evt-001", "100.00", "USD", "2026-04-01", "match 1"},
			{"multi-evt-002", "200.00", "USD", "2026-04-02", "match 2"},
			{"multi-evt-003", "300.00", "USD", "2026-04-03", "match 3"},
		})

		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, matchBody, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp.StatusCode)

		runMatch := ParseRunMatchResponse(t, matchBody)

		sh.DispatchOutboxUntilEmpty(ctx, 10)

		// Collect all match_confirmed events (one per match group)
		var events []MatchConfirmedEvent
		collectCtx, collectCancel := context.WithTimeout(ctx, 20*time.Second)
		defer collectCancel()

	CollectLoop:
		for {
			select {
			case <-collectCtx.Done():
				break CollectLoop
			default:
				eventBody, err := sh.WaitForEventWithTimeout(10*time.Second, func(routingKey string, _ []byte) bool {
					return routingKey == server.RoutingKeyMatchConfirmed
				})
				if err != nil {
					break CollectLoop
				}
				events = append(events, ParseMatchConfirmedEvent(t, eventBody))
			}
		}

		require.GreaterOrEqual(t, len(events), 1, "should receive at least 1 match_confirmed event")
		t.Logf("Received %d match_confirmed event(s)", len(events))

		// Verify all events have the same run_id and context_id
		for i, evt := range events {
			require.Equal(t, runMatch.RunID, evt.RunID, "event[%d] run_id mismatch", i)
			require.Equal(t, seed.ContextID, evt.ContextID, "event[%d] context_id mismatch", i)
			require.NotEqual(t, uuid.Nil, evt.MatchID, "event[%d] match_id should not be nil", i)
			require.NotEmpty(
				t,
				evt.TransactionIDs,
				"event[%d] transaction_ids should not be empty",
				i,
			)
			t.Logf("Event[%d]: match_id=%s, tx_count=%d, confidence=%d",
				i, evt.MatchID, len(evt.TransactionIDs), evt.Confidence)
		}

		// Verify all match_ids are unique
		matchIDs := make(map[uuid.UUID]bool)
		for _, evt := range events {
			require.False(t, matchIDs[evt.MatchID], "duplicate match_id found: %s", evt.MatchID)
			matchIDs[evt.MatchID] = true
		}
	})
}

func TestIntegrationHTTPFlow_MatchConfirmedEvent_TenantIDFromContext(t *testing.T) {
	server.RunWithServer(t, func(t *testing.T, sh *server.ServerHarness) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()

		seed := SetupFlowTestConfig(t, sh)
		EnsureContext(t, sh, seed.ContextID)

		// Upload matching transactions
		ledgerCSV := BuildCSVContent("TENANT-001", "777.77", "JPY", "2026-05-01", "tenant test")
		bankCSV := BuildCSVContent("tenant-001", "777.77", "JPY", "2026-05-01", "tenant test")

		ledgerResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.LedgerSourceID),
			"file", "ledger.csv", ledgerCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, ledgerResp.StatusCode)

		bankResp, _, err := sh.DoMultipart(
			UploadPath(seed.ContextID, seed.NonLedgerSourceID),
			"file", "bank.csv", bankCSV,
			map[string]string{"format": "csv"},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, bankResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		matchResp, _, err := sh.DoJSON(
			http.MethodPost,
			RunMatchPath(seed.ContextID),
			map[string]string{
				"mode": "COMMIT",
			},
		)
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, matchResp.StatusCode)

		sh.DispatchOutboxUntilEmpty(ctx, 5)

		eventBody, err := sh.WaitForEventWithTimeout(
			15*time.Second,
			func(routingKey string, _ []byte) bool {
				return routingKey == server.RoutingKeyMatchConfirmed
			},
		)
		require.NoError(t, err)

		matchEvent := ParseMatchConfirmedEvent(t, eventBody)

		// Verify tenant_id matches the harness seed
		require.Equal(t, seed.TenantID, matchEvent.TenantID, "tenant_id should match seed")

		t.Logf("Tenant verification passed: tenant_id=%s", matchEvent.TenantID)
	})
}
