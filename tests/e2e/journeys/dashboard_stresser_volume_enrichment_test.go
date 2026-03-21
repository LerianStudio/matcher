//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/client"
)

func runDashboardStresserHighVolumeEnrichment(
	t *testing.T,
	tc *e2e.TestContext,
	apiClient *e2e.Client,
	reconciliationContextID string,
	reconciliationContextName string,
	ledgerSourceID string,
	ledgerJobID string,
	bankJobID string,
	ledgerCSV []byte,
	dateFrom string,
	dateTo string,
	dashboard *client.DashboardAggregates,
	matchGroups []client.MatchGroup,
) {
	ctx := context.Background()

	// ============================================================
	// ENRICHED API COVERAGE PHASES (Steps 10-22)
	// All non-critical: assert.NoError + log warnings
	// ============================================================

	// Tracking variables for enriched summary
	var (
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
	)

	// endpointFailures collects all endpoint errors for categorized evaluation at the end.
	endpointFailures := make(map[string]string)

	// ============================================================
	// STEP 10: Configuration Verification
	// ============================================================
	tc.Logf("\n[STEP 10/22] Configuration verification...")

	if contexts, err := apiClient.Configuration.ListContexts(ctx); err != nil {
		endpointFailures["ListContexts"] = err.Error()
	} else {
		tc.Logf("  ListContexts: %d contexts", len(contexts))
		enrichedEndpoints++
	}

	if ctxDetail, err := apiClient.Configuration.GetContext(ctx, reconciliationContextID); err != nil {
		endpointFailures["GetContext"] = err.Error()
	} else {
		tc.Logf("  GetContext: %s (%s)", ctxDetail.Name, ctxDetail.ID)
		enrichedEndpoints++
	}

	if _, err := apiClient.Configuration.UpdateContext(ctx, reconciliationContextID, client.UpdateContextRequest{
		Description: strPtr("Enriched stresser - updated description"),
	}); err != nil {
		endpointFailures["UpdateContext"] = err.Error()
	} else {
		tc.Logf("  UpdateContext: description updated")
		enrichedEndpoints++
	}

	if sources, err := apiClient.Configuration.ListSources(ctx, reconciliationContextID); err != nil {
		endpointFailures["ListSources"] = err.Error()
	} else {
		tc.Logf("  ListSources: %d sources", len(sources))
		enrichedEndpoints++
	}

	if src, err := apiClient.Configuration.GetSource(ctx, reconciliationContextID, ledgerSourceID); err != nil {
		endpointFailures["GetSource"] = err.Error()
	} else {
		tc.Logf("  GetSource: %s (%s)", src.Name, src.Type)
		enrichedEndpoints++
	}

	if rules, err := apiClient.Configuration.ListMatchRules(ctx, reconciliationContextID); err != nil {
		endpointFailures["ListMatchRules"] = err.Error()
	} else {
		tc.Logf("  ListMatchRules: %d rules", len(rules))
		enrichedEndpoints++

		// Reorder rules: reverse order, then restore original order.
		if len(rules) >= 2 {
			ruleIDs := make([]string, len(rules))
			for i, r := range rules {
				ruleIDs[i] = r.ID
			}

			// Phase 1: reverse
			reversed := make([]string, len(ruleIDs))
			for i, id := range ruleIDs {
				reversed[len(ruleIDs)-1-i] = id
			}

			if err := apiClient.Configuration.ReorderMatchRules(ctx, reconciliationContextID, client.ReorderMatchRulesRequest{
				RuleIDs: reversed,
			}); err != nil {
				endpointFailures["ReorderMatchRules_reverse"] = err.Error()
			} else {
				tc.Logf("  ReorderMatchRules: reversed %d rules", len(reversed))
				enrichedEndpoints++

				// Phase 2: restore original order
				if err := apiClient.Configuration.ReorderMatchRules(ctx, reconciliationContextID, client.ReorderMatchRulesRequest{
					RuleIDs: ruleIDs,
				}); err != nil {
					endpointFailures["ReorderMatchRules_restore"] = err.Error()
				} else {
					tc.Logf("  ReorderMatchRules: restored original order")
				}
			}
		}
	}

	if fm, err := apiClient.Configuration.GetFieldMapBySource(ctx, reconciliationContextID, ledgerSourceID); err != nil {
		endpointFailures["GetFieldMapBySource"] = err.Error()
	} else {
		tc.Logf("  GetFieldMapBySource: %s", fm.ID)
		enrichedEndpoints++
	}

	// ============================================================
	// STEP 11: Schedule CRUD
	// ============================================================
	tc.Logf("\n[STEP 11/22] Schedule CRUD...")

	schedule, err := apiClient.Configuration.CreateSchedule(ctx, reconciliationContextID, client.CreateScheduleRequest{
		CronExpression: "0 0 * * *",
		Enabled:        true,
	})
	if err != nil {
		endpointFailures["CreateSchedule"] = err.Error()
	} else {
		scheduleCreated = true
		tc.Logf("  CreateSchedule: %s", schedule.ID)
		enrichedEndpoints++

		if got, err := apiClient.Configuration.GetSchedule(ctx, reconciliationContextID, schedule.ID); err != nil {
			endpointFailures["GetSchedule"] = err.Error()
		} else {
			tc.Logf("  GetSchedule: cron=%s enabled=%v", got.CronExpression, got.Enabled)
			enrichedEndpoints++
		}

		if list, err := apiClient.Configuration.ListSchedules(ctx, reconciliationContextID); err != nil {
			endpointFailures["ListSchedules"] = err.Error()
		} else {
			tc.Logf("  ListSchedules: %d schedules", len(list))
			enrichedEndpoints++
		}

		disabled := false
		if _, err := apiClient.Configuration.UpdateSchedule(ctx, reconciliationContextID, schedule.ID, client.UpdateScheduleRequest{
			Enabled: &disabled,
		}); err != nil {
			endpointFailures["UpdateSchedule"] = err.Error()
		} else {
			tc.Logf("  UpdateSchedule: disabled schedule")
			enrichedEndpoints++
		}

		if err := apiClient.Configuration.DeleteSchedule(ctx, reconciliationContextID, schedule.ID); err != nil {
			endpointFailures["DeleteSchedule"] = err.Error()
		} else {
			tc.Logf("  DeleteSchedule: removed schedule")
			enrichedEndpoints++
		}
	}

	// ============================================================
	// STEP 12: Ingestion Enrichment
	// ============================================================
	tc.Logf("\n[STEP 12/22] Ingestion enrichment...")

	if jobs, err := apiClient.Ingestion.ListJobsByContext(ctx, reconciliationContextID); err != nil {
		endpointFailures["ListJobsByContext"] = err.Error()
	} else {
		tc.Logf("  ListJobsByContext: %d jobs", len(jobs))
		enrichedEndpoints++
	}

	// Collect some transactions from the first job for later use
	var allTransactions []client.Transaction
	if txns, err := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContextID, ledgerJobID); err != nil {
		endpointFailures["ListTransactionsByJob"] = err.Error()
	} else {
		allTransactions = txns
		tc.Logf("  ListTransactionsByJob: %d transactions from ledger job", len(txns))
		enrichedEndpoints++
	}

	// Preview a file
	if preview, err := apiClient.Ingestion.PreviewFile(
		ctx,
		reconciliationContextID,
		ledgerSourceID,
		"preview-sample.csv",
		ledgerCSV,
		"csv",
		5,
	); err != nil {
		endpointFailures["PreviewFile"] = err.Error()
	} else {
		tc.Logf("  PreviewFile: %d columns, %d rows, format=%s", len(preview.Columns), preview.RowCount, preview.Format)
		enrichedEndpoints++
	}

	// Search transactions
	if searchResp, err := apiClient.Ingestion.SearchTransactions(ctx, reconciliationContextID, client.SearchTransactionsParams{
		Currency: "USD",
		Limit:    10,
	}); err != nil {
		endpointFailures["SearchTransactions"] = err.Error()
	} else {
		searchResultsFound = searchResp.Total
		tc.Logf("  SearchTransactions: %d results (total: %d)", len(searchResp.Items), searchResp.Total)
		enrichedEndpoints++
	}

	// Ignore a transaction (pick last unmatched one if available)
	if len(allTransactions) > 0 {
		// Find an unmatched transaction
		var txToIgnore *client.Transaction
		for i := len(allTransactions) - 1; i >= 0; i-- {
			if allTransactions[i].Status == "UNMATCHED" {
				txToIgnore = &allTransactions[i]
				break
			}
		}
		if txToIgnore != nil {
			if _, err := apiClient.Ingestion.IgnoreTransaction(ctx, reconciliationContextID, txToIgnore.ID, client.IgnoreTransactionRequest{
				Reason: "stresser test - ignore for coverage",
			}); err != nil {
				endpointFailures["IgnoreTransaction"] = err.Error()
			} else {
				tc.Logf("  IgnoreTransaction: %s", txToIgnore.ID)
				enrichedEndpoints++
			}
		}
	}

	// ============================================================
	// STEP 13: Matching Operations
	// ============================================================
	tc.Logf("\n[STEP 13/22] Matching operations...")

	// List match runs
	if runs, err := apiClient.Matching.ListMatchRuns(ctx, reconciliationContextID); err != nil {
		endpointFailures["ListMatchRuns"] = err.Error()
	} else {
		tc.Logf("  ListMatchRuns: %d runs", len(runs))
		enrichedEndpoints++
	}

	// Manual match: pick 1 unmatched transaction from ledger + 1 from bank
	// ManualMatch requires transactions from at least 2 different sources.
	{
		var ledgerTxID, bankTxID string
		for _, tx := range allTransactions {
			if tx.Status == "UNMATCHED" && ledgerTxID == "" {
				ledgerTxID = tx.ID
				break
			}
		}

		bankTxns, bankTxErr := apiClient.Ingestion.ListTransactionsByJob(ctx, reconciliationContextID, bankJobID)
		if bankTxErr != nil {
			endpointFailures["ListTransactionsByJob_bank"] = bankTxErr.Error()
		} else {
			for _, tx := range bankTxns {
				if tx.Status == "UNMATCHED" && bankTxID == "" {
					bankTxID = tx.ID
					break
				}
			}
		}

		if ledgerTxID != "" && bankTxID != "" {
			if resp, err := apiClient.Matching.ManualMatch(ctx, reconciliationContextID, client.ManualMatchRequest{
				TransactionIDs: []string{ledgerTxID, bankTxID},
				Notes:          "stresser manual match test",
			}); err != nil {
				endpointFailures["ManualMatch"] = err.Error()
			} else {
				tc.Logf("  ManualMatch: group %s with %d items", resp.MatchGroup.ID, len(resp.MatchGroup.Items))
				enrichedEndpoints++
			}
		}
	}

	// Create an adjustment
	if len(allTransactions) > 0 {
		if _, err := apiClient.Matching.CreateAdjustment(ctx, reconciliationContextID, client.CreateAdjustmentRequest{
			Type:          "ROUNDING",
			Amount:        "1.50",
			Currency:      "USD",
			Direction:     "CREDIT",
			Reason:        "stresser_test",
			Description:   "Stresser adjustment for API coverage",
			TransactionID: allTransactions[0].ID,
		}); err != nil {
			endpointFailures["CreateAdjustment"] = err.Error()
		} else {
			tc.Logf("  CreateAdjustment: created for transaction %s", allTransactions[0].ID)
			enrichedEndpoints++
		}
	}

	// Unmatch a group (pick the last one to minimize impact)
	if len(matchGroups) > 0 {
		lastGroup := matchGroups[len(matchGroups)-1]
		if err := apiClient.Matching.UnmatchGroup(ctx, reconciliationContextID, lastGroup.ID, client.UnmatchRequest{
			Reason: "stresser unmatch test",
		}); err != nil {
			endpointFailures["UnmatchGroup"] = err.Error()
		} else {
			tc.Logf("  UnmatchGroup: unmatched group %s", lastGroup.ID)
			enrichedEndpoints++
		}
	}

	// ============================================================
	// STEP 14: Exception Lifecycle
	// ============================================================
	tc.Logf("\n[STEP 14/22] Exception lifecycle...")

	var exceptionIDs []string

	// List all exceptions
	if allExc, err := apiClient.Exception.ListExceptions(ctx, client.ExceptionListFilter{Limit: 100}); err != nil {
		endpointFailures["ListExceptions"] = err.Error()
	} else {
		tc.Logf("  ListExceptions: %d exceptions", len(allExc.Items))
		enrichedEndpoints++
	}

	// List open exceptions
	openExceptions, err := apiClient.Exception.ListOpenExceptions(ctx)
	if err != nil {
		endpointFailures["ListOpenExceptions"] = err.Error()
	} else {
		tc.Logf("  ListOpenExceptions: %d open exceptions", len(openExceptions.Items))
		enrichedEndpoints++
		for _, exc := range openExceptions.Items {
			exceptionIDs = append(exceptionIDs, exc.ID)
		}
		exceptionsFound = len(openExceptions.Items)
	}

	// Get details of first exception
	if len(exceptionIDs) > 0 {
		if exc, err := apiClient.Exception.GetException(ctx, exceptionIDs[0]); err != nil {
			endpointFailures["GetException"] = err.Error()
		} else {
			tc.Logf("  GetException: %s severity=%s status=%s", exc.ID, exc.Severity, exc.Status)
			enrichedEndpoints++
		}
	}

	// Force match the first exception
	if len(exceptionIDs) > 0 {
		if _, err := apiClient.Exception.ForceMatch(ctx, exceptionIDs[0], client.ForceMatchRequest{
			OverrideReason: "stresser test",
			Notes:          "Force matching for API coverage",
		}); err != nil {
			endpointFailures["ForceMatch"] = err.Error()
		} else {
			forceMatched++
			tc.Logf("  ForceMatch: %s", exceptionIDs[0])
			enrichedEndpoints++
		}
	}

	// Adjust entry on second exception
	if len(exceptionIDs) > 1 {
		if _, err := apiClient.Exception.AdjustEntry(ctx, exceptionIDs[1], client.AdjustEntryRequest{
			ReasonCode:  "AMOUNT_CORRECTION",
			Notes:       "Stresser adjustment entry",
			Amount:      decimal.RequireFromString("0.50"),
			Currency:    "USD",
			EffectiveAt: time.Now().UTC(),
		}); err != nil {
			endpointFailures["AdjustEntry"] = err.Error()
		} else {
			adjustedEntries++
			tc.Logf("  AdjustEntry: %s", exceptionIDs[1])
			enrichedEndpoints++
		}
	}

	// Dispatch third exception to external system
	if len(exceptionIDs) > 2 {
		if _, err := apiClient.Exception.DispatchToExternal(ctx, exceptionIDs[2], client.DispatchRequest{
			TargetSystem: "jira",
			Queue:        "RECON-TEAM",
		}); err != nil {
			endpointFailures["DispatchToExternal"] = err.Error()
		} else {
			dispatched++
			tc.Logf("  DispatchToExternal: %s", exceptionIDs[2])
			enrichedEndpoints++
		}
	}

	// Get history for first exception
	if len(exceptionIDs) > 0 {
		if history, err := apiClient.Exception.GetExceptionHistory(ctx, exceptionIDs[0], "", 50); err != nil {
			endpointFailures["GetExceptionHistory"] = err.Error()
		} else {
			tc.Logf("  GetExceptionHistory: %d events", len(history.Items))
			enrichedEndpoints++
		}
	}

	// ============================================================
	// STEP 15: Exception Comments
	// ============================================================
	tc.Logf("\n[STEP 15/22] Exception comments...")

	if len(exceptionIDs) > 3 {
		excID := exceptionIDs[3]

		// Add a comment
		comment, err := apiClient.Exception.AddComment(ctx, excID, client.AddCommentRequest{
			Content: "Stresser test comment - investigating this exception",
		})
		if err != nil {
			endpointFailures["AddComment"] = err.Error()
		} else {
			commentsAdded++
			tc.Logf("  AddComment: %s on exception %s", comment.ID, excID)
			enrichedEndpoints++

			// List comments
			if comments, err := apiClient.Exception.ListComments(ctx, excID); err != nil {
				endpointFailures["ListComments"] = err.Error()
			} else {
				tc.Logf("  ListComments: %d comments", len(comments.Items))
				enrichedEndpoints++
			}

			// Delete the comment
			if err := apiClient.Exception.DeleteComment(ctx, excID, comment.ID); err != nil {
				endpointFailures["DeleteComment"] = err.Error()
			} else {
				commentsDeleted++
				tc.Logf("  DeleteComment: %s", comment.ID)
				enrichedEndpoints++
			}
		}
	} else {
		tc.Logf("  SKIP: Not enough exceptions for comments (need >3, have %d)", len(exceptionIDs))
	}

	// ============================================================
	// STEP 16: Bulk Exception Operations
	// ============================================================
	tc.Logf("\n[STEP 16/22] Bulk exception operations...")

	// Use remaining exception IDs for bulk operations (skip first 4 used above).
	// The >= 4 guard ensures we never reuse IDs consumed by Steps 14-15.
	bulkIDs := exceptionIDs
	if len(bulkIDs) >= 4 {
		bulkIDs = bulkIDs[4:]
	} else {
		bulkIDs = nil
	}

	if len(bulkIDs) >= 3 {
		bulkN := len(bulkIDs)
		third := bulkN / 3

		// Bulk assign first third
		assignBatch := bulkIDs[:min(third, 10)]
		if len(assignBatch) > 0 {
			if resp, err := apiClient.Exception.BulkAssign(ctx, client.BulkAssignRequest{
				ExceptionIDs: assignBatch,
				Assignee:     "stresser-test@example.com",
			}); err != nil {
				endpointFailures["BulkAssign"] = err.Error()
			} else {
				bulkAssigned = len(resp.Succeeded)
				tc.Logf("  BulkAssign: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
				enrichedEndpoints++
			}
		}

		// Bulk resolve second third
		resolveBatch := bulkIDs[third:min(2*third, bulkN)]
		if len(resolveBatch) > 10 {
			resolveBatch = resolveBatch[:10]
		}
		if len(resolveBatch) > 0 {
			if resp, err := apiClient.Exception.BulkResolve(ctx, client.BulkResolveRequest{
				ExceptionIDs: resolveBatch,
				Resolution:   "ACCEPTED",
				Reason:       "Stresser bulk resolve test",
			}); err != nil {
				endpointFailures["BulkResolve"] = err.Error()
			} else {
				bulkResolved = len(resp.Succeeded)
				tc.Logf("  BulkResolve: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
				enrichedEndpoints++
			}
		}

		// Bulk dispatch final third
		dispatchBatch := bulkIDs[2*third:]
		if len(dispatchBatch) > 10 {
			dispatchBatch = dispatchBatch[:10]
		}
		if len(dispatchBatch) > 0 {
			if resp, err := apiClient.Exception.BulkDispatch(ctx, client.BulkDispatchRequest{
				ExceptionIDs: dispatchBatch,
				TargetSystem: "jira",
				Queue:        "BULK-TEST",
			}); err != nil {
				endpointFailures["BulkDispatch"] = err.Error()
			} else {
				bulkDispatched = len(resp.Succeeded)
				tc.Logf("  BulkDispatch: %d succeeded, %d failed", len(resp.Succeeded), len(resp.Failed))
				enrichedEndpoints++
			}
		}
	} else {
		tc.Logf("  SKIP: Not enough exceptions for bulk ops (need >=3, have %d)", len(bulkIDs))
	}

	// ============================================================
	// STEP 17: Dispute Lifecycle
	// ============================================================
	tc.Logf("\n[STEP 17/22] Dispute lifecycle...")

	// Use exception IDs not touched by prior steps
	// Steps 14 used [0:4], Step 15 used [3], Step 16 used [4:~37]
	// Pick from index 50+ to ensure fresh OPEN exceptions
	var disputeExcIDs []string
	if len(exceptionIDs) > 52 {
		disputeExcIDs = exceptionIDs[50:52]
	} else if len(exceptionIDs) > 42 {
		disputeExcIDs = exceptionIDs[40:42]
	}

	var disputeIDs []string
	for i, excID := range disputeExcIDs {
		dispute, err := apiClient.Exception.OpenDispute(ctx, excID, client.OpenDisputeRequest{
			Category:    "AMOUNT_MISMATCH",
			Description: fmt.Sprintf("Stresser dispute #%d", i),
		})
		if err != nil {
			endpointFailures["OpenDispute"] = err.Error()
			continue
		}
		disputesOpened++
		disputeIDs = append(disputeIDs, dispute.ID)
		tc.Logf("  OpenDispute: %s on exception %s", dispute.ID, excID)
		enrichedEndpoints++

		// Submit evidence
		if _, err := apiClient.Exception.SubmitEvidence(ctx, dispute.ID, client.SubmitEvidenceRequest{
			Comment: fmt.Sprintf("Evidence for dispute #%d - bank statement attached", i),
		}); err != nil {
			endpointFailures["SubmitEvidence"] = err.Error()
		} else {
			tc.Logf("  SubmitEvidence: added to dispute %s", dispute.ID)
			enrichedEndpoints++
		}
	}

	// List disputes
	if disputeList, err := apiClient.Exception.ListDisputes(ctx); err != nil {
		endpointFailures["ListDisputes"] = err.Error()
	} else {
		tc.Logf("  ListDisputes: %d disputes", len(disputeList.Items))
		enrichedEndpoints++
	}

	// Get first dispute detail
	if len(disputeIDs) > 0 {
		if d, err := apiClient.Exception.GetDispute(ctx, disputeIDs[0]); err != nil {
			endpointFailures["GetDispute"] = err.Error()
		} else {
			tc.Logf("  GetDispute: %s state=%s", d.ID, d.State)
			enrichedEndpoints++
		}
	}

	// Close first dispute
	if len(disputeIDs) > 0 {
		won := true
		if _, err := apiClient.Exception.CloseDispute(ctx, disputeIDs[0], client.CloseDisputeRequest{
			Resolution: "Amount confirmed correct after review",
			Won:        &won,
		}); err != nil {
			endpointFailures["CloseDispute"] = err.Error()
		} else {
			disputesClosed++
			tc.Logf("  CloseDispute: %s (won)", disputeIDs[0])
			enrichedEndpoints++
		}
	}

	for i := 1; i < len(disputeIDs); i++ {
		lost := false
		if _, err := apiClient.Exception.CloseDispute(ctx, disputeIDs[i], client.CloseDisputeRequest{
			Resolution: "Auto-closed for test cleanup",
			Won:        &lost,
		}); err != nil {
			endpointFailures[fmt.Sprintf("CloseDispute_cleanup_%d_%s", i, disputeIDs[i])] = err.Error()
		} else {
			disputesClosed++
			tc.Logf("  CloseDispute: %s (lost, cleanup)", disputeIDs[i])
		}
	}

	// ============================================================
	// STEP 18: Reporting Deep Dive
	// ============================================================
	tc.Logf("\n[STEP 18/22] Reporting deep dive...")

	if vol, err := apiClient.Reporting.GetVolumeStats(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetVolumeStats"] = err.Error()
	} else {
		tc.Logf("  GetVolumeStats: period=%s, totalVolume=%s", vol.Period, vol.TotalVolume)
		enrichedEndpoints++
	}

	if mr, err := apiClient.Reporting.GetMatchRateStats(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetMatchRateStats"] = err.Error()
	} else {
		tc.Logf("  GetMatchRateStats: rate=%.2f%%", mr.MatchRate)
		enrichedEndpoints++
	}

	if sla, err := apiClient.Reporting.GetSLAStats(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetSLAStats"] = err.Error()
	} else {
		tc.Logf("  GetSLAStats: compliance=%.2f%%", sla.SLAComplianceRate)
		enrichedEndpoints++
	}

	if metrics, err := apiClient.Reporting.GetDashboardMetrics(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetDashboardMetrics"] = err.Error()
	} else {
		tc.Logf("  GetDashboardMetrics: updatedAt=%s", metrics.UpdatedAt)
		enrichedEndpoints++
	}

	if sb, err := apiClient.Reporting.GetSourceBreakdown(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetSourceBreakdown"] = err.Error()
	} else {
		tc.Logf("  GetSourceBreakdown: %d sources", len(sb.Items))
		enrichedEndpoints++
	}

	if ci, err := apiClient.Reporting.GetCashImpact(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["GetCashImpact"] = err.Error()
	} else {
		tc.Logf("  GetCashImpact: total=%s, %d currencies", ci.TotalUnreconciledAmount, len(ci.CurrencyExposures))
		enrichedEndpoints++
	}

	if txCount, err := apiClient.Reporting.CountTransactions(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["CountTransactions"] = err.Error()
	} else {
		tc.Logf("  CountTransactions: %d", txCount.Count)
		enrichedEndpoints++
	}

	if mCount, err := apiClient.Reporting.CountMatches(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["CountMatches"] = err.Error()
	} else {
		tc.Logf("  CountMatches: %d", mCount.Count)
		enrichedEndpoints++
	}

	if eCount, err := apiClient.Reporting.CountExceptions(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["CountExceptions"] = err.Error()
	} else {
		tc.Logf("  CountExceptions: %d", eCount.Count)
		enrichedEndpoints++
	}

	// ============================================================
	// STEP 19: Export Pipeline
	// ============================================================
	tc.Logf("\n[STEP 19/22] Export pipeline...")

	// Sync exports
	if data, err := apiClient.Reporting.ExportMatchedReport(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["ExportMatchedReport"] = err.Error()
	} else {
		syncReports++
		tc.Logf("  ExportMatchedReport: %d bytes", len(data))
		enrichedEndpoints++
	}

	if data, err := apiClient.Reporting.ExportUnmatchedReport(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["ExportUnmatchedReport"] = err.Error()
	} else {
		syncReports++
		tc.Logf("  ExportUnmatchedReport: %d bytes", len(data))
		enrichedEndpoints++
	}

	if data, err := apiClient.Reporting.ExportSummaryReport(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["ExportSummaryReport"] = err.Error()
	} else {
		syncReports++
		tc.Logf("  ExportSummaryReport: %d bytes", len(data))
		enrichedEndpoints++
	}

	if data, err := apiClient.Reporting.ExportVarianceReport(ctx, reconciliationContextID, dateFrom, dateTo); err != nil {
		endpointFailures["ExportVarianceReport"] = err.Error()
	} else {
		syncReports++
		tc.Logf("  ExportVarianceReport: %d bytes", len(data))
		enrichedEndpoints++
	}

	// Async export job
	if exportJob, err := apiClient.Reporting.CreateExportJob(ctx, reconciliationContextID, client.CreateExportJobRequest{
		ReportType: "MATCHED",
		Format:     "csv",
		DateFrom:   dateFrom,
		DateTo:     dateTo,
	}); err != nil {
		endpointFailures["CreateExportJob"] = err.Error()
	} else {
		asyncJobs++
		tc.Logf("  CreateExportJob: %s status=%s", exportJob.JobID, exportJob.Status)
		enrichedEndpoints++

		// Wait for completion and try download
		if err := e2e.WaitForExportJobComplete(ctx, tc, apiClient, exportJob.JobID); err != nil {
			endpointFailures["WaitForExportJobComplete"] = err.Error()
		} else {
			if job, err := apiClient.Reporting.GetExportJob(ctx, exportJob.JobID); err != nil {
				endpointFailures["GetExportJob"] = err.Error()
			} else {
				tc.Logf("  GetExportJob: status=%s records=%d", job.Status, job.RecordsWritten)
				enrichedEndpoints++
			}

			if data, err := apiClient.Reporting.DownloadExportJob(ctx, exportJob.JobID); err != nil {
				endpointFailures["DownloadExportJob"] = err.Error()
			} else {
				tc.Logf("  DownloadExportJob: %d bytes", len(data))
				enrichedEndpoints++
			}
		}
	}

	// List export jobs
	if jobs, err := apiClient.Reporting.ListExportJobs(ctx); err != nil {
		endpointFailures["ListExportJobs"] = err.Error()
	} else {
		tc.Logf("  ListExportJobs: %d jobs", len(jobs))
		enrichedEndpoints++
	}

	// ============================================================
	// STEP 20: Governance Audit
	// ============================================================
	tc.Logf("\n[STEP 20/22] Governance audit...")

	// Wait for audit logs to propagate (async outbox)
	if logs, err := e2e.WaitForAuditLogs(ctx, tc, apiClient, "context", reconciliationContextID, 1); err != nil {
		endpointFailures["WaitForAuditLogs"] = err.Error()
	} else {
		auditLogsFound = len(logs)
		tc.Logf("  ListAuditLogsByEntity: %d logs for context", len(logs))
		enrichedEndpoints++

		// Get first audit log detail
		if len(logs) > 0 {
			if log, err := apiClient.Governance.GetAuditLog(ctx, logs[0].ID); err != nil {
				endpointFailures["GetAuditLog"] = err.Error()
			} else {
				tc.Logf("  GetAuditLog: %s action=%s entity=%s", log.ID, log.Action, log.EntityType)
				enrichedEndpoints++
			}
		}
	}

	// List by entity type
	if logs, err := apiClient.Governance.ListAuditLogsByEntityType(ctx, "context"); err != nil {
		endpointFailures["ListAuditLogsByEntityType"] = err.Error()
	} else {
		tc.Logf("  ListAuditLogsByEntityType: %d context logs", len(logs))
		enrichedEndpoints++
	}

	// List by action
	if logs, err := apiClient.Governance.ListAuditLogsByAction(ctx, "CREATE"); err != nil {
		endpointFailures["ListAuditLogsByAction"] = err.Error()
	} else {
		tc.Logf("  ListAuditLogsByAction(CREATE): %d logs", len(logs))
		enrichedEndpoints++
	}

	// List archives
	if archives, err := apiClient.Governance.ListArchives(ctx); err != nil {
		endpointFailures["ListArchives"] = err.Error()
	} else {
		tc.Logf("  ListArchives: %d archives", len(archives))
		enrichedEndpoints++
	}

	// ============================================================
	// STEP 21: Clone Context
	// ============================================================
	tc.Logf("\n[STEP 21/22] Clone context...")

	if cloneResp, err := apiClient.Configuration.CloneContext(ctx, reconciliationContextID, client.CloneContextRequest{
		Name: tc.UniqueName("stresser-clone"),
	}); err != nil {
		endpointFailures["CloneContext"] = err.Error()
	} else {
		clonedContextID = cloneResp.Context.ID
		tc.Logf("  CloneContext: %s (sources=%d, rules=%d, fieldMaps=%d)",
			cloneResp.Context.ID, cloneResp.SourcesCloned, cloneResp.RulesCloned, cloneResp.FieldMapsCloned)
		enrichedEndpoints++

		// Clean up cloned context (must delete children first: field maps → rules + sources → context)
		if !shouldSkipCleanup() {
			deleteClonedContext(ctx, tc, apiClient, clonedContextID, endpointFailures, &enrichedEndpoints)
		}
	}

	// ============================================================
	// STEP 22: Fee Schedule CRUD + Simulation
	// ============================================================
	tc.Logf("\n[STEP 22/22] Fee schedule CRUD + simulation...")
	feeScheduleName := tc.UniqueName("stresser-fee-schedule")

	feeSchedule, err := apiClient.FeeSchedule.CreateFeeSchedule(ctx, client.CreateFeeScheduleRequest{
		Name:             feeScheduleName,
		Currency:         "USD",
		ApplicationOrder: "PARALLEL",
		RoundingScale:    2,
		RoundingMode:     "HALF_UP",
		Items: []client.CreateFeeScheduleItemRequest{
			{
				Name:          "Processing Fee",
				Priority:      1,
				StructureType: "FLAT",
				Structure:     map[string]any{"amount": "2.50"},
			},
		},
	})
	if err != nil {
		endpointFailures["CreateFeeSchedule"] = err.Error()
	} else {
		tc.Logf("  CreateFeeSchedule: %s", feeSchedule.ID)
		enrichedEndpoints++

		if got, err := apiClient.FeeSchedule.GetFeeSchedule(ctx, feeSchedule.ID); err != nil {
			endpointFailures["GetFeeSchedule"] = err.Error()
		} else {
			tc.Logf("  GetFeeSchedule: %s currency=%s", got.Name, got.Currency)
			enrichedEndpoints++
		}

		if list, err := apiClient.FeeSchedule.ListFeeSchedules(ctx); err != nil {
			endpointFailures["ListFeeSchedules"] = err.Error()
		} else {
			tc.Logf("  ListFeeSchedules: %d schedules", len(list))
			enrichedEndpoints++
		}

		newName := tc.UniqueName("stresser-fee-updated")
		if _, err := apiClient.FeeSchedule.UpdateFeeSchedule(ctx, feeSchedule.ID, client.UpdateFeeScheduleRequest{
			Name: &newName,
		}); err != nil {
			endpointFailures["UpdateFeeSchedule"] = err.Error()
		} else {
			tc.Logf("  UpdateFeeSchedule: renamed to %s", newName)
			enrichedEndpoints++
		}

		if sim, err := apiClient.FeeSchedule.SimulateFeeSchedule(ctx, feeSchedule.ID, client.SimulateFeeRequest{
			GrossAmount: "1000.00",
			Currency:    "USD",
		}); err != nil {
			endpointFailures["SimulateFeeSchedule"] = err.Error()
		} else {
			feeScheduleSimulated = true
			tc.Logf("  SimulateFeeSchedule: gross=%s net=%s fee=%s", sim.GrossAmount, sim.NetAmount, sim.TotalFee)
			enrichedEndpoints++
		}

		if !shouldSkipCleanup() {
			if err := apiClient.FeeSchedule.DeleteFeeSchedule(ctx, feeSchedule.ID); err != nil {
				endpointFailures["DeleteFeeSchedule"] = err.Error()
			} else {
				tc.Logf("  DeleteFeeSchedule: removed %s", feeSchedule.ID)
				enrichedEndpoints++
			}
		} else {
			tc.Logf("  DeleteFeeSchedule: SKIPPED (E2E_KEEP_DATA set, preserving %s)", feeSchedule.ID)
		}
	}

	finalizeDashboardStresserHighVolumeEnrichment(
		t,
		tc,
		reconciliationContextID,
		reconciliationContextName,
		dashboard,
		matchGroups,
		endpointFailures,
		dashboardStresserHighVolumeStats{
			enrichedEndpoints:    enrichedEndpoints,
			scheduleCreated:      scheduleCreated,
			clonedContextID:      clonedContextID,
			feeScheduleSimulated: feeScheduleSimulated,
			exceptionsFound:      exceptionsFound,
			commentsAdded:        commentsAdded,
			commentsDeleted:      commentsDeleted,
			disputesOpened:       disputesOpened,
			disputesClosed:       disputesClosed,
			bulkAssigned:         bulkAssigned,
			bulkResolved:         bulkResolved,
			bulkDispatched:       bulkDispatched,
			forceMatched:         forceMatched,
			adjustedEntries:      adjustedEntries,
			dispatched:           dispatched,
			syncReports:          syncReports,
			asyncJobs:            asyncJobs,
			auditLogsFound:       auditLogsFound,
			searchResultsFound:   searchResultsFound,
		},
	)
}
