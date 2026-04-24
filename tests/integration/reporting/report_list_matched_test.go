//go:build integration

package reporting

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	reportRepo "github.com/LerianStudio/matcher/internal/reporting/adapters/postgres/report"
	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	pgcommon "github.com/LerianStudio/matcher/internal/shared/adapters/postgres/common"
	"github.com/LerianStudio/matcher/tests/integration"
)

// TestIntegration_Reporting_ListMatched_EmptyResult verifies that ListMatched returns an empty slice
// and no cursor when no matched data exists within the queried date range.
func TestIntegration_Reporting_ListMatched_EmptyResult(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2099, 12, 31, 23, 59, 59, 0, time.UTC),
			Limit:     10,
			SortOrder: "ASC",
		}

		items, pagination, err := repo.ListMatched(ctx, filter)
		require.NoError(t, err)
		require.Empty(t, items, "expected empty slice when no matched data in range")
		require.Empty(t, pagination.Next, "expected no next cursor on empty result")
		require.Empty(t, pagination.Prev, "expected no prev cursor on empty result")
	})
}

// TestIntegration_Reporting_ListMatched_ReturnsAllFields inserts a single matched transaction chain
// (ingestion_job → transaction → match_run → match_group CONFIRMED → match_item)
// and verifies that ListMatched returns all MatchedItem fields correctly.
func TestIntegration_Reporting_ListMatched_ReturnsAllFields(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		txID := uuid.New()
		groupID := uuid.New()
		baseDate := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
		amount := decimal.NewFromFloat(1234.56)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO transactions
					(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, 'BRL', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
			`, txID, jobID, seed.SourceID, "FIELDS-"+txID.String()[:8], amount, baseDate)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
				VALUES ($1, $2, $3, $4, 99, 'CONFIRMED')
			`, groupID, seed.ContextID, runID, seed.RuleID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
				VALUES ($1, $2, $3, $4, 'BRL')
			`, uuid.New(), groupID, txID, amount)
			if execErr != nil {
				return struct{}{}, execErr
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
			Limit:     10,
			SortOrder: "ASC",
		}

		items, _, err := repo.ListMatched(ctx, filter)
		require.NoError(t, err)
		require.Len(t, items, 1, "expected exactly 1 matched item")

		item := items[0]
		require.Equal(t, txID, item.TransactionID, "TransactionID mismatch")
		require.Equal(t, groupID, item.MatchGroupID, "MatchGroupID mismatch")
		require.Equal(t, seed.SourceID, item.SourceID, "SourceID mismatch")
		require.True(t, amount.Equal(item.Amount), "Amount mismatch: got %s, want %s", item.Amount, amount)
		require.Equal(t, "BRL", item.Currency, "Currency mismatch")
		require.WithinDuration(t, baseDate, item.Date, time.Second, "Date mismatch")
	})
}

// TestIntegration_Reporting_ListMatched_PaginationForward inserts 5 matched transactions, then pages
// through them with Limit=2 in ASC order. Verifies: first page has 2 items + cursor,
// second page has 2 items + cursor, third page has 1 item + empty cursor.
func TestIntegration_Reporting_ListMatched_PaginationForward(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 2, 10, 8, 0, 0, 0, time.UTC)
		const totalItems = 5

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i := range totalItems {
				txID := uuid.New()
				amount := decimal.NewFromFloat(100.00 + float64(i)*10)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "PAGE-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 90, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		dateFilter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 2, 28, 23, 59, 59, 0, time.UTC),
			Limit:     2,
			SortOrder: "ASC",
		}

		// --- Page 1: expect 2 items and a next cursor ---
		page1Items, page1Pagination, err := repo.ListMatched(ctx, dateFilter)
		require.NoError(t, err)
		require.Len(t, page1Items, 2, "page 1 should have 2 items")
		require.NotEmpty(t, page1Pagination.Next, "page 1 should have a next cursor")

		// --- Page 2: use cursor from page 1 ---
		page2Filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 2, 28, 23, 59, 59, 0, time.UTC),
			Limit:     2,
			SortOrder: "ASC",
			Cursor:    page1Pagination.Next,
		}

		page2Items, page2Pagination, err := repo.ListMatched(ctx, page2Filter)
		require.NoError(t, err)
		require.Len(t, page2Items, 2, "page 2 should have 2 items")
		require.NotEmpty(t, page2Pagination.Next, "page 2 should have a next cursor")

		// --- Page 3: use cursor from page 2 ---
		page3Filter := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 2, 28, 23, 59, 59, 0, time.UTC),
			Limit:     2,
			SortOrder: "ASC",
			Cursor:    page2Pagination.Next,
		}

		page3Items, page3Pagination, err := repo.ListMatched(ctx, page3Filter)
		require.NoError(t, err)
		require.Len(t, page3Items, 1, "page 3 should have 1 remaining item")
		require.Empty(t, page3Pagination.Next, "page 3 should have no next cursor (last page)")

		// Verify no overlap: all 5 items should be distinct transaction IDs.
		allIDs := make(map[uuid.UUID]struct{})
		for _, item := range page1Items {
			allIDs[item.TransactionID] = struct{}{}
		}
		for _, item := range page2Items {
			allIDs[item.TransactionID] = struct{}{}
		}
		for _, item := range page3Items {
			allIDs[item.TransactionID] = struct{}{}
		}
		require.Len(t, allIDs, totalItems, "all pages combined should yield %d distinct transactions", totalItems)
	})
}

// TestIntegration_Reporting_ListMatched_SourceFilter inserts matched transactions across 2 distinct sources,
// then filters by SourceID and verifies only the filtered source's items are returned.
func TestIntegration_Reporting_ListMatched_SourceFilter(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		seed := seedDashboardConfig(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		source2ID := uuid.New()
		baseDate := time.Date(2025, 3, 5, 9, 0, 0, 0, time.UTC)

		const source1Count = 3
		const source2Count = 2

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			// Create second source
			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO reconciliation_sources (id, context_id, name, type, side, config, created_at, updated_at)
				VALUES ($1, $2, $3, 'BANK', 'RIGHT', '{}', NOW(), NOW())
			`, source2ID, seed.ContextID, "Second Source")
			if execErr != nil {
				return struct{}{}, execErr
			}

			jobID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, seed.ContextID, seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Need a separate ingestion job for source2 (FK constraint: ingestion_jobs.source_id)
			jobID2 := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID2, seed.ContextID, source2ID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			runID := uuid.New()

			_, execErr = tx.ExecContext(ctx, `
				INSERT INTO match_runs (id, context_id, mode, status, started_at, stats)
				VALUES ($1, $2, 'COMMIT', 'COMPLETED', NOW(), '{}')
			`, runID, seed.ContextID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			// Insert matched transactions for source 1
			for i := range source1Count {
				txID := uuid.New()
				amount := decimal.NewFromFloat(200.00 + float64(i)*25)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID, seed.SourceID, "SRC1-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 95, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'USD')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			// Insert matched transactions for source 2
			for i := range source2Count {
				txID := uuid.New()
				amount := decimal.NewFromFloat(500.00 + float64(i)*50)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'EUR', $6, 'COMPLETE', 'MATCHED', NOW(), NOW())
				`, txID, jobID2, source2ID, "SRC2-"+txID.String()[:8], amount, baseDate.Add(time.Duration(source1Count+i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				groupID := uuid.New()

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_groups (id, context_id, run_id, rule_id, confidence, status)
					VALUES ($1, $2, $3, $4, 88, 'CONFIRMED')
				`, groupID, seed.ContextID, runID, seed.RuleID)
				if execErr != nil {
					return struct{}{}, execErr
				}

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO match_items (id, match_group_id, transaction_id, allocated_amount, allocated_currency)
					VALUES ($1, $2, $3, $4, 'EUR')
				`, uuid.New(), groupID, txID, amount)
				if execErr != nil {
					return struct{}{}, execErr
				}
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		dateRange := entities.ReportFilter{
			ContextID: seed.ContextID,
			DateFrom:  time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 3, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
			SortOrder: "ASC",
		}

		// Verify unfiltered returns all 5
		allItems, _, err := repo.ListMatched(ctx, dateRange)
		require.NoError(t, err)
		require.Len(t, allItems, source1Count+source2Count, "unfiltered should return all matched items")

		// Filter by source 1 only
		src1Filter := dateRange
		src1Filter.SourceID = &seed.SourceID

		src1Items, _, err := repo.ListMatched(ctx, src1Filter)
		require.NoError(t, err)
		require.Len(t, src1Items, source1Count, "source 1 filter should return %d items", source1Count)

		for _, item := range src1Items {
			require.Equal(t, seed.SourceID, item.SourceID, "all items should belong to source 1")
		}

		// Filter by source 2 only
		src2Filter := dateRange
		src2Filter.SourceID = &source2ID

		src2Items, _, err := repo.ListMatched(ctx, src2Filter)
		require.NoError(t, err)
		require.Len(t, src2Items, source2Count, "source 2 filter should return %d items", source2Count)

		for _, item := range src2Items {
			require.Equal(t, source2ID, item.SourceID, "all items should belong to source 2")
		}
	})
}

// TestIntegration_Reporting_ListUnmatched_BasicQuery inserts 3 unmatched transactions (status=UNMATCHED)
// and verifies ListUnmatched returns them with correct fields.
func TestIntegration_Reporting_ListUnmatched_BasicQuery(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 4, 10, 14, 0, 0, 0, time.UTC)
		const unmatchedCount = 3

		type txRecord struct {
			id     uuid.UUID
			amount decimal.Decimal
		}

		inserted := make([]txRecord, 0, unmatchedCount)

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			for i := range unmatchedCount {
				txID := uuid.New()
				amount := decimal.NewFromFloat(300.00 + float64(i)*75)

				_, execErr = tx.ExecContext(ctx, `
					INSERT INTO transactions
						(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
					VALUES ($1, $2, $3, $4, $5, 'GBP', $6, 'COMPLETE', 'UNMATCHED', NOW(), NOW())
				`, txID, jobID, h.Seed.SourceID, "BASIC-UM-"+txID.String()[:8], amount, baseDate.Add(time.Duration(i)*time.Hour))
				if execErr != nil {
					return struct{}{}, execErr
				}

				inserted = append(inserted, txRecord{id: txID, amount: amount})
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 4, 30, 23, 59, 59, 0, time.UTC),
			Limit:     100,
			SortOrder: "ASC",
		}

		items, _, err := repo.ListUnmatched(ctx, filter)
		require.NoError(t, err)
		require.Len(t, items, unmatchedCount, "expected %d unmatched items", unmatchedCount)

		// Build a map of inserted IDs for lookup
		insertedByID := make(map[uuid.UUID]txRecord)
		for _, rec := range inserted {
			insertedByID[rec.id] = rec
		}

		for _, item := range items {
			rec, exists := insertedByID[item.TransactionID]
			require.True(t, exists, "returned TransactionID %s was not inserted", item.TransactionID)
			require.Equal(t, h.Seed.SourceID, item.SourceID, "SourceID mismatch")
			require.True(t, rec.amount.Equal(item.Amount), "Amount mismatch for tx %s: got %s, want %s", item.TransactionID, item.Amount, rec.amount)
			require.Equal(t, "GBP", item.Currency, "Currency mismatch")
			require.Equal(t, "UNMATCHED", item.Status, "Status mismatch")
			// No exceptions linked, so ExceptionID should be nil
			require.Nil(t, item.ExceptionID, "ExceptionID should be nil when no exception linked")
			require.Nil(t, item.DueAt, "DueAt should be nil when no exception linked")
		}
	})
}

// TestIntegration_Reporting_ListUnmatched_StatusFilter inserts transactions with mixed statuses
// (UNMATCHED, PENDING_REVIEW, MATCHED) and verifies ListUnmatched excludes
// MATCHED but includes UNMATCHED and PENDING_REVIEW (the query uses t.status != 'MATCHED').
func TestIntegration_Reporting_ListUnmatched_StatusFilter(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		ctx := testCtx(t, h)
		repo := reportRepo.NewRepository(h.Provider())

		baseDate := time.Date(2025, 5, 20, 11, 0, 0, 0, time.UTC)

		statusCounts := map[string]int{
			"UNMATCHED":      2,
			"PENDING_REVIEW": 2,
			"MATCHED":        3,
		}

		_, err := pgcommon.WithTenantTx(ctx, h.Connection, func(tx *sql.Tx) (struct{}, error) {
			jobID := uuid.New()

			_, execErr := tx.ExecContext(ctx, `
				INSERT INTO ingestion_jobs (id, context_id, source_id, status, started_at, metadata)
				VALUES ($1, $2, $3, 'COMPLETED', NOW(), '{}')
			`, jobID, h.Seed.ContextID, h.Seed.SourceID)
			if execErr != nil {
				return struct{}{}, execErr
			}

			offset := 0

			for status, count := range statusCounts {
				for i := range count {
					txID := uuid.New()
					amount := decimal.NewFromFloat(100.00 + float64(offset+i)*10)

					_, execErr = tx.ExecContext(ctx, `
						INSERT INTO transactions
							(id, ingestion_job_id, source_id, external_id, amount, currency, date, extraction_status, status, created_at, updated_at)
						VALUES ($1, $2, $3, $4, $5, 'USD', $6, 'COMPLETE', $7, NOW(), NOW())
					`, txID, jobID, h.Seed.SourceID, "STATUS-"+txID.String()[:8], amount, baseDate.Add(time.Duration(offset+i)*time.Hour), status)
					if execErr != nil {
						return struct{}{}, execErr
					}
				}

				offset += count
			}

			return struct{}{}, nil
		})
		require.NoError(t, err)

		filter := entities.ReportFilter{
			ContextID: h.Seed.ContextID,
			DateFrom:  time.Date(2025, 5, 1, 0, 0, 0, 0, time.UTC),
			DateTo:    time.Date(2025, 5, 31, 23, 59, 59, 0, time.UTC),
			Limit:     100,
			SortOrder: "ASC",
		}

		// ListUnmatched uses t.status != 'MATCHED', so it should include
		// UNMATCHED + PENDING_REVIEW = 4, and exclude MATCHED = 3.
		expectedCount := statusCounts["UNMATCHED"] + statusCounts["PENDING_REVIEW"]

		items, _, err := repo.ListUnmatched(ctx, filter)
		require.NoError(t, err)
		require.Len(t, items, expectedCount,
			"ListUnmatched should return %d items (UNMATCHED + PENDING_REVIEW), got %d",
			expectedCount, len(items))

		// Verify none of the returned items have status MATCHED
		for _, item := range items {
			require.NotEqual(t, "MATCHED", item.Status,
				"ListUnmatched must not return MATCHED transactions (got tx %s with status %s)",
				item.TransactionID, item.Status)
		}

		// Count returned statuses to verify distribution
		statusMap := make(map[string]int)
		for _, item := range items {
			statusMap[item.Status]++
		}

		require.Equal(t, statusCounts["UNMATCHED"], statusMap["UNMATCHED"],
			"expected %d UNMATCHED items", statusCounts["UNMATCHED"])
		require.Equal(t, statusCounts["PENDING_REVIEW"], statusMap["PENDING_REVIEW"],
			"expected %d PENDING_REVIEW items", statusCounts["PENDING_REVIEW"])
	})
}
