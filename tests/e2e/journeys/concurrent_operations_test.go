//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/factories"
)

// TestConcurrentOperations_ParallelIngestion tests parallel file uploads to different sources.
func TestConcurrentOperations_ParallelIngestion(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().
				WithName("parallel-ingest").
				MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PAR-001", "100.00", "USD", "2026-01-15", "tx1").
				AddRow("PAR-002", "200.00", "USD", "2026-01-16", "tx2").
				AddRow("PAR-003", "300.00", "USD", "2026-01-17", "tx3").
				Build()

			bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
				AddRow("PAR-001", "100.00", "USD", "2026-01-15", "tx1").
				AddRow("PAR-002", "200.00", "USD", "2026-01-16", "tx2").
				AddRow("PAR-003", "300.00", "USD", "2026-01-17", "tx3").
				Build()

			var wg sync.WaitGroup
			var ledgerErr, bankErr error
			var ledgerJobID, bankJobID string

			wg.Add(2)
			go func() {
				defer wg.Done()
				job, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					ledgerSource.ID,
					"ledger.csv",
					ledgerCSV,
				)
				if err != nil {
					ledgerErr = err
					return
				}
				ledgerJobID = job.ID
				ledgerErr = e2e.WaitForJobComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					job.ID,
				)
			}()

			go func() {
				defer wg.Done()
				job, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					bankSource.ID,
					"bank.csv",
					bankCSV,
				)
				if err != nil {
					bankErr = err
					return
				}
				bankJobID = job.ID
				bankErr = e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, job.ID)
			}()

			wg.Wait()
			require.NoError(t, ledgerErr, "ledger ingestion should complete")
			require.NoError(t, bankErr, "bank ingestion should complete")
			tc.Logf("Both jobs completed: ledger=%s, bank=%s", ledgerJobID, bankJobID)

			matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
			require.NoError(t, err)
			require.NoError(
				t,
				e2e.WaitForMatchRunComplete(
					ctx,
					tc,
					client,
					reconciliationContext.ID,
					matchResp.RunID,
				),
			)

			groups, err := client.Matching.GetMatchRunResults(
				ctx,
				reconciliationContext.ID,
				matchResp.RunID,
			)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(groups), 3, "all 3 transactions should match")

			tc.Logf("✓ Parallel ingestion completed with %d matches", len(groups))
		},
	)
}

// TestConcurrentOperations_MultipleContexts tests operations across multiple contexts simultaneously.
func TestConcurrentOperations_MultipleContexts(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			numContexts := 3
			var wg sync.WaitGroup
			errors := make(chan error, numContexts)

			for i := 0; i < numContexts; i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()

					reconciliationContext := f.Context.NewContext().
						WithName(fmt.Sprintf("multi-ctx-%d", idx)).
						MustCreate(ctx)
					ledgerSource := f.Source.NewSource(reconciliationContext.ID).
						WithName("ledger").
						AsLedger().
						MustCreate(ctx)
					bankSource := f.Source.NewSource(reconciliationContext.ID).
						WithName("bank").
						AsBank().
						MustCreate(ctx)

					f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
						WithStandardMapping().
						MustCreate(ctx)
					f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
						WithStandardMapping().
						MustCreate(ctx)
					f.Rule.NewRule(reconciliationContext.ID).
						Exact().
						WithExactConfig(true, true).
						MustCreate(ctx)

					csv := factories.NewCSVBuilder(tc.NamePrefix()).
						AddRow("MC-001", "100.00", "USD", "2026-01-15", "multi").
						Build()

					ledgerJob, err := client.Ingestion.UploadCSV(
						ctx,
						reconciliationContext.ID,
						ledgerSource.ID,
						"l.csv",
						csv,
					)
					if err != nil {
						errors <- err
						return
					}
					if err := e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, ledgerJob.ID); err != nil {
						errors <- err
						return
					}

					bankJob, err := client.Ingestion.UploadCSV(
						ctx,
						reconciliationContext.ID,
						bankSource.ID,
						"b.csv",
						csv,
					)
					if err != nil {
						errors <- err
						return
					}
					if err := e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, bankJob.ID); err != nil {
						errors <- err
						return
					}

					matchResp, err := client.Matching.RunMatchCommit(ctx, reconciliationContext.ID, "")
					if err != nil {
						errors <- err
						return
					}
					if err := e2e.WaitForMatchRunComplete(ctx, tc, client, reconciliationContext.ID, matchResp.RunID); err != nil {
						errors <- err
						return
					}

					tc.Logf("Context %d completed: %s", idx, reconciliationContext.ID)
				}(i)
			}

			wg.Wait()
			close(errors)

			for err := range errors {
				require.NoError(t, err)
			}

			tc.Logf("✓ Multiple contexts completed successfully")
		},
	)
}

// TestConcurrentOperations_RapidSequentialUploads tests rapid sequential uploads to same source.
func TestConcurrentOperations_RapidSequentialUploads(t *testing.T) {
	e2e.RunE2EWithTimeout(
		t,
		3*time.Minute,
		func(t *testing.T, tc *e2e.TestContext, client *e2e.Client) {
			ctx := context.Background()
			f := factories.New(tc, client)

			reconciliationContext := f.Context.NewContext().WithName("rapid-upload").MustCreate(ctx)
			ledgerSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("ledger").
				AsLedger().
				MustCreate(ctx)
			bankSource := f.Source.NewSource(reconciliationContext.ID).
				WithName("bank").
				AsBank().
				MustCreate(ctx)

			f.Source.NewFieldMap(reconciliationContext.ID, ledgerSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Source.NewFieldMap(reconciliationContext.ID, bankSource.ID).
				WithStandardMapping().
				MustCreate(ctx)
			f.Rule.NewRule(reconciliationContext.ID).
				Exact().
				WithExactConfig(true, true).
				MustCreate(ctx)

			numBatches := 3
			jobIDs := make([]string, 0, numBatches*2)

			for i := 0; i < numBatches; i++ {
				ledgerCSV := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRow("RAPID-L-"+string(rune('A'+i)), "100.00", "USD", "2026-01-15", "batch").
					Build()
				bankCSV := factories.NewCSVBuilder(tc.NamePrefix()).
					AddRow("RAPID-B-"+string(rune('A'+i)), "100.00", "USD", "2026-01-15", "batch").
					Build()

				ledgerJob, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					ledgerSource.ID,
					"l.csv",
					ledgerCSV,
				)
				require.NoError(t, err)
				jobIDs = append(jobIDs, ledgerJob.ID)

				bankJob, err := client.Ingestion.UploadCSV(
					ctx,
					reconciliationContext.ID,
					bankSource.ID,
					"b.csv",
					bankCSV,
				)
				require.NoError(t, err)
				jobIDs = append(jobIDs, bankJob.ID)
			}

			for _, jobID := range jobIDs {
				require.NoError(
					t,
					e2e.WaitForJobComplete(ctx, tc, client, reconciliationContext.ID, jobID),
				)
			}

			tc.Logf("✓ Rapid sequential uploads completed (%d jobs)", len(jobIDs))
		},
	)
}
