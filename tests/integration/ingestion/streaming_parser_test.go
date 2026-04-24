//go:build integration

package ingestion

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/ingestion/adapters/parsers"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/tests/integration"
)

// streamingTestFixtures holds the shared scaffolding every streaming parser test needs:
// an IngestionJob (with valid ContextID/SourceID from the harness seed) and a FieldMap
// whose mapping keys match the CSV/JSON columns used in the generated test data.
type streamingTestFixtures struct {
	job      *entities.IngestionJob
	fieldMap *shared.FieldMap
}

// newStreamingFixtures creates a valid IngestionJob + FieldMap for parser tests.
// The job is in QUEUED state (parsers only read ID/SourceID — no state transition needed).
func newStreamingFixtures(
	t *testing.T,
	h *integration.TestHarness,
) streamingTestFixtures {
	t.Helper()

	ctx := h.Ctx()

	job, err := entities.NewIngestionJob(
		ctx,
		h.Seed.ContextID,
		h.Seed.SourceID,
		"streaming_test.csv",
		0,
	)
	require.NoError(t, err, "creating ingestion job fixture")

	fieldMap := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: h.Seed.ContextID,
		SourceID:  h.Seed.SourceID,
		Mapping: map[string]any{
			"external_id": "id",
			"amount":      "amount",
			"currency":    "currency",
			"date":        "date",
			"description": "description",
		},
	}

	return streamingTestFixtures{job: job, fieldMap: fieldMap}
}

// generateLargeCSV creates a well-formed CSV string with the given number of data rows.
// Every row is valid: parseable amount, ISO-4217 currency, and unambiguous YYYY-MM-DD date.
func generateLargeCSV(numRows int) string {
	var sb strings.Builder

	sb.WriteString("id,amount,currency,date,description\n")

	for i := 1; i <= numRows; i++ {
		fmt.Fprintf(
			&sb,
			"TX-%d,%.2f,USD,2024-01-%02d,Payment %d\n",
			i, float64(i)*10.0, (i%28)+1, i,
		)
	}

	return sb.String()
}

// generateLargeJSON creates a JSON array with the given number of objects.
// Field names match the FieldMap produced by newStreamingFixtures.
func generateLargeJSON(numObjects int) string {
	var sb strings.Builder

	sb.WriteString("[")

	for i := 1; i <= numObjects; i++ {
		if i > 1 {
			sb.WriteString(",")
		}

		fmt.Fprintf(
			&sb,
			`{"id":"TX-%d","amount":"%.2f","currency":"USD","date":"2024-01-%02d","description":"Payment %d"}`,
			i, float64(i)*10.0, (i%28)+1, i,
		)
	}

	sb.WriteString("]")

	return sb.String()
}

// --------------------------------------------------------------------------
// Test 1: CSV streaming parser processes all rows from a large file.
// --------------------------------------------------------------------------

func TestIntegration_Ingestion_StreamingParser_CSV_LargeFile(t *testing.T) {
	t.Parallel()

	const totalRows = 1000

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		fix := newStreamingFixtures(t, h)
		csvParser := parsers.NewCSVParser()

		csvData := generateLargeCSV(totalRows)

		var collected []*shared.Transaction

		result, err := csvParser.ParseStreaming(
			context.Background(),
			strings.NewReader(csvData),
			fix.job,
			fix.fieldMap,
			ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, chunkErrors []ports.ParseError) error {
				collected = append(collected, chunk...)
				return nil
			},
		)
		require.NoError(t, err)

		// Every row is valid — we expect exactly totalRows transactions.
		require.Equal(t, totalRows, result.TotalRecords,
			"StreamingParseResult.TotalRecords must match generated rows")
		require.Equal(t, 0, result.TotalErrors,
			"all rows are valid — no errors expected")
		require.Len(t, collected, totalRows,
			"callback must have delivered every parsed transaction")

		// Spot-check first and last transaction.
		require.Equal(t, "TX-1", collected[0].ExternalID)
		require.Equal(t, fmt.Sprintf("TX-%d", totalRows), collected[totalRows-1].ExternalID)

		// Date range must span the generated dates (Jan 1 – Jan 28 cycle).
		require.NotNil(t, result.DateRange,
			"DateRange must be populated for non-empty input")
		require.False(t, result.DateRange.Start.IsZero())
		require.False(t, result.DateRange.End.IsZero())
	})
}

// --------------------------------------------------------------------------
// Test 2: CSV streaming parser delivers data in batches via callback.
// --------------------------------------------------------------------------

func TestIntegration_Ingestion_StreamingParser_CSV_BatchCallbacks(t *testing.T) {
	t.Parallel()

	const (
		totalRows = 250
		chunkSize = 100
	)

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		fix := newStreamingFixtures(t, h)
		csvParser := parsers.NewCSVParser()

		csvData := generateLargeCSV(totalRows)

		var callbackCount atomic.Int64

		var totalDelivered atomic.Int64

		result, err := csvParser.ParseStreaming(
			context.Background(),
			strings.NewReader(csvData),
			fix.job,
			fix.fieldMap,
			chunkSize,
			func(chunk []*shared.Transaction, _ []ports.ParseError) error {
				callbackCount.Add(1)
				totalDelivered.Add(int64(len(chunk)))
				return nil
			},
		)
		require.NoError(t, err)

		// 250 rows / 100 chunk = 2 full chunks + 1 remainder chunk of 50.
		// That's at least 3 callback invocations (2 full + 1 tail).
		require.GreaterOrEqual(t, callbackCount.Load(), int64(3),
			"callback must be invoked multiple times for chunked delivery")
		require.Equal(t, int64(totalRows), totalDelivered.Load(),
			"sum of all chunks must equal total rows")
		require.Equal(t, totalRows, result.TotalRecords)
	})
}

// --------------------------------------------------------------------------
// Test 3: JSON streaming parser processes all objects from a large array.
// --------------------------------------------------------------------------

func TestIntegration_Ingestion_StreamingParser_JSON_LargeFile(t *testing.T) {
	t.Parallel()

	const totalObjects = 500

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		fix := newStreamingFixtures(t, h)
		jsonParser := parsers.NewJSONParser()

		jsonData := generateLargeJSON(totalObjects)

		var collected []*shared.Transaction

		result, err := jsonParser.ParseStreaming(
			context.Background(),
			strings.NewReader(jsonData),
			fix.job,
			fix.fieldMap,
			ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, chunkErrors []ports.ParseError) error {
				collected = append(collected, chunk...)
				return nil
			},
		)
		require.NoError(t, err)

		require.Equal(t, totalObjects, result.TotalRecords,
			"StreamingParseResult.TotalRecords must match generated objects")
		require.Equal(t, 0, result.TotalErrors,
			"all objects are valid — no errors expected")
		require.Len(t, collected, totalObjects,
			"callback must have delivered every parsed transaction")

		// Spot-check first and last.
		require.Equal(t, "TX-1", collected[0].ExternalID)
		require.Equal(t, fmt.Sprintf("TX-%d", totalObjects), collected[totalObjects-1].ExternalID)

		require.NotNil(t, result.DateRange)
	})
}

// --------------------------------------------------------------------------
// Test 4: CSV streaming parser handles an empty file (headers only or no data).
// --------------------------------------------------------------------------

func TestIntegration_Ingestion_StreamingParser_CSV_EmptyFile(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		fix := newStreamingFixtures(t, h)
		csvParser := parsers.NewCSVParser()

		// Header-only CSV — zero data rows.
		csvData := "id,amount,currency,date,description\n"

		var callbackCount int

		result, err := csvParser.ParseStreaming(
			context.Background(),
			strings.NewReader(csvData),
			fix.job,
			fix.fieldMap,
			ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, _ []ports.ParseError) error {
				callbackCount++
				return nil
			},
		)

		// The parser reads headers successfully, finds no rows, and returns
		// an empty result. This is not an error condition — it's a valid
		// (albeit trivial) file.
		require.NoError(t, err, "header-only CSV is not an error")
		require.Equal(t, 0, result.TotalRecords, "no data rows means zero records")
		require.Equal(t, 0, result.TotalErrors, "no data rows means zero errors")
		require.Nil(t, result.DateRange, "no transactions means no date range")
	})
}

// --------------------------------------------------------------------------
// Test 5: CSV streaming parser handles malformed rows gracefully.
// --------------------------------------------------------------------------

func TestIntegration_Ingestion_StreamingParser_CSV_MalformedRows(t *testing.T) {
	t.Parallel()

	integration.RunWithDatabase(t, func(t *testing.T, h *integration.TestHarness) {
		fix := newStreamingFixtures(t, h)
		csvParser := parsers.NewCSVParser()

		// Mix valid and invalid rows:
		//   - Row 1: valid
		//   - Row 2: empty amount (required field)
		//   - Row 3: valid
		//   - Row 4: invalid date
		//   - Row 5: invalid currency (XYZ is not ISO 4217)
		//   - Row 6: valid
		csvData := strings.Join([]string{
			"id,amount,currency,date,description",
			"TX-1,100.00,USD,2024-01-15,Good row",
			"TX-2,,USD,2024-01-16,Missing amount",
			"TX-3,300.00,USD,2024-01-17,Good row",
			"TX-4,400.00,USD,not-a-date,Bad date",
			"TX-5,500.00,XYZ,2024-01-19,Bad currency",
			"TX-6,600.00,EUR,2024-01-20,Good row",
			"",
		}, "\n")

		var (
			collectedTxns  []*shared.Transaction
			collectedErrs  []ports.ParseError
			callbackCalled bool
		)

		result, err := csvParser.ParseStreaming(
			context.Background(),
			strings.NewReader(csvData),
			fix.job,
			fix.fieldMap,
			ports.DefaultChunkSize,
			func(chunk []*shared.Transaction, chunkErrors []ports.ParseError) error {
				callbackCalled = true
				collectedTxns = append(collectedTxns, chunk...)
				collectedErrs = append(collectedErrs, chunkErrors...)
				return nil
			},
		)
		require.NoError(t, err, "malformed rows produce parse errors, not a top-level error")
		require.True(t, callbackCalled, "callback must be invoked even with mixed data")

		// 3 valid rows should have been parsed successfully.
		require.Equal(t, 3, result.TotalRecords,
			"only valid rows count toward TotalRecords")
		require.Len(t, collectedTxns, 3,
			"callback should deliver exactly 3 valid transactions")

		// 3 malformed rows should have been captured as errors.
		require.Equal(t, 3, result.TotalErrors,
			"3 rows have parse errors")
		require.Len(t, collectedErrs, 3,
			"callback should deliver exactly 3 parse errors")

		// Verify the valid transactions are the ones we expect.
		validIDs := make([]string, 0, len(collectedTxns))
		for _, tx := range collectedTxns {
			validIDs = append(validIDs, tx.ExternalID)
		}

		require.Contains(t, validIDs, "TX-1")
		require.Contains(t, validIDs, "TX-3")
		require.Contains(t, validIDs, "TX-6")

		// Verify error rows reference the correct row numbers.
		// Row numbering: header=1, data rows start at 2.
		errorRows := make([]int, 0, len(collectedErrs))
		for _, pe := range collectedErrs {
			errorRows = append(errorRows, pe.Row)
		}

		require.Contains(t, errorRows, 3, "row 3 (TX-2) has empty amount")
		require.Contains(t, errorRows, 5, "row 5 (TX-4) has invalid date")
		require.Contains(t, errorRows, 6, "row 6 (TX-5) has invalid currency")
	})
}
