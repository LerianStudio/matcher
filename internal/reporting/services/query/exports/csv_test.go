//go:build unit

package exports

import (
	"bytes"
	"encoding/csv"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestBuildMatchedCSV_HeaderAndOrdering(t *testing.T) {
	t.Parallel()

	firstID := uuid.New()
	secondID := uuid.New()

	items := []*entities.MatchedItem{
		{
			TransactionID: secondID,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(20),
			Currency:      "USD",
			Date:          time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
		},
		{
			TransactionID: firstID,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(10),
			Currency:      "USD",
			Date:          time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	output, err := BuildMatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(
		t,
		[]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"},
		rows[0],
	)
	assert.Equal(t, firstID.String(), rows[1][0])
	assert.Equal(t, secondID.String(), rows[2][0])
}

func TestBuildMatchedCSV_NilItems(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)

	items := []*entities.MatchedItem{
		nil,
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(10),
			Currency:      "USD",
			Date:          fixedTime,
		},
	}

	output, err := BuildMatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestBuildMatchedCSV_SanitizesFormulaValues(t *testing.T) {
	t.Parallel()

	items := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(10),
			Currency:      "=USD",
			Date:          time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	output, err := BuildMatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "'=USD", rows[1][4])
}

func TestBuildUnmatchedCSV_HeaderAndOrdering(t *testing.T) {
	t.Parallel()

	firstID := uuid.New()
	secondID := uuid.New()
	exceptionID := uuid.New()
	dueAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	items := []*entities.UnmatchedItem{
		{
			TransactionID: secondID,
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "EUR",
			Status:        "PENDING",
			Date:          time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
			ExceptionID:   &exceptionID,
			DueAt:         &dueAt,
		},
		{
			TransactionID: firstID,
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "EUR",
			Status:        "UNMATCHED",
			Date:          time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		},
	}

	output, err := BuildUnmatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(
		t,
		[]string{
			"transaction_id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
		rows[0],
	)
	assert.Equal(t, firstID.String(), rows[1][0])
	assert.Empty(t, rows[1][6])
	assert.Empty(t, rows[1][7])
	assert.Equal(t, secondID.String(), rows[2][0])
	assert.Equal(t, exceptionID.String(), rows[2][6])
	assert.Equal(t, "2026-02-01T00:00:00Z", rows[2][7])
}

func TestBuildUnmatchedCSV_SanitizesFormulaValues(t *testing.T) {
	t.Parallel()

	items := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Status:        "=OPEN",
			Date:          time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
		},
	}

	output, err := BuildUnmatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "'=OPEN", rows[1][4])
}

func TestBuildSummaryCSV(t *testing.T) {
	t.Parallel()

	summary := &entities.SummaryReport{
		MatchedCount:    10,
		UnmatchedCount:  5,
		TotalAmount:     decimal.NewFromFloat(1500.50),
		MatchedAmount:   decimal.NewFromFloat(1000.25),
		UnmatchedAmount: decimal.NewFromFloat(500.25),
	}

	output, err := BuildSummaryCSV(summary)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(
		t,
		[]string{
			"matched_count",
			"unmatched_count",
			"total_amount",
			"matched_amount",
			"unmatched_amount",
		},
		rows[0],
	)
	assert.Equal(t, "10", rows[1][0])
	assert.Equal(t, "5", rows[1][1])
}

func TestBuildSummaryCSV_NegativeNumbersNotSanitized(t *testing.T) {
	t.Parallel()

	summary := &entities.SummaryReport{
		MatchedCount:    -1,
		UnmatchedCount:  2,
		TotalAmount:     decimal.NewFromInt(-100),
		MatchedAmount:   decimal.NewFromInt(-60),
		UnmatchedAmount: decimal.NewFromInt(-40),
	}

	output, err := BuildSummaryCSV(summary)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "-1", rows[1][0])
	assert.Equal(t, "-100", rows[1][2])
}

func TestBuildSummaryCSV_NilSummary(t *testing.T) {
	t.Parallel()

	_, err := BuildSummaryCSV(nil)
	require.ErrorIs(t, err, ErrSummaryRequired)
}

func TestBuildVarianceCSV_HeaderAndOrdering(t *testing.T) {
	t.Parallel()

	sourceA := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	sourceB := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	variancePct := decimal.NewFromFloat(10.0)

	rows := []*entities.VarianceReportRow{
		{
			SourceID:      sourceB,
			Currency:      "USD",
			FeeType:       "PERCENTAGE",
			TotalExpected: decimal.NewFromInt(100),
			TotalActual:   decimal.NewFromInt(110),
			NetVariance:   decimal.NewFromInt(10),
			VariancePct:   &variancePct,
		},
		{
			SourceID:      sourceA,
			Currency:      "USD",
			FeeType:       "FLAT",
			TotalExpected: decimal.NewFromInt(50),
			TotalActual:   decimal.NewFromInt(50),
			NetVariance:   decimal.Zero,
			VariancePct:   nil,
		},
	}

	output, err := BuildVarianceCSV(rows)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, csvRows, 3)
	assert.Equal(
		t,
		[]string{
			"source_id",
			"currency",
			"fee_type",
			"total_expected",
			"total_actual",
			"net_variance",
			"variance_pct",
		},
		csvRows[0],
	)
	assert.Equal(t, sourceA.String(), csvRows[1][0])
	assert.Equal(t, "FLAT", csvRows[1][2])
	assert.Empty(t, csvRows[1][6])
	assert.Equal(t, sourceB.String(), csvRows[2][0])
	assert.Equal(t, "PERCENTAGE", csvRows[2][2])
	assert.Equal(t, "10.00", csvRows[2][6])
}

func TestBuildVarianceCSV_SanitizesFormulaValues(t *testing.T) {
	t.Parallel()

	rows := []*entities.VarianceReportRow{
		{
			SourceID:      uuid.New(),
			Currency:      "=USD",
			FeeType:       "+FEE",
			TotalExpected: decimal.NewFromInt(100),
			TotalActual:   decimal.NewFromInt(90),
			NetVariance:   decimal.NewFromInt(-10),
			VariancePct:   nil,
		},
	}

	output, err := BuildVarianceCSV(rows)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, csvRows, 2)
	assert.Equal(t, "'=USD", csvRows[1][1])
	assert.Equal(t, "'+FEE", csvRows[1][2])
	assert.Equal(t, "-10", csvRows[1][5])
}

func TestBuildVarianceCSV_NilRowsSkipped(t *testing.T) {
	t.Parallel()

	variancePct := decimal.NewFromFloat(5.0)
	rows := []*entities.VarianceReportRow{
		nil,
		{
			SourceID:      uuid.New(),
			Currency:      "EUR",
			FeeType:       "TIERED",
			TotalExpected: decimal.NewFromInt(200),
			TotalActual:   decimal.NewFromInt(210),
			NetVariance:   decimal.NewFromInt(10),
			VariancePct:   &variancePct,
		},
	}

	output, err := BuildVarianceCSV(rows)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, csvRows, 2)
}

func TestBuildVarianceCSV_EmptyRows(t *testing.T) {
	t.Parallel()

	output, err := BuildVarianceCSV([]*entities.VarianceReportRow{})
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, csvRows, 1)
	assert.Equal(
		t,
		[]string{
			"source_id",
			"currency",
			"fee_type",
			"total_expected",
			"total_actual",
			"net_variance",
			"variance_pct",
		},
		csvRows[0],
	)
}

func TestBuildMatchedCSV_EmptyItems(t *testing.T) {
	t.Parallel()

	output, err := BuildMatchedCSV([]*entities.MatchedItem{})
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(
		t,
		[]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"},
		rows[0],
	)
}

func TestBuildUnmatchedCSV_EmptyItems(t *testing.T) {
	t.Parallel()

	output, err := BuildUnmatchedCSV([]*entities.UnmatchedItem{})
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(
		t,
		[]string{
			"transaction_id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
		rows[0],
	)
}

func TestBuildUnmatchedCSV_NilItems(t *testing.T) {
	t.Parallel()

	items := []*entities.UnmatchedItem{
		nil,
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(10),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC(),
		},
	}

	output, err := BuildUnmatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestBuildMatchedCSV_SortsSameDateByTransactionID(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	sameDate := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	items := []*entities.MatchedItem{
		{
			TransactionID: id2,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "USD",
			Date:          sameDate,
		},
		{
			TransactionID: id1,
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          sameDate,
		},
	}

	output, err := BuildMatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, id1.String(), rows[1][0])
	assert.Equal(t, id2.String(), rows[2][0])
}

func TestBuildUnmatchedCSV_SortsSameDateByTransactionID(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	sameDate := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	items := []*entities.UnmatchedItem{
		{
			TransactionID: id2,
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          sameDate,
		},
		{
			TransactionID: id1,
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          sameDate,
		},
	}

	output, err := BuildUnmatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, rows, 3)
	assert.Equal(t, id1.String(), rows[1][0])
	assert.Equal(t, id2.String(), rows[2][0])
}

func TestBuildVarianceCSV_SortsByCurrencyThenFeeType(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()

	rows := []*entities.VarianceReportRow{
		{
			SourceID:      sourceID,
			Currency:      "USD",
			FeeType:       "PERCENTAGE",
			TotalExpected: decimal.NewFromInt(100),
			TotalActual:   decimal.NewFromInt(100),
			NetVariance:   decimal.Zero,
		},
		{
			SourceID:      sourceID,
			Currency:      "EUR",
			FeeType:       "FLAT",
			TotalExpected: decimal.NewFromInt(50),
			TotalActual:   decimal.NewFromInt(50),
			NetVariance:   decimal.Zero,
		},
		{
			SourceID:      sourceID,
			Currency:      "EUR",
			FeeType:       "PERCENTAGE",
			TotalExpected: decimal.NewFromInt(75),
			TotalActual:   decimal.NewFromInt(75),
			NetVariance:   decimal.Zero,
		},
	}

	output, err := BuildVarianceCSV(rows)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	require.Len(t, csvRows, 4)
	assert.Equal(t, "EUR", csvRows[1][1])
	assert.Equal(t, "FLAT", csvRows[1][2])
	assert.Equal(t, "EUR", csvRows[2][1])
	assert.Equal(t, "PERCENTAGE", csvRows[2][2])
	assert.Equal(t, "USD", csvRows[3][1])
}

func TestSanitizeCSVValue_EmptyString(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("")
	assert.Equal(t, "", result)
}

func TestSanitizeCSVValue_AtSymbol(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("@test")
	assert.Equal(t, "'@test", result)
}

func TestSanitizeCSVValue_PlusNumeric(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("+123")
	assert.Equal(t, "+123", result)
}

func TestSanitizeCSVValue_PlusFormula(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("+CMD")
	assert.Equal(t, "'+CMD", result)
}

func TestSanitizeCSVValue_NegativeNumber(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("-100.50")
	assert.Equal(t, "-100.50", result)
}

func TestSanitizeCSVValue_MinusFormula(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("-CMD")
	assert.Equal(t, "'-CMD", result)
}

func TestSanitizeCSVValue_MinusOnly(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("-")
	assert.Equal(t, "'-", result)
}

func TestSanitizeCSVValue_NormalString(t *testing.T) {
	t.Parallel()

	result := sanitizeCSVValue("normal value")
	assert.Equal(t, "normal value", result)
}

func TestBuildMatchedCSV_MultipleItems(t *testing.T) {
	t.Parallel()

	items := make([]*entities.MatchedItem, 100)
	for i := 0; i < 100; i++ {
		items[i] = &entities.MatchedItem{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i * 10)),
			Currency:      "USD",
			Date:          time.Now().UTC().Add(time.Duration(i) * time.Hour),
		}
	}

	output, err := BuildMatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 101)
}

func TestBuildUnmatchedCSV_MultipleItems(t *testing.T) {
	t.Parallel()

	items := make([]*entities.UnmatchedItem, 50)
	for i := 0; i < 50; i++ {
		items[i] = &entities.UnmatchedItem{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i * 10)),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Now().UTC().Add(time.Duration(i) * time.Hour),
		}
	}

	output, err := BuildUnmatchedCSV(items)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(string(output)))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 51)
}

var errTestIterator = errors.New("iterator error")

type mockMatchedIterator struct {
	items []*entities.MatchedItem
	idx   int
	err   error
}

func (m *mockMatchedIterator) Next() bool {
	if m.err != nil {
		return false
	}

	m.idx++

	return m.idx <= len(m.items)
}

func (m *mockMatchedIterator) Scan() (*entities.MatchedItem, error) {
	if m.idx <= 0 || m.idx > len(m.items) {
		return nil, errors.New("no current row")
	}

	return m.items[m.idx-1], nil
}

func (m *mockMatchedIterator) Err() error {
	return m.err
}

func (m *mockMatchedIterator) Close() error {
	return nil
}

type mockUnmatchedIterator struct {
	items []*entities.UnmatchedItem
	idx   int
	err   error
}

func (m *mockUnmatchedIterator) Next() bool {
	if m.err != nil {
		return false
	}

	m.idx++

	return m.idx <= len(m.items)
}

func (m *mockUnmatchedIterator) Scan() (*entities.UnmatchedItem, error) {
	if m.idx <= 0 || m.idx > len(m.items) {
		return nil, errors.New("no current row")
	}

	return m.items[m.idx-1], nil
}

func (m *mockUnmatchedIterator) Err() error {
	return m.err
}

func (m *mockUnmatchedIterator) Close() error {
	return nil
}

type mockVarianceIterator struct {
	items []*entities.VarianceReportRow
	idx   int
	err   error
}

func (m *mockVarianceIterator) Next() bool {
	if m.err != nil {
		return false
	}

	m.idx++

	return m.idx <= len(m.items)
}

func (m *mockVarianceIterator) Scan() (*entities.VarianceReportRow, error) {
	if m.idx <= 0 || m.idx > len(m.items) {
		return nil, errors.New("no current row")
	}

	return m.items[m.idx-1], nil
}

func (m *mockVarianceIterator) Err() error {
	return m.err
}

func (m *mockVarianceIterator) Close() error {
	return nil
}

func TestStreamMatchedCSV_Success(t *testing.T) {
	t.Parallel()

	items := []*entities.MatchedItem{
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Date:          time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		},
		{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "EUR",
			Date:          time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
		},
	}

	iter := &mockMatchedIterator{items: items}
	var buf bytes.Buffer

	err := StreamMatchedCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 3)
	assert.Equal(
		t,
		[]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"},
		rows[0],
	)
}

func TestStreamMatchedCSV_EmptyIterator(t *testing.T) {
	t.Parallel()

	iter := &mockMatchedIterator{items: []*entities.MatchedItem{}}
	var buf bytes.Buffer

	err := StreamMatchedCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestStreamMatchedCSV_IteratorError(t *testing.T) {
	t.Parallel()

	iter := &mockMatchedIterator{items: []*entities.MatchedItem{}, err: errTestIterator}
	var buf bytes.Buffer

	err := StreamMatchedCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterating matched rows")
}

func TestStreamUnmatchedCSV_Success(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.New()
	dueAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	items := []*entities.UnmatchedItem{
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(100),
			Currency:      "USD",
			Status:        "UNMATCHED",
			Date:          time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			ExceptionID:   &exceptionID,
			DueAt:         &dueAt,
		},
		{
			TransactionID: uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(200),
			Currency:      "EUR",
			Status:        "PENDING",
			Date:          time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
		},
	}

	iter := &mockUnmatchedIterator{items: items}
	var buf bytes.Buffer

	err := StreamUnmatchedCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 3)
	assert.Equal(
		t,
		[]string{
			"transaction_id",
			"source_id",
			"amount",
			"currency",
			"status",
			"date",
			"exception_id",
			"due_at",
		},
		rows[0],
	)
	assert.Equal(t, exceptionID.String(), rows[1][6])
	assert.Equal(t, "2026-02-01T00:00:00Z", rows[1][7])
	assert.Empty(t, rows[2][6])
	assert.Empty(t, rows[2][7])
}

func TestStreamUnmatchedCSV_EmptyIterator(t *testing.T) {
	t.Parallel()

	iter := &mockUnmatchedIterator{items: []*entities.UnmatchedItem{}}
	var buf bytes.Buffer

	err := StreamUnmatchedCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestStreamUnmatchedCSV_IteratorError(t *testing.T) {
	t.Parallel()

	iter := &mockUnmatchedIterator{items: []*entities.UnmatchedItem{}, err: errTestIterator}
	var buf bytes.Buffer

	err := StreamUnmatchedCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterating unmatched rows")
}

func TestStreamVarianceCSV_Success(t *testing.T) {
	t.Parallel()

	variancePct := decimal.NewFromFloat(10.5)

	rows := []*entities.VarianceReportRow{
		{
			SourceID:      uuid.New(),
			Currency:      "USD",
			FeeType:       "PERCENTAGE",
			TotalExpected: decimal.NewFromInt(1000),
			TotalActual:   decimal.NewFromInt(1100),
			NetVariance:   decimal.NewFromInt(100),
			VariancePct:   &variancePct,
		},
		{
			SourceID:      uuid.New(),
			Currency:      "EUR",
			FeeType:       "FLAT",
			TotalExpected: decimal.NewFromInt(500),
			TotalActual:   decimal.NewFromInt(500),
			NetVariance:   decimal.Zero,
			VariancePct:   nil,
		},
	}

	iter := &mockVarianceIterator{items: rows}
	var buf bytes.Buffer

	err := StreamVarianceCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	csvRows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, csvRows, 3)
	assert.Equal(
		t,
		[]string{
			"source_id",
			"currency",
			"fee_type",
			"total_expected",
			"total_actual",
			"net_variance",
			"variance_pct",
		},
		csvRows[0],
	)
	assert.Equal(t, "10.50", csvRows[1][6])
	assert.Empty(t, csvRows[2][6])
}

func TestStreamVarianceCSV_EmptyIterator(t *testing.T) {
	t.Parallel()

	iter := &mockVarianceIterator{items: []*entities.VarianceReportRow{}}
	var buf bytes.Buffer

	err := StreamVarianceCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 1)
}

func TestStreamVarianceCSV_IteratorError(t *testing.T) {
	t.Parallel()

	iter := &mockVarianceIterator{items: []*entities.VarianceReportRow{}, err: errTestIterator}
	var buf bytes.Buffer

	err := StreamVarianceCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "iterating variance rows")
}

func TestStreamMatchedCSV_LargeDataset(t *testing.T) {
	t.Parallel()

	items := make([]*entities.MatchedItem, 1000)
	for i := 0; i < 1000; i++ {
		items[i] = &entities.MatchedItem{
			TransactionID: uuid.New(),
			MatchGroupID:  uuid.New(),
			SourceID:      uuid.New(),
			Amount:        decimal.NewFromInt(int64(i * 10)),
			Currency:      "USD",
			Date:          time.Now().UTC(),
		}
	}

	iter := &mockMatchedIterator{items: items}
	var buf bytes.Buffer

	err := StreamMatchedCSV(&buf, iter)
	require.NoError(t, err)

	reader := csv.NewReader(strings.NewReader(buf.String()))
	rows, err := reader.ReadAll()
	require.NoError(t, err)
	assert.Len(t, rows, 1001)
}

type mockMatchedScanErrorIterator struct {
	called bool
}

func (m *mockMatchedScanErrorIterator) Next() bool {
	if m.called {
		return false
	}
	m.called = true

	return true
}

func (m *mockMatchedScanErrorIterator) Scan() (*entities.MatchedItem, error) {
	return nil, errors.New("scan error")
}

func (m *mockMatchedScanErrorIterator) Err() error {
	return nil
}

func (m *mockMatchedScanErrorIterator) Close() error {
	return nil
}

func TestStreamMatchedCSV_ScanError(t *testing.T) {
	t.Parallel()

	iter := &mockMatchedScanErrorIterator{}
	var buf bytes.Buffer

	err := StreamMatchedCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning matched row")
}

type mockUnmatchedScanErrorIterator struct {
	called bool
}

func (m *mockUnmatchedScanErrorIterator) Next() bool {
	if m.called {
		return false
	}
	m.called = true

	return true
}

func (m *mockUnmatchedScanErrorIterator) Scan() (*entities.UnmatchedItem, error) {
	return nil, errors.New("scan error")
}

func (m *mockUnmatchedScanErrorIterator) Err() error {
	return nil
}

func (m *mockUnmatchedScanErrorIterator) Close() error {
	return nil
}

func TestStreamUnmatchedCSV_ScanError(t *testing.T) {
	t.Parallel()

	iter := &mockUnmatchedScanErrorIterator{}
	var buf bytes.Buffer

	err := StreamUnmatchedCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning unmatched row")
}

type mockVarianceScanErrorIterator struct {
	called bool
}

func (m *mockVarianceScanErrorIterator) Next() bool {
	if m.called {
		return false
	}
	m.called = true

	return true
}

func (m *mockVarianceScanErrorIterator) Scan() (*entities.VarianceReportRow, error) {
	return nil, errors.New("scan error")
}

func (m *mockVarianceScanErrorIterator) Err() error {
	return nil
}

func (m *mockVarianceScanErrorIterator) Close() error {
	return nil
}

func TestStreamVarianceCSV_ScanError(t *testing.T) {
	t.Parallel()

	iter := &mockVarianceScanErrorIterator{}
	var buf bytes.Buffer

	err := StreamVarianceCSV(&buf, iter)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scanning variance row")
}
