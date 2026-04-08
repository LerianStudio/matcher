// Package exports provides report export builders for CSV and PDF formats.
package exports

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
	"github.com/LerianStudio/matcher/internal/reporting/domain/repositories"
	"github.com/LerianStudio/matcher/internal/shared/sanitize"
)

// ErrSummaryRequired is returned when a summary is required but not provided.
var ErrSummaryRequired = errors.New("summary is required")

const variancePctDecimalPlaces = 2

func errWriteCSVHeader(err error) error {
	return fmt.Errorf("write csv header: %w", err)
}

func errWriteCSVRow(err error) error {
	return fmt.Errorf("write csv row: %w", err)
}

func errFlushCSV(err error) error {
	return fmt.Errorf("flush csv: %w", err)
}

// BuildMatchedCSV generates a CSV file from matched items, sorted by date then transaction ID.
func BuildMatchedCSV(items []*entities.MatchedItem) ([]byte, error) {
	sorted := make([]*entities.MatchedItem, 0, len(items))

	for _, item := range items {
		if item != nil {
			sorted = append(sorted, item)
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Date.Equal(sorted[j].Date) {
			return sorted[i].TransactionID.String() < sorted[j].TransactionID.String()
		}

		return sorted[i].Date.Before(sorted[j].Date)
	})

	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)

	if err := writer.Write([]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"}); err != nil {
		return nil, errWriteCSVHeader(err)
	}

	for _, item := range sorted {
		if err := writer.Write([]string{
			sanitizeCSVValue(item.TransactionID.String()),
			sanitizeCSVValue(item.MatchGroupID.String()),
			sanitizeCSVValue(item.SourceID.String()),
			sanitizeCSVValue(item.Amount.String()),
			sanitizeCSVValue(item.Currency),
			sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
		}); err != nil {
			return nil, errWriteCSVRow(err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return nil, errFlushCSV(err)
	}

	return buffer.Bytes(), nil
}

// BuildUnmatchedCSV generates a CSV file from unmatched items, sorted by date then transaction ID.
func BuildUnmatchedCSV(items []*entities.UnmatchedItem) ([]byte, error) {
	sorted := make([]*entities.UnmatchedItem, 0, len(items))

	for _, item := range items {
		if item != nil {
			sorted = append(sorted, item)
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Date.Equal(sorted[j].Date) {
			return sorted[i].TransactionID.String() < sorted[j].TransactionID.String()
		}

		return sorted[i].Date.Before(sorted[j].Date)
	})

	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)

	if err := writer.Write([]string{"transaction_id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}); err != nil {
		return nil, errWriteCSVHeader(err)
	}

	for _, item := range sorted {
		exceptionID := ""
		if item.ExceptionID != nil {
			exceptionID = item.ExceptionID.String()
		}

		dueAt := ""
		if item.DueAt != nil {
			dueAt = item.DueAt.UTC().Format(time.RFC3339)
		}

		if err := writer.Write([]string{
			sanitizeCSVValue(item.TransactionID.String()),
			sanitizeCSVValue(item.SourceID.String()),
			sanitizeCSVValue(item.Amount.String()),
			sanitizeCSVValue(item.Currency),
			sanitizeCSVValue(item.Status),
			sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
			sanitizeCSVValue(exceptionID),
			sanitizeCSVValue(dueAt),
		}); err != nil {
			return nil, errWriteCSVRow(err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return nil, errFlushCSV(err)
	}

	return buffer.Bytes(), nil
}

// BuildSummaryCSV generates a CSV file from a summary report.
func BuildSummaryCSV(summary *entities.SummaryReport) ([]byte, error) {
	if summary == nil {
		return nil, ErrSummaryRequired
	}

	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)

	if err := writer.Write([]string{"matched_count", "unmatched_count", "total_amount", "matched_amount", "unmatched_amount"}); err != nil {
		return nil, errWriteCSVHeader(err)
	}

	if err := writer.Write([]string{
		sanitizeCSVValue(strconv.Itoa(summary.MatchedCount)),
		sanitizeCSVValue(strconv.Itoa(summary.UnmatchedCount)),
		sanitizeCSVValue(summary.TotalAmount.String()),
		sanitizeCSVValue(summary.MatchedAmount.String()),
		sanitizeCSVValue(summary.UnmatchedAmount.String()),
	}); err != nil {
		return nil, errWriteCSVRow(err)
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return nil, errFlushCSV(err)
	}

	return buffer.Bytes(), nil
}

// BuildVarianceCSV generates a CSV file from variance report rows, sorted by source, currency, and fee schedule.
func BuildVarianceCSV(rows []*entities.VarianceReportRow) ([]byte, error) {
	sorted := make([]*entities.VarianceReportRow, 0, len(rows))

	for _, row := range rows {
		if row != nil {
			sorted = append(sorted, row)
		}
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].SourceID != sorted[j].SourceID {
			return sorted[i].SourceID.String() < sorted[j].SourceID.String()
		}

		if sorted[i].Currency != sorted[j].Currency {
			return sorted[i].Currency < sorted[j].Currency
		}

		return sorted[i].FeeScheduleID.String() < sorted[j].FeeScheduleID.String()
	})

	buffer := &bytes.Buffer{}
	writer := csv.NewWriter(buffer)

	if err := writer.Write([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance", "variance_pct"}); err != nil {
		return nil, errWriteCSVHeader(err)
	}

	for _, row := range sorted {
		variancePct := ""
		if row.VariancePct != nil {
			variancePct = row.VariancePct.StringFixed(variancePctDecimalPlaces)
		}

		if err := writer.Write([]string{
			sanitizeCSVValue(row.SourceID.String()),
			sanitizeCSVValue(row.Currency),
			sanitizeCSVValue(row.FeeScheduleID.String()),
			sanitizeCSVValue(row.FeeScheduleName),
			sanitizeCSVValue(row.TotalExpected.String()),
			sanitizeCSVValue(row.TotalActual.String()),
			sanitizeCSVValue(row.NetVariance.String()),
			sanitizeCSVValue(variancePct),
		}); err != nil {
			return nil, errWriteCSVRow(err)
		}
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return nil, errFlushCSV(err)
	}

	return buffer.Bytes(), nil
}

// sanitizeCSVValue delegates to the shared sanitize package to prevent
// CSV/spreadsheet formula injection. See sanitize.SanitizeFormulaInjection for
// full documentation.
func sanitizeCSVValue(value string) string {
	return sanitize.SanitizeFormulaInjection(value)
}

// StreamMatchedCSV writes matched items as CSV to the writer, streaming row by row.
// This avoids loading all data into memory for large exports.
func StreamMatchedCSV(w io.Writer, iter repositories.MatchedRowIterator) error {
	writer := csv.NewWriter(w)

	if err := writer.Write([]string{"transaction_id", "match_group_id", "source_id", "amount", "currency", "date"}); err != nil {
		return errWriteCSVHeader(err)
	}

	for iter.Next() {
		item, err := iter.Scan()
		if err != nil {
			return fmt.Errorf("scanning matched row: %w", err)
		}

		if err := writer.Write([]string{
			sanitizeCSVValue(item.TransactionID.String()),
			sanitizeCSVValue(item.MatchGroupID.String()),
			sanitizeCSVValue(item.SourceID.String()),
			sanitizeCSVValue(item.Amount.String()),
			sanitizeCSVValue(item.Currency),
			sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
		}); err != nil {
			return errWriteCSVRow(err)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterating matched rows: %w", err)
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return errFlushCSV(err)
	}

	return nil
}

// StreamUnmatchedCSV writes unmatched items as CSV to the writer, streaming row by row.
func StreamUnmatchedCSV(w io.Writer, iter repositories.UnmatchedRowIterator) error {
	writer := csv.NewWriter(w)

	if err := writer.Write([]string{"transaction_id", "source_id", "amount", "currency", "status", "date", "exception_id", "due_at"}); err != nil {
		return errWriteCSVHeader(err)
	}

	for iter.Next() {
		item, err := iter.Scan()
		if err != nil {
			return fmt.Errorf("scanning unmatched row: %w", err)
		}

		exceptionID := ""
		if item.ExceptionID != nil {
			exceptionID = item.ExceptionID.String()
		}

		dueAt := ""
		if item.DueAt != nil {
			dueAt = item.DueAt.UTC().Format(time.RFC3339)
		}

		if err := writer.Write([]string{
			sanitizeCSVValue(item.TransactionID.String()),
			sanitizeCSVValue(item.SourceID.String()),
			sanitizeCSVValue(item.Amount.String()),
			sanitizeCSVValue(item.Currency),
			sanitizeCSVValue(item.Status),
			sanitizeCSVValue(item.Date.UTC().Format(time.RFC3339)),
			sanitizeCSVValue(exceptionID),
			sanitizeCSVValue(dueAt),
		}); err != nil {
			return errWriteCSVRow(err)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterating unmatched rows: %w", err)
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return errFlushCSV(err)
	}

	return nil
}

// StreamVarianceCSV writes variance rows as CSV to the writer, streaming row by row.
func StreamVarianceCSV(w io.Writer, iter repositories.VarianceRowIterator) error {
	writer := csv.NewWriter(w)

	if err := writer.Write([]string{"source_id", "currency", "fee_schedule_id", "fee_schedule_name", "total_expected", "total_actual", "net_variance", "variance_pct"}); err != nil {
		return errWriteCSVHeader(err)
	}

	for iter.Next() {
		row, err := iter.Scan()
		if err != nil {
			return fmt.Errorf("scanning variance row: %w", err)
		}

		variancePct := ""
		if row.VariancePct != nil {
			variancePct = row.VariancePct.StringFixed(variancePctDecimalPlaces)
		}

		if err := writer.Write([]string{
			sanitizeCSVValue(row.SourceID.String()),
			sanitizeCSVValue(row.Currency),
			sanitizeCSVValue(row.FeeScheduleID.String()),
			sanitizeCSVValue(row.FeeScheduleName),
			sanitizeCSVValue(row.TotalExpected.String()),
			sanitizeCSVValue(row.TotalActual.String()),
			sanitizeCSVValue(row.NetVariance.String()),
			sanitizeCSVValue(variancePct),
		}); err != nil {
			return errWriteCSVRow(err)
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterating variance rows: %w", err)
	}

	writer.Flush()

	if err := writer.Error(); err != nil {
		return errFlushCSV(err)
	}

	return nil
}
