// Package exports provides report export builders for CSV and PDF formats.
package exports

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"time"

	"codeberg.org/go-pdf/fpdf"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

const (
	pdfTitleHeight       = 10
	pdfSummaryRowHeight  = 8
	pdfHeaderRowHeight   = 7
	pdfDataRowHeight     = 6
	pdfColumnWidthNarrow = 25.0
	pdfColumnWidthMedium = 40.0
	pdfColumnWidthWide   = 55.0
	pdfCellMaxLength     = 40
	pdfCellTruncLength   = 37
	pdfFontSizeSmall     = 7
	pdfFontSizeMedium    = 12
)

func errWritePDF(err error) error {
	return fmt.Errorf("write pdf: %w", err)
}

// BuildMatchedPDF generates a PDF file from matched items, sorted by date then transaction ID.
func BuildMatchedPDF(items []*entities.MatchedItem) ([]byte, error) {
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

	rows := make([][]string, 0, len(sorted))

	for _, item := range sorted {
		rows = append(rows, []string{
			item.TransactionID.String(),
			item.MatchGroupID.String(),
			item.SourceID.String(),
			item.Amount.String(),
			item.Currency,
			item.Date.UTC().Format(time.RFC3339),
		})
	}

	return buildItemsPDF(
		"Matched Report",
		[]string{"Transaction", "Match Group", "Source", "Amount", "Currency", "Date"},
		rows,
	)
}

// BuildUnmatchedPDF generates a PDF file from unmatched items, sorted by date then transaction ID.
func BuildUnmatchedPDF(items []*entities.UnmatchedItem) ([]byte, error) {
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

	rows := make([][]string, 0, len(sorted))

	for _, item := range sorted {
		exceptionID := ""
		if item.ExceptionID != nil {
			exceptionID = item.ExceptionID.String()
		}

		dueAt := ""
		if item.DueAt != nil {
			dueAt = item.DueAt.UTC().Format(time.RFC3339)
		}

		rows = append(rows, []string{
			item.TransactionID.String(),
			item.SourceID.String(),
			item.Amount.String(),
			item.Currency,
			item.Status,
			item.Date.UTC().Format(time.RFC3339),
			exceptionID,
			dueAt,
		})
	}

	return buildItemsPDF(
		"Unmatched Report",
		[]string{
			"Transaction",
			"Source",
			"Amount",
			"Currency",
			"Status",
			"Date",
			"Exception",
			"Due At",
		},
		rows,
	)
}

// BuildSummaryPDF generates a PDF file from a summary report.
func BuildSummaryPDF(summary *entities.SummaryReport) ([]byte, error) {
	if summary == nil {
		return nil, ErrSummaryRequired
	}

	pdf := fpdf.New("P", "mm", "A4", "")
	pdf.SetFont("Arial", "", pdfFontSizeMedium)
	pdf.AddPage()
	pdf.CellFormat(0, pdfTitleHeight, "Summary Report", "", 1, "L", false, 0, "")
	pdf.CellFormat(
		0,
		pdfSummaryRowHeight,
		"Matched Count: "+strconv.Itoa(summary.MatchedCount),
		"",
		1,
		"L",
		false,
		0,
		"",
	)
	pdf.CellFormat(
		0,
		pdfSummaryRowHeight,
		"Unmatched Count: "+strconv.Itoa(summary.UnmatchedCount),
		"",
		1,
		"L",
		false,
		0,
		"",
	)
	pdf.CellFormat(
		0,
		pdfSummaryRowHeight,
		"Matched Amount: "+summary.MatchedAmount.String(),
		"",
		1,
		"L",
		false,
		0,
		"",
	)
	pdf.CellFormat(
		0,
		pdfSummaryRowHeight,
		"Unmatched Amount: "+summary.UnmatchedAmount.String(),
		"",
		1,
		"L",
		false,
		0,
		"",
	)
	pdf.CellFormat(
		0,
		pdfSummaryRowHeight,
		"Total Amount: "+summary.TotalAmount.String(),
		"",
		1,
		"L",
		false,
		0,
		"",
	)

	var buffer bytes.Buffer

	if err := pdf.Output(&buffer); err != nil {
		return nil, errWritePDF(err)
	}

	return buffer.Bytes(), nil
}

func buildItemsPDF(title string, headers []string, rows [][]string) ([]byte, error) {
	pdf := fpdf.New("L", "mm", "A3", "")
	pdf.SetFont("Arial", "", pdfFontSizeSmall)
	pdf.AddPage()
	pdf.CellFormat(0, pdfTitleHeight, title, "", 1, "L", false, 0, "")

	for _, header := range headers {
		width := getColumnWidth(header)
		pdf.CellFormat(width, pdfHeaderRowHeight, header, "1", 0, "L", false, 0, "")
	}

	pdf.Ln(-1)

	for _, row := range rows {
		for i, cell := range row {
			width := getColumnWidth(headers[i])
			displayCell := cell

			if len(displayCell) > pdfCellMaxLength {
				displayCell = displayCell[:pdfCellTruncLength] + "..."
			}

			pdf.CellFormat(width, pdfDataRowHeight, displayCell, "1", 0, "L", false, 0, "")
		}

		pdf.Ln(-1)
	}

	var buffer bytes.Buffer

	if err := pdf.Output(&buffer); err != nil {
		return nil, errWritePDF(err)
	}

	return buffer.Bytes(), nil
}

func getColumnWidth(header string) float64 {
	switch header {
	case "Transaction", "Match Group", "Source", "Exception":
		return pdfColumnWidthWide
	case "Amount", "Expected", "Actual", "Variance", "Date", "Due At":
		return pdfColumnWidthMedium
	default:
		return pdfColumnWidthNarrow
	}
}

// BuildVariancePDF generates a PDF file from variance report rows, sorted by source, currency, fee type.
func BuildVariancePDF(rows []*entities.VarianceReportRow) ([]byte, error) {
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

		return sorted[i].FeeType < sorted[j].FeeType
	})

	pdfRows := make([][]string, 0, len(sorted))

	for _, row := range sorted {
		variancePct := ""
		if row.VariancePct != nil {
			variancePct = row.VariancePct.StringFixed(variancePctDecimalPlaces) + "%"
		}

		pdfRows = append(pdfRows, []string{
			row.SourceID.String(),
			row.Currency,
			row.FeeType,
			row.TotalExpected.String(),
			row.TotalActual.String(),
			row.NetVariance.String(),
			variancePct,
		})
	}

	return buildItemsPDF(
		"Variance Report",
		[]string{"Source", "Currency", "Fee Type", "Expected", "Actual", "Variance", "Pct"},
		pdfRows,
	)
}
