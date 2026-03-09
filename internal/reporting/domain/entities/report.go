// Package entities defines reporting domain types and aggregation logic.
package entities

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v4/commons/safe"
)

// ReportFilter defines criteria for querying reports.
type ReportFilter struct {
	ContextID uuid.UUID
	DateFrom  time.Time
	DateTo    time.Time
	SourceID  *uuid.UUID
	Status    *string
	Limit     int
	Cursor    string
	SortOrder string
}

// MatchedItem represents a matched transaction in a report.
type MatchedItem struct {
	TransactionID uuid.UUID
	MatchGroupID  uuid.UUID
	SourceID      uuid.UUID
	Amount        decimal.Decimal
	Currency      string
	Date          time.Time
}

// UnmatchedItem represents an unmatched transaction in a report.
type UnmatchedItem struct {
	TransactionID uuid.UUID
	SourceID      uuid.UUID
	Amount        decimal.Decimal
	Currency      string
	Status        string
	ExceptionID   *uuid.UUID
	DueAt         *time.Time
	Date          time.Time
}

// SummaryReport aggregates matched and unmatched counts and amounts.
type SummaryReport struct {
	MatchedCount    int
	UnmatchedCount  int
	TotalAmount     decimal.Decimal
	MatchedAmount   decimal.Decimal
	UnmatchedAmount decimal.Decimal
}

// BuildSummaryReport computes a summary from matched and unmatched items.
func BuildSummaryReport(matched []*MatchedItem, unmatched []*UnmatchedItem) SummaryReport {
	summary := SummaryReport{
		TotalAmount:     decimal.Zero,
		MatchedAmount:   decimal.Zero,
		UnmatchedAmount: decimal.Zero,
	}

	for _, item := range matched {
		if item == nil {
			continue
		}

		summary.MatchedCount++
		summary.MatchedAmount = summary.MatchedAmount.Add(item.Amount)
	}

	for _, item := range unmatched {
		if item == nil {
			continue
		}

		summary.UnmatchedCount++
		summary.UnmatchedAmount = summary.UnmatchedAmount.Add(item.Amount)
	}

	summary.TotalAmount = summary.MatchedAmount.Add(summary.UnmatchedAmount)

	return summary
}

// VarianceReportFilter defines criteria for querying variance reports.
type VarianceReportFilter struct {
	ContextID uuid.UUID
	DateFrom  time.Time
	DateTo    time.Time
	SourceID  *uuid.UUID
	Limit     int
	Cursor    string
	SortOrder string
}

// VarianceReportRow represents an aggregated variance grouped by source, currency, and fee type.
type VarianceReportRow struct {
	SourceID      uuid.UUID
	Currency      string
	FeeType       string
	TotalExpected decimal.Decimal
	TotalActual   decimal.Decimal
	NetVariance   decimal.Decimal
	VariancePct   *decimal.Decimal
}

// BuildVarianceRow creates a VarianceReportRow with calculated variance percentage.
func BuildVarianceRow(
	sourceID uuid.UUID,
	currency, feeType string,
	totalExpected, totalActual, netVariance decimal.Decimal,
) *VarianceReportRow {
	row := &VarianceReportRow{
		SourceID:      sourceID,
		Currency:      currency,
		FeeType:       feeType,
		TotalExpected: totalExpected,
		TotalActual:   totalActual,
		NetVariance:   netVariance,
	}

	if !totalExpected.IsZero() {
		pct := safe.PercentageOrZero(netVariance, totalExpected)
		row.VariancePct = &pct
	}

	return row
}
