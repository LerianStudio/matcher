// Package entities defines reporting domain types and aggregation logic.
package entities

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// MatcherDashboardMetrics represents the complete dashboard response as per MATCHER_METRICS_SPEC.
type MatcherDashboardMetrics struct {
	Summary    *SummaryMetrics   `json:"summary"`
	Trends     *TrendMetrics     `json:"trends"`
	Breakdowns *BreakdownMetrics `json:"breakdowns"`
	UpdatedAt  time.Time         `json:"updatedAt"`
}

// SummaryMetrics represents the executive summary snapshot of current state.
type SummaryMetrics struct {
	TotalTransactions  int             `json:"totalTransactions"`
	TotalMatches       int             `json:"totalMatches"`
	MatchRate          float64         `json:"matchRate"`
	PendingExceptions  int             `json:"pendingExceptions"`
	CriticalExposure   decimal.Decimal `json:"criticalExposure"`
	OldestExceptionAge float64         `json:"oldestExceptionAgeHours"`
}

// TrendMetrics represents time-series data for charts.
type TrendMetrics struct {
	Dates      []string  `json:"dates"`
	Ingestion  []int     `json:"ingestion"`
	Matches    []int     `json:"matches"`
	Exceptions []int     `json:"exceptions"`
	MatchRates []float64 `json:"matchRates"`
}

// BreakdownMetrics represents categorical aggregations for pie/donut charts.
type BreakdownMetrics struct {
	BySeverity map[string]int   `json:"bySeverity"`
	ByReason   map[string]int   `json:"byReason"`
	ByRule     []RuleMatchCount `json:"byRule"`
	ByAge      []AgeBucket      `json:"byAge"`
}

// RuleMatchCount represents match counts per rule.
type RuleMatchCount struct {
	ID    uuid.UUID `json:"id"`
	Name  string    `json:"name"`
	Count int       `json:"count"`
}

// AgeBucket represents exception counts by age range.
type AgeBucket struct {
	Bucket string `json:"bucket"`
	Value  int    `json:"value"`
}

// DailyTrendPoint represents a single day's trend data.
type DailyTrendPoint struct {
	Date       time.Time
	Ingested   int
	Matched    int
	Exceptions int
	MatchRate  float64
}

// ExceptionReason represents an exception categorization.
type ExceptionReason struct {
	Reason string `json:"reason"`
	Count  int    `json:"count"`
}

// Age bucket constants for SLA monitoring.
const (
	AgeBucketLessThan24h = "<24h"
	AgeBucketOneToThreeD = "1-3d"
	AgeBucketMoreThan3d  = ">3d"
)

// Age thresholds in hours.
const (
	ageThreshold24Hours = 24
	ageThreshold72Hours = 72
)

// NewEmptyTrendMetrics creates an initialized TrendMetrics with empty slices.
func NewEmptyTrendMetrics() *TrendMetrics {
	return &TrendMetrics{
		Dates:      make([]string, 0),
		Ingestion:  make([]int, 0),
		Matches:    make([]int, 0),
		Exceptions: make([]int, 0),
		MatchRates: make([]float64, 0),
	}
}

// NewEmptyBreakdownMetrics creates an initialized BreakdownMetrics with empty maps/slices.
func NewEmptyBreakdownMetrics() *BreakdownMetrics {
	return &BreakdownMetrics{
		BySeverity: make(map[string]int),
		ByReason:   make(map[string]int),
		ByRule:     make([]RuleMatchCount, 0),
		ByAge:      make([]AgeBucket, 0),
	}
}

// BuildTrendMetrics converts daily points into the API response structure.
func BuildTrendMetrics(points []DailyTrendPoint) *TrendMetrics {
	if len(points) == 0 {
		return NewEmptyTrendMetrics()
	}

	metrics := &TrendMetrics{
		Dates:      make([]string, len(points)),
		Ingestion:  make([]int, len(points)),
		Matches:    make([]int, len(points)),
		Exceptions: make([]int, len(points)),
		MatchRates: make([]float64, len(points)),
	}

	for i, p := range points {
		metrics.Dates[i] = p.Date.Format(time.DateOnly)
		metrics.Ingestion[i] = p.Ingested
		metrics.Matches[i] = p.Matched
		metrics.Exceptions[i] = p.Exceptions
		metrics.MatchRates[i] = p.MatchRate
	}

	return metrics
}

// ClassifyExceptionAge classifies an exception age in hours into age buckets.
func ClassifyExceptionAge(ageHours float64) string {
	switch {
	case ageHours < ageThreshold24Hours:
		return AgeBucketLessThan24h
	case ageHours < ageThreshold72Hours:
		return AgeBucketOneToThreeD
	default:
		return AgeBucketMoreThan3d
	}
}

// SourceBreakdown represents per-source reconciliation statistics.
type SourceBreakdown struct {
	SourceID        uuid.UUID       `json:"sourceId"`
	SourceName      string          `json:"sourceName"`
	TotalTxns       int64           `json:"totalTransactions"`
	MatchedTxns     int64           `json:"matchedTransactions"`
	UnmatchedTxns   int64           `json:"unmatchedTransactions"`
	MatchRate       float64         `json:"matchRate"`
	TotalAmount     decimal.Decimal `json:"totalAmount"`
	UnmatchedAmount decimal.Decimal `json:"unmatchedAmount"`
	Currency        string          `json:"currency"`
}

// CashImpactSummary represents the total unreconciled financial exposure.
type CashImpactSummary struct {
	TotalUnmatchedAmount decimal.Decimal    `json:"totalUnmatchedAmount"`
	ByCurrency           []CurrencyExposure `json:"byCurrency"`
	ByAge                []AgeExposure      `json:"byAge"`
}

// CurrencyExposure represents unmatched exposure for a single currency.
type CurrencyExposure struct {
	Currency         string          `json:"currency"`
	Amount           decimal.Decimal `json:"amount"`
	TransactionCount int64           `json:"transactionCount"`
}

// AgeExposure represents unmatched exposure bucketed by age.
type AgeExposure struct {
	Bucket           string          `json:"bucket"`
	Amount           decimal.Decimal `json:"amount"`
	TransactionCount int64           `json:"transactionCount"`
}

// Cash impact age bucket constants.
const (
	CashImpactBucket0To24h  = "0-24h"
	CashImpactBucket1To3d   = "1-3d"
	CashImpactBucket3To7d   = "3-7d"
	CashImpactBucket7To30d  = "7-30d"
	CashImpactBucket30dPlus = "30d+"
)
