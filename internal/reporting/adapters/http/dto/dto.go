// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package dto provides data transfer objects for the reporting HTTP API.
package dto

import (
	"time"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/lib-commons/v5/commons/pointers"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

// ExportCountResponse represents the row count for an export query.
// Used by the frontend to decide between sync download (<1000) and async job (>=1000).
type ExportCountResponse struct {
	Count int64 `json:"count" example:"4250"`
}

// VolumeStatsResponse represents volume statistics in API responses.
type VolumeStatsResponse struct {
	TotalTransactions   int    `json:"totalTransactions"   example:"12500"`
	MatchedTransactions int    `json:"matchedTransactions" example:"11800"`
	UnmatchedCount      int    `json:"unmatchedCount"      example:"700"`
	TotalAmount         string `json:"totalAmount"         example:"1250000.00"`
	MatchedAmount       string `json:"matchedAmount"       example:"1180000.00"`
	UnmatchedAmount     string `json:"unmatchedAmount"     example:"70000.00"`
	PeriodStart         string `json:"periodStart"         example:"2025-01-01T00:00:00Z" format:"date-time"`
	PeriodEnd           string `json:"periodEnd"           example:"2025-01-31T23:59:59Z" format:"date-time"`
}

// MatchRateStatsResponse represents match rate statistics in API responses.
type MatchRateStatsResponse struct {
	MatchRate       float64 `json:"matchRate"       example:"94.4"`
	MatchRateAmount float64 `json:"matchRateAmount" example:"94.4"`
	TotalCount      int     `json:"totalCount"      example:"12500"`
	MatchedCount    int     `json:"matchedCount"    example:"11800"`
	UnmatchedCount  int     `json:"unmatchedCount"  example:"700"`
}

// SLAStatsResponse represents SLA statistics in API responses.
type SLAStatsResponse struct {
	TotalExceptions     int     `json:"totalExceptions"     example:"42"`
	ResolvedOnTime      int     `json:"resolvedOnTime"      example:"35"`
	ResolvedLate        int     `json:"resolvedLate"        example:"3"`
	PendingWithinSLA    int     `json:"pendingWithinSLA"    example:"2"`
	PendingOverdue      int     `json:"pendingOverdue"      example:"2"`
	SLAComplianceRate   float64 `json:"slaComplianceRate"   example:"83.3"`
	AverageResolutionMs int64   `json:"averageResolutionMs" example:"3600000"`
}

// DashboardAggregatesResponse represents all dashboard aggregates in API responses.
type DashboardAggregatesResponse struct {
	Volume    *VolumeStatsResponse    `json:"volume"`
	MatchRate *MatchRateStatsResponse `json:"matchRate"`
	SLA       *SLAStatsResponse       `json:"sla"`
	UpdatedAt string                  `json:"updatedAt" example:"2025-01-15T10:30:00Z" format:"date-time"`
}

// VolumeStatsToResponse converts VolumeStats entity to response DTO.
// Returns a zero-value response when stats is nil to ensure stable JSON structure.
func VolumeStatsToResponse(stats *entities.VolumeStats) *VolumeStatsResponse {
	if stats == nil {
		return &VolumeStatsResponse{}
	}

	return &VolumeStatsResponse{
		TotalTransactions:   stats.TotalTransactions,
		MatchedTransactions: stats.MatchedTransactions,
		UnmatchedCount:      stats.UnmatchedCount,
		TotalAmount:         stats.TotalAmount.String(),
		MatchedAmount:       stats.MatchedAmount.String(),
		UnmatchedAmount:     stats.UnmatchedAmount.String(),
		PeriodStart:         stats.PeriodStart.Format(time.RFC3339),
		PeriodEnd:           stats.PeriodEnd.Format(time.RFC3339),
	}
}

// MatchRateStatsToResponse converts MatchRateStats entity to response DTO.
// Returns a zero-value response when stats is nil to ensure stable JSON structure.
func MatchRateStatsToResponse(stats *entities.MatchRateStats) *MatchRateStatsResponse {
	if stats == nil {
		return &MatchRateStatsResponse{}
	}

	return &MatchRateStatsResponse{
		MatchRate:       stats.MatchRate,
		MatchRateAmount: stats.MatchRateAmount,
		TotalCount:      stats.TotalCount,
		MatchedCount:    stats.MatchedCount,
		UnmatchedCount:  stats.UnmatchedCount,
	}
}

// SLAStatsToResponse converts SLAStats entity to response DTO.
// Returns a zero-value response when stats is nil to ensure stable JSON structure.
func SLAStatsToResponse(stats *entities.SLAStats) *SLAStatsResponse {
	if stats == nil {
		return &SLAStatsResponse{}
	}

	return &SLAStatsResponse{
		TotalExceptions:     stats.TotalExceptions,
		ResolvedOnTime:      stats.ResolvedOnTime,
		ResolvedLate:        stats.ResolvedLate,
		PendingWithinSLA:    stats.PendingWithinSLA,
		PendingOverdue:      stats.PendingOverdue,
		SLAComplianceRate:   stats.SLAComplianceRate,
		AverageResolutionMs: stats.AverageResolutionMs,
	}
}

// DashboardAggregatesToResponse converts DashboardAggregates entity to response DTO.
// Returns a zero-value response when aggregates is nil to ensure stable JSON structure.
func DashboardAggregatesToResponse(
	aggregates *entities.DashboardAggregates,
) *DashboardAggregatesResponse {
	if aggregates == nil {
		return &DashboardAggregatesResponse{
			Volume:    &VolumeStatsResponse{},
			MatchRate: &MatchRateStatsResponse{},
			SLA:       &SLAStatsResponse{},
			UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		}
	}

	return &DashboardAggregatesResponse{
		Volume:    VolumeStatsToResponse(aggregates.Volume),
		MatchRate: MatchRateStatsToResponse(aggregates.MatchRate),
		SLA:       SLAStatsToResponse(aggregates.SLA),
		UpdatedAt: aggregates.UpdatedAt.Format(time.RFC3339),
	}
}

// MatcherDashboardMetricsResponse represents the comprehensive dashboard metrics response.
type MatcherDashboardMetricsResponse struct {
	Summary    *SummaryMetricsResponse   `json:"summary"`
	Trends     *TrendMetricsResponse     `json:"trends"`
	Breakdowns *BreakdownMetricsResponse `json:"breakdowns"`
	UpdatedAt  string                    `json:"updatedAt" example:"2025-01-15T10:30:00Z" format:"date-time"`
}

// SummaryMetricsResponse represents executive summary metrics.
type SummaryMetricsResponse struct {
	TotalTransactions  int     `json:"totalTransactions"       example:"12500"`
	TotalMatches       int     `json:"totalMatches"            example:"11800"`
	MatchRate          float64 `json:"matchRate"               example:"94.4"`
	PendingExceptions  int     `json:"pendingExceptions"       example:"4"`
	CriticalExposure   string  `json:"criticalExposure"        example:"15000.00"`
	OldestExceptionAge float64 `json:"oldestExceptionAgeHours" example:"72.5"`
}

// TrendMetricsResponse represents time-series trend data.
type TrendMetricsResponse struct {
	Dates      []string  `json:"dates"      validate:"omitempty,max=366" maxItems:"366"`
	Ingestion  []int     `json:"ingestion"  validate:"omitempty,max=366" maxItems:"366"`
	Matches    []int     `json:"matches"    validate:"omitempty,max=366" maxItems:"366"`
	Exceptions []int     `json:"exceptions" validate:"omitempty,max=366" maxItems:"366"`
	MatchRates []float64 `json:"matchRates" validate:"omitempty,max=366" maxItems:"366"`
}

// BreakdownMetricsResponse represents categorical aggregations.
type BreakdownMetricsResponse struct {
	BySeverity map[string]int      `json:"bySeverity"`
	ByReason   map[string]int      `json:"byReason"`
	ByRule     []RuleMatchCountDTO `json:"byRule" validate:"omitempty,max=100" maxItems:"100"`
	ByAge      []AgeBucketDTO      `json:"byAge"  validate:"omitempty,max=20"  maxItems:"20"`
}

// RuleMatchCountDTO represents match count per rule in API response.
type RuleMatchCountDTO struct {
	ID    string `json:"id"    example:"550e8400-e29b-41d4-a716-446655440000"`
	Name  string `json:"name"  example:"Exact Amount Match"`
	Count int    `json:"count" example:"1250"`
}

// AgeBucketDTO represents age bucket distribution in API response.
type AgeBucketDTO struct {
	Bucket string `json:"bucket" example:"0-24h"`
	Value  int    `json:"value"  example:"15"`
}

// MatcherDashboardMetricsToResponse converts domain entity to response DTO.
// Returns a zero-value response when metrics is nil to ensure stable JSON structure.
func MatcherDashboardMetricsToResponse(
	metrics *entities.MatcherDashboardMetrics,
) *MatcherDashboardMetricsResponse {
	if metrics == nil {
		return &MatcherDashboardMetricsResponse{
			Summary:    summaryMetricsToResponse(nil),
			Trends:     trendMetricsToResponse(nil),
			Breakdowns: breakdownMetricsToResponse(nil),
			UpdatedAt:  time.Now().UTC().Format(time.RFC3339),
		}
	}

	return &MatcherDashboardMetricsResponse{
		Summary:    summaryMetricsToResponse(metrics.Summary),
		Trends:     trendMetricsToResponse(metrics.Trends),
		Breakdowns: breakdownMetricsToResponse(metrics.Breakdowns),
		UpdatedAt:  metrics.UpdatedAt.Format(time.RFC3339),
	}
}

func summaryMetricsToResponse(summary *entities.SummaryMetrics) *SummaryMetricsResponse {
	if summary == nil {
		return &SummaryMetricsResponse{}
	}

	return &SummaryMetricsResponse{
		TotalTransactions:  summary.TotalTransactions,
		TotalMatches:       summary.TotalMatches,
		MatchRate:          summary.MatchRate,
		PendingExceptions:  summary.PendingExceptions,
		CriticalExposure:   summary.CriticalExposure.String(),
		OldestExceptionAge: summary.OldestExceptionAge,
	}
}

func trendMetricsToResponse(trends *entities.TrendMetrics) *TrendMetricsResponse {
	if trends == nil {
		return &TrendMetricsResponse{
			Dates:      []string{},
			Ingestion:  []int{},
			Matches:    []int{},
			Exceptions: []int{},
			MatchRates: []float64{},
		}
	}

	dates := trends.Dates
	if dates == nil {
		dates = []string{}
	}

	ingestion := trends.Ingestion
	if ingestion == nil {
		ingestion = []int{}
	}

	matches := trends.Matches
	if matches == nil {
		matches = []int{}
	}

	exceptions := trends.Exceptions
	if exceptions == nil {
		exceptions = []int{}
	}

	matchRates := trends.MatchRates
	if matchRates == nil {
		matchRates = []float64{}
	}

	return &TrendMetricsResponse{
		Dates:      dates,
		Ingestion:  ingestion,
		Matches:    matches,
		Exceptions: exceptions,
		MatchRates: matchRates,
	}
}

// SourceBreakdownResponse represents per-source reconciliation statistics in API responses.
type SourceBreakdownResponse struct {
	SourceID        string  `json:"sourceId"                example:"550e8400-e29b-41d4-a716-446655440000"`
	SourceName      string  `json:"sourceName"              example:"Primary Bank Account"`
	TotalTxns       int64   `json:"totalTransactions"       example:"5000"`
	MatchedTxns     int64   `json:"matchedTransactions"     example:"4750"`
	UnmatchedTxns   int64   `json:"unmatchedTransactions"   example:"250"`
	MatchRate       float64 `json:"matchRate"               example:"95.0"`
	TotalAmount     string  `json:"totalAmount"             example:"500000.00"`
	UnmatchedAmount string  `json:"unmatchedAmount"         example:"25000.00"`
	Currency        string  `json:"currency"                example:"USD"`
}

// SourceBreakdownListResponse wraps the list of source breakdowns.
type SourceBreakdownListResponse struct {
	Sources []SourceBreakdownResponse `json:"sources"`
}

// SourceBreakdownToResponse converts domain entities to response DTOs.
func SourceBreakdownToResponse(breakdowns []entities.SourceBreakdown) *SourceBreakdownListResponse {
	if breakdowns == nil {
		return &SourceBreakdownListResponse{Sources: make([]SourceBreakdownResponse, 0)}
	}

	sources := make([]SourceBreakdownResponse, len(breakdowns))

	for i, sb := range breakdowns {
		sources[i] = SourceBreakdownResponse{
			SourceID:        sb.SourceID.String(),
			SourceName:      sb.SourceName,
			TotalTxns:       sb.TotalTxns,
			MatchedTxns:     sb.MatchedTxns,
			UnmatchedTxns:   sb.UnmatchedTxns,
			MatchRate:       sb.MatchRate,
			TotalAmount:     sb.TotalAmount.String(),
			UnmatchedAmount: sb.UnmatchedAmount.String(),
			Currency:        sb.Currency,
		}
	}

	return &SourceBreakdownListResponse{Sources: sources}
}

// CurrencyExposureResponse represents currency exposure in API responses.
type CurrencyExposureResponse struct {
	Currency         string `json:"currency"         example:"USD"`
	Amount           string `json:"amount"           example:"25000.00"`
	TransactionCount int64  `json:"transactionCount" example:"250"`
}

// AgeExposureResponse represents age-bucketed exposure in API responses.
type AgeExposureResponse struct {
	Bucket           string `json:"bucket"           example:"0-24h"`
	Amount           string `json:"amount"           example:"10000.00"`
	TransactionCount int64  `json:"transactionCount" example:"120"`
}

// CashImpactSummaryResponse represents cash impact summary in API responses.
type CashImpactSummaryResponse struct {
	TotalUnmatchedAmount string                     `json:"totalUnmatchedAmount" example:"70000.00"`
	ByCurrency           []CurrencyExposureResponse `json:"byCurrency"`
	ByAge                []AgeExposureResponse      `json:"byAge"`
}

// CashImpactSummaryToResponse converts domain entity to response DTO.
func CashImpactSummaryToResponse(summary *entities.CashImpactSummary) *CashImpactSummaryResponse {
	if summary == nil {
		return &CashImpactSummaryResponse{
			TotalUnmatchedAmount: "0",
			ByCurrency:           make([]CurrencyExposureResponse, 0),
			ByAge:                make([]AgeExposureResponse, 0),
		}
	}

	byCurrency := make([]CurrencyExposureResponse, len(summary.ByCurrency))
	for i, ce := range summary.ByCurrency {
		byCurrency[i] = CurrencyExposureResponse{
			Currency:         ce.Currency,
			Amount:           ce.Amount.String(),
			TransactionCount: ce.TransactionCount,
		}
	}

	byAge := make([]AgeExposureResponse, len(summary.ByAge))
	for i, ae := range summary.ByAge {
		byAge[i] = AgeExposureResponse{
			Bucket:           ae.Bucket,
			Amount:           ae.Amount.String(),
			TransactionCount: ae.TransactionCount,
		}
	}

	return &CashImpactSummaryResponse{
		TotalUnmatchedAmount: summary.TotalUnmatchedAmount.String(),
		ByCurrency:           byCurrency,
		ByAge:                byAge,
	}
}

// MatchedItemResponse represents a matched transaction in a report.
type MatchedItemResponse struct {
	TransactionID string `json:"transactionId" example:"550e8400-e29b-41d4-a716-446655440000"`
	MatchGroupID  string `json:"matchGroupId"  example:"550e8400-e29b-41d4-a716-446655440001"`
	SourceID      string `json:"sourceId"      example:"550e8400-e29b-41d4-a716-446655440002"`
	Amount        string `json:"amount"        example:"1250.50"`
	Currency      string `json:"currency"      example:"USD"`
	Date          string `json:"date"          example:"2025-01-15T10:30:00Z" format:"date-time"`
}

// UnmatchedItemResponse represents an unmatched transaction in a report.
type UnmatchedItemResponse struct {
	TransactionID string  `json:"transactionId"          example:"550e8400-e29b-41d4-a716-446655440000"`
	SourceID      string  `json:"sourceId"               example:"550e8400-e29b-41d4-a716-446655440002"`
	Amount        string  `json:"amount"                 example:"1250.50"`
	Currency      string  `json:"currency"               example:"USD"`
	Status        string  `json:"status"                 example:"PENDING"`
	ExceptionID   *string `json:"exceptionId,omitempty"  example:"550e8400-e29b-41d4-a716-446655440003"`
	DueAt         *string `json:"dueAt,omitempty"        example:"2025-02-15T10:30:00Z" format:"date-time"`
	Date          string  `json:"date"                   example:"2025-01-15T10:30:00Z" format:"date-time"`
}

// SummaryReportResponse represents an aggregated summary.
type SummaryReportResponse struct {
	MatchedCount    int    `json:"matchedCount"    example:"11800"`
	UnmatchedCount  int    `json:"unmatchedCount"  example:"700"`
	TotalAmount     string `json:"totalAmount"     example:"1250000.00"`
	MatchedAmount   string `json:"matchedAmount"   example:"1180000.00"`
	UnmatchedAmount string `json:"unmatchedAmount" example:"70000.00"`
}

// VarianceReportRowResponse represents a row in the variance report.
type VarianceReportRowResponse struct {
	SourceID        string  `json:"sourceId"              example:"550e8400-e29b-41d4-a716-446655440000"`
	Currency        string  `json:"currency"              example:"USD"`
	FeeScheduleID   string  `json:"feeScheduleId"         example:"550e8400-e29b-41d4-a716-446655440001"`
	FeeScheduleName string  `json:"feeScheduleName"       example:"INTERCHANGE"`
	TotalExpected   string  `json:"totalExpected"         example:"5000.00"`
	TotalActual     string  `json:"totalActual"           example:"4800.00"`
	NetVariance     string  `json:"netVariance"           example:"-200.00"`
	VariancePct     *string `json:"variancePct,omitempty" example:"-4.00"`
}

// ListMatchedReportResponse wraps a paginated list of matched items.
type ListMatchedReportResponse struct {
	Items      []MatchedItemResponse    `json:"items"`
	Pagination libHTTP.CursorPagination `json:"pagination"`
}

// ListUnmatchedReportResponse wraps a paginated list of unmatched items.
type ListUnmatchedReportResponse struct {
	Items      []UnmatchedItemResponse  `json:"items"`
	Pagination libHTTP.CursorPagination `json:"pagination"`
}

// ListVarianceReportResponse wraps a paginated list of variance report rows.
type ListVarianceReportResponse struct {
	Items      []VarianceReportRowResponse `json:"items"`
	Pagination libHTTP.CursorPagination    `json:"pagination"`
}

// MatchedItemsToResponse converts domain entities to response DTOs.
func MatchedItemsToResponse(items []*entities.MatchedItem) []MatchedItemResponse {
	if items == nil {
		return make([]MatchedItemResponse, 0)
	}

	result := make([]MatchedItemResponse, 0, len(items))

	for _, item := range items {
		if item == nil {
			continue
		}

		result = append(result, MatchedItemResponse{
			TransactionID: item.TransactionID.String(),
			MatchGroupID:  item.MatchGroupID.String(),
			SourceID:      item.SourceID.String(),
			Amount:        item.Amount.String(),
			Currency:      item.Currency,
			Date:          item.Date.Format(time.RFC3339),
		})
	}

	return result
}

// UnmatchedItemsToResponse converts domain entities to response DTOs.
func UnmatchedItemsToResponse(items []*entities.UnmatchedItem) []UnmatchedItemResponse {
	if items == nil {
		return make([]UnmatchedItemResponse, 0)
	}

	result := make([]UnmatchedItemResponse, 0, len(items))

	for _, item := range items {
		if item == nil {
			continue
		}

		resp := UnmatchedItemResponse{
			TransactionID: item.TransactionID.String(),
			SourceID:      item.SourceID.String(),
			Amount:        item.Amount.String(),
			Currency:      item.Currency,
			Status:        item.Status,
			Date:          item.Date.Format(time.RFC3339),
		}

		if item.ExceptionID != nil {
			resp.ExceptionID = pointers.String(item.ExceptionID.String())
		}

		if item.DueAt != nil {
			resp.DueAt = pointers.String(item.DueAt.Format(time.RFC3339))
		}

		result = append(result, resp)
	}

	return result
}

// SummaryReportToResponse converts a domain SummaryReport to response DTO.
// Returns a zero-value response when summary is nil to ensure stable JSON structure.
func SummaryReportToResponse(summary *entities.SummaryReport) *SummaryReportResponse {
	if summary == nil {
		return &SummaryReportResponse{
			TotalAmount:     "0",
			MatchedAmount:   "0",
			UnmatchedAmount: "0",
		}
	}

	return &SummaryReportResponse{
		MatchedCount:    summary.MatchedCount,
		UnmatchedCount:  summary.UnmatchedCount,
		TotalAmount:     summary.TotalAmount.String(),
		MatchedAmount:   summary.MatchedAmount.String(),
		UnmatchedAmount: summary.UnmatchedAmount.String(),
	}
}

// VarianceRowsToResponse converts domain entities to response DTOs.
func VarianceRowsToResponse(rows []*entities.VarianceReportRow) []VarianceReportRowResponse {
	if rows == nil {
		return make([]VarianceReportRowResponse, 0)
	}

	result := make([]VarianceReportRowResponse, 0, len(rows))

	for _, row := range rows {
		if row == nil {
			continue
		}

		resp := VarianceReportRowResponse{
			SourceID:        row.SourceID.String(),
			Currency:        row.Currency,
			FeeScheduleID:   row.FeeScheduleID.String(),
			FeeScheduleName: row.FeeScheduleName,
			TotalExpected:   row.TotalExpected.String(),
			TotalActual:     row.TotalActual.String(),
			NetVariance:     row.NetVariance.String(),
		}

		if row.VariancePct != nil {
			resp.VariancePct = pointers.String(row.VariancePct.String())
		}

		result = append(result, resp)
	}

	return result
}

func breakdownMetricsToResponse(breakdowns *entities.BreakdownMetrics) *BreakdownMetricsResponse {
	if breakdowns == nil {
		return &BreakdownMetricsResponse{
			BySeverity: make(map[string]int),
			ByReason:   make(map[string]int),
			ByRule:     make([]RuleMatchCountDTO, 0),
			ByAge:      make([]AgeBucketDTO, 0),
		}
	}

	byRule := make([]RuleMatchCountDTO, len(breakdowns.ByRule))

	for i, r := range breakdowns.ByRule {
		byRule[i] = RuleMatchCountDTO{
			ID:    r.ID.String(),
			Name:  r.Name,
			Count: r.Count,
		}
	}

	byAge := make([]AgeBucketDTO, len(breakdowns.ByAge))

	for i, a := range breakdowns.ByAge {
		byAge[i] = AgeBucketDTO{
			Bucket: a.Bucket,
			Value:  a.Value,
		}
	}

	bySeverity := breakdowns.BySeverity
	if bySeverity == nil {
		bySeverity = make(map[string]int)
	}

	byReason := breakdowns.ByReason
	if byReason == nil {
		byReason = make(map[string]int)
	}

	return &BreakdownMetricsResponse{
		BySeverity: bySeverity,
		ByReason:   byReason,
		ByRule:     byRule,
		ByAge:      byAge,
	}
}
