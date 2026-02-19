// Package entities defines reporting domain types and aggregation logic.
package entities

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-uncommons/v2/uncommons/safe"
)

const (
	// fullPercentage is the value representing 100% compliance.
	fullPercentage = 100.0
)

// DashboardFilter defines criteria for querying dashboard metrics.
type DashboardFilter struct {
	ContextID uuid.UUID
	DateFrom  time.Time
	DateTo    time.Time
	SourceID  *uuid.UUID
}

// VolumeStats represents transaction volume statistics.
type VolumeStats struct {
	TotalTransactions   int             `json:"totalTransactions"`
	MatchedTransactions int             `json:"matchedTransactions"`
	UnmatchedCount      int             `json:"unmatchedCount"`
	TotalAmount         decimal.Decimal `json:"totalAmount"`
	MatchedAmount       decimal.Decimal `json:"matchedAmount"`
	UnmatchedAmount     decimal.Decimal `json:"unmatchedAmount"`
	PeriodStart         time.Time       `json:"periodStart"`
	PeriodEnd           time.Time       `json:"periodEnd"`
}

// MatchRateStats represents match rate statistics.
type MatchRateStats struct {
	MatchRate       float64 `json:"matchRate"`
	MatchRateAmount float64 `json:"matchRateAmount"`
	TotalCount      int     `json:"totalCount"`
	MatchedCount    int     `json:"matchedCount"`
	UnmatchedCount  int     `json:"unmatchedCount"`
}

// SLAStats represents SLA compliance statistics.
type SLAStats struct {
	TotalExceptions     int     `json:"totalExceptions"`
	ResolvedOnTime      int     `json:"resolvedOnTime"`
	ResolvedLate        int     `json:"resolvedLate"`
	PendingWithinSLA    int     `json:"pendingWithinSla"`
	PendingOverdue      int     `json:"pendingOverdue"`
	SLAComplianceRate   float64 `json:"slaComplianceRate"`
	AverageResolutionMs int64   `json:"averageResolutionMs"`
}

// DashboardAggregates combines all dashboard metrics.
type DashboardAggregates struct {
	Volume    *VolumeStats    `json:"volume"`
	MatchRate *MatchRateStats `json:"matchRate"`
	SLA       *SLAStats       `json:"sla"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

// CalculateMatchRate computes match rate from volume stats.
func CalculateMatchRate(volume *VolumeStats) *MatchRateStats {
	if volume == nil || volume.TotalTransactions == 0 {
		return &MatchRateStats{}
	}

	matched := decimal.NewFromInt(int64(volume.MatchedTransactions))
	total := decimal.NewFromInt(int64(volume.TotalTransactions))
	matchRateDecimal := safe.PercentageOrZero(matched, total)
	matchRate, _ := matchRateDecimal.Float64()

	matchRateAmountDecimal := safe.PercentageOrZero(volume.MatchedAmount, volume.TotalAmount)
	matchRateAmount, _ := matchRateAmountDecimal.Float64()

	return &MatchRateStats{
		MatchRate:       matchRate,
		MatchRateAmount: matchRateAmount,
		TotalCount:      volume.TotalTransactions,
		MatchedCount:    volume.MatchedTransactions,
		UnmatchedCount:  volume.UnmatchedCount,
	}
}

// CalculateSLACompliance computes SLA compliance rate.
func CalculateSLACompliance(stats *SLAStats) float64 {
	if stats == nil {
		return 0
	}

	denominator := stats.ResolvedOnTime + stats.ResolvedLate + stats.PendingOverdue
	if denominator == 0 {
		return fullPercentage
	}

	resolved := decimal.NewFromInt(int64(stats.ResolvedOnTime))
	denom := decimal.NewFromInt(int64(denominator))
	rate := safe.PercentageOrZero(resolved, denom)
	result, _ := rate.Float64()

	return result
}
