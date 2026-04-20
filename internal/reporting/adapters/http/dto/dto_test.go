//go:build unit

package dto

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/reporting/domain/entities"
)

func TestVolumeStatsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.VolumeStats
		expected *VolumeStatsResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: &VolumeStatsResponse{},
		},
		{
			name: "valid input",
			input: &entities.VolumeStats{
				TotalTransactions:   100,
				MatchedTransactions: 80,
				UnmatchedCount:      20,
				TotalAmount:         decimal.NewFromFloat(10000.50),
				MatchedAmount:       decimal.NewFromFloat(8000.25),
				UnmatchedAmount:     decimal.NewFromFloat(2000.25),
				PeriodStart:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				PeriodEnd:           time.Date(2025, 1, 31, 23, 59, 59, 0, time.UTC),
			},
			expected: &VolumeStatsResponse{
				TotalTransactions:   100,
				MatchedTransactions: 80,
				UnmatchedCount:      20,
				TotalAmount:         "10000.5",
				MatchedAmount:       "8000.25",
				UnmatchedAmount:     "2000.25",
				PeriodStart:         "2025-01-01T00:00:00Z",
				PeriodEnd:           "2025-01-31T23:59:59Z",
			},
		},
		{
			name: "zero values",
			input: &entities.VolumeStats{
				TotalTransactions:   0,
				MatchedTransactions: 0,
				UnmatchedCount:      0,
				TotalAmount:         decimal.Zero,
				MatchedAmount:       decimal.Zero,
				UnmatchedAmount:     decimal.Zero,
				PeriodStart:         time.Time{},
				PeriodEnd:           time.Time{},
			},
			expected: &VolumeStatsResponse{
				TotalTransactions:   0,
				MatchedTransactions: 0,
				UnmatchedCount:      0,
				TotalAmount:         "0",
				MatchedAmount:       "0",
				UnmatchedAmount:     "0",
				PeriodStart:         "0001-01-01T00:00:00Z",
				PeriodEnd:           "0001-01-01T00:00:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := VolumeStatsToResponse(tt.input)

			assert.NotNil(t, result)
			assert.Equal(t, tt.expected.TotalTransactions, result.TotalTransactions)
			assert.Equal(t, tt.expected.MatchedTransactions, result.MatchedTransactions)
			assert.Equal(t, tt.expected.UnmatchedCount, result.UnmatchedCount)
			assert.Equal(t, tt.expected.TotalAmount, result.TotalAmount)
			assert.Equal(t, tt.expected.MatchedAmount, result.MatchedAmount)
			assert.Equal(t, tt.expected.UnmatchedAmount, result.UnmatchedAmount)
			assert.Equal(t, tt.expected.PeriodStart, result.PeriodStart)
			assert.Equal(t, tt.expected.PeriodEnd, result.PeriodEnd)
		})
	}
}

func TestMatchRateStatsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.MatchRateStats
		expected *MatchRateStatsResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: &MatchRateStatsResponse{},
		},
		{
			name: "valid input",
			input: &entities.MatchRateStats{
				MatchRate:       85.5,
				MatchRateAmount: 90.2,
				TotalCount:      1000,
				MatchedCount:    855,
				UnmatchedCount:  145,
			},
			expected: &MatchRateStatsResponse{
				MatchRate:       85.5,
				MatchRateAmount: 90.2,
				TotalCount:      1000,
				MatchedCount:    855,
				UnmatchedCount:  145,
			},
		},
		{
			name: "zero values",
			input: &entities.MatchRateStats{
				MatchRate:       0,
				MatchRateAmount: 0,
				TotalCount:      0,
				MatchedCount:    0,
				UnmatchedCount:  0,
			},
			expected: &MatchRateStatsResponse{
				MatchRate:       0,
				MatchRateAmount: 0,
				TotalCount:      0,
				MatchedCount:    0,
				UnmatchedCount:  0,
			},
		},
		{
			name: "100% match rate",
			input: &entities.MatchRateStats{
				MatchRate:       100.0,
				MatchRateAmount: 100.0,
				TotalCount:      500,
				MatchedCount:    500,
				UnmatchedCount:  0,
			},
			expected: &MatchRateStatsResponse{
				MatchRate:       100.0,
				MatchRateAmount: 100.0,
				TotalCount:      500,
				MatchedCount:    500,
				UnmatchedCount:  0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := MatchRateStatsToResponse(tt.input)

			assert.NotNil(t, result)
			assert.InDelta(t, tt.expected.MatchRate, result.MatchRate, 0.001)
			assert.InDelta(t, tt.expected.MatchRateAmount, result.MatchRateAmount, 0.001)
			assert.Equal(t, tt.expected.TotalCount, result.TotalCount)
			assert.Equal(t, tt.expected.MatchedCount, result.MatchedCount)
			assert.Equal(t, tt.expected.UnmatchedCount, result.UnmatchedCount)
		})
	}
}

func TestSLAStatsToResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *entities.SLAStats
		expected *SLAStatsResponse
	}{
		{
			name:     "nil input returns empty struct",
			input:    nil,
			expected: &SLAStatsResponse{},
		},
		{
			name: "valid input",
			input: &entities.SLAStats{
				TotalExceptions:     50,
				ResolvedOnTime:      40,
				ResolvedLate:        5,
				PendingWithinSLA:    3,
				PendingOverdue:      2,
				SLAComplianceRate:   80.0,
				AverageResolutionMs: 3600000,
			},
			expected: &SLAStatsResponse{
				TotalExceptions:     50,
				ResolvedOnTime:      40,
				ResolvedLate:        5,
				PendingWithinSLA:    3,
				PendingOverdue:      2,
				SLAComplianceRate:   80.0,
				AverageResolutionMs: 3600000,
			},
		},
		{
			name: "zero values",
			input: &entities.SLAStats{
				TotalExceptions:     0,
				ResolvedOnTime:      0,
				ResolvedLate:        0,
				PendingWithinSLA:    0,
				PendingOverdue:      0,
				SLAComplianceRate:   0,
				AverageResolutionMs: 0,
			},
			expected: &SLAStatsResponse{
				TotalExceptions:     0,
				ResolvedOnTime:      0,
				ResolvedLate:        0,
				PendingWithinSLA:    0,
				PendingOverdue:      0,
				SLAComplianceRate:   0,
				AverageResolutionMs: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := SLAStatsToResponse(tt.input)

			assert.NotNil(t, result)
			assert.Equal(t, tt.expected.TotalExceptions, result.TotalExceptions)
			assert.Equal(t, tt.expected.ResolvedOnTime, result.ResolvedOnTime)
			assert.Equal(t, tt.expected.ResolvedLate, result.ResolvedLate)
			assert.Equal(t, tt.expected.PendingWithinSLA, result.PendingWithinSLA)
			assert.Equal(t, tt.expected.PendingOverdue, result.PendingOverdue)
			assert.InDelta(t, tt.expected.SLAComplianceRate, result.SLAComplianceRate, 0.001)
			assert.Equal(t, tt.expected.AverageResolutionMs, result.AverageResolutionMs)
		})
	}
}

func createFullDashboardAggregatesInput(now time.Time) *entities.DashboardAggregates {
	return &entities.DashboardAggregates{
		Volume: &entities.VolumeStats{
			TotalTransactions:   100,
			MatchedTransactions: 80,
			UnmatchedCount:      20,
			TotalAmount:         decimal.NewFromFloat(10000.00),
			MatchedAmount:       decimal.NewFromFloat(8000.00),
			UnmatchedAmount:     decimal.NewFromFloat(2000.00),
			PeriodStart:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			PeriodEnd:           time.Date(2025, 1, 31, 0, 0, 0, 0, time.UTC),
		},
		MatchRate: &entities.MatchRateStats{
			MatchRate:       80.0,
			MatchRateAmount: 80.0,
			TotalCount:      100,
			MatchedCount:    80,
			UnmatchedCount:  20,
		},
		SLA: &entities.SLAStats{
			TotalExceptions:     10,
			ResolvedOnTime:      8,
			ResolvedLate:        2,
			PendingWithinSLA:    0,
			PendingOverdue:      0,
			SLAComplianceRate:   80.0,
			AverageResolutionMs: 1800000,
		},
		UpdatedAt: now,
	}
}

func createFullDashboardAggregatesResponse() *DashboardAggregatesResponse {
	return &DashboardAggregatesResponse{
		Volume: &VolumeStatsResponse{
			TotalTransactions:   100,
			MatchedTransactions: 80,
			UnmatchedCount:      20,
			TotalAmount:         "10000",
			MatchedAmount:       "8000",
			UnmatchedAmount:     "2000",
			PeriodStart:         "2025-01-01T00:00:00Z",
			PeriodEnd:           "2025-01-31T00:00:00Z",
		},
		MatchRate: &MatchRateStatsResponse{
			MatchRate:       80.0,
			MatchRateAmount: 80.0,
			TotalCount:      100,
			MatchedCount:    80,
			UnmatchedCount:  20,
		},
		SLA: &SLAStatsResponse{
			TotalExceptions:     10,
			ResolvedOnTime:      8,
			ResolvedLate:        2,
			PendingWithinSLA:    0,
			PendingOverdue:      0,
			SLAComplianceRate:   80.0,
			AverageResolutionMs: 1800000,
		},
		UpdatedAt: "2025-01-15T12:00:00Z",
	}
}

func TestDashboardAggregatesToResponse(t *testing.T) {
	t.Parallel()

	now := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		input    *entities.DashboardAggregates
		expected *DashboardAggregatesResponse
	}{
		{
			name:  "nil input returns empty struct",
			input: nil,
			expected: &DashboardAggregatesResponse{
				Volume:    &VolumeStatsResponse{},
				MatchRate: &MatchRateStatsResponse{},
				SLA:       &SLAStatsResponse{},
			},
		},
		{
			name:     "full aggregates",
			input:    createFullDashboardAggregatesInput(now),
			expected: createFullDashboardAggregatesResponse(),
		},
		{
			name: "nil nested fields return empty structs",
			input: &entities.DashboardAggregates{
				Volume:    nil,
				MatchRate: nil,
				SLA:       nil,
				UpdatedAt: now,
			},
			expected: &DashboardAggregatesResponse{
				Volume:    &VolumeStatsResponse{},
				MatchRate: &MatchRateStatsResponse{},
				SLA:       &SLAStatsResponse{},
				UpdatedAt: "2025-01-15T12:00:00Z",
			},
		},
		{
			name: "partial fields",
			input: &entities.DashboardAggregates{
				Volume: &entities.VolumeStats{
					TotalTransactions: 50,
					TotalAmount:       decimal.NewFromFloat(5000.00),
					MatchedAmount:     decimal.Zero,
					UnmatchedAmount:   decimal.NewFromFloat(5000.00),
					PeriodStart:       time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
					PeriodEnd:         time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC),
				},
				UpdatedAt: now,
			},
			expected: &DashboardAggregatesResponse{
				Volume: &VolumeStatsResponse{
					TotalTransactions: 50,
					TotalAmount:       "5000",
					MatchedAmount:     "0",
					UnmatchedAmount:   "5000",
					PeriodStart:       "2025-01-01T00:00:00Z",
					PeriodEnd:         "2025-01-15T00:00:00Z",
				},
				MatchRate: &MatchRateStatsResponse{},
				SLA:       &SLAStatsResponse{},
				UpdatedAt: "2025-01-15T12:00:00Z",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := DashboardAggregatesToResponse(tt.input)

			assert.NotNil(t, result)

			assert.NotNil(t, result.Volume)
			assert.Equal(t, tt.expected.Volume.TotalTransactions, result.Volume.TotalTransactions)

			assert.NotNil(t, result.MatchRate)
			assert.InDelta(t, tt.expected.MatchRate.MatchRate, result.MatchRate.MatchRate, 0.001)

			assert.NotNil(t, result.SLA)
			assert.Equal(t, tt.expected.SLA.TotalExceptions, result.SLA.TotalExceptions)

			if tt.expected.UpdatedAt != "" {
				assert.Equal(t, tt.expected.UpdatedAt, result.UpdatedAt)
			}
		})
	}
}

func TestMatcherDashboardMetricsToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty struct", func(t *testing.T) {
		t.Parallel()

		result := MatcherDashboardMetricsToResponse(nil)
		assert.NotNil(t, result)
		assert.NotNil(t, result.Summary)
		assert.NotNil(t, result.Trends)
		assert.NotNil(t, result.Breakdowns)
		assert.Equal(t, []string{}, result.Trends.Dates)
		assert.Equal(t, []int{}, result.Trends.Ingestion)
		assert.Equal(t, []int{}, result.Trends.Matches)
		assert.Equal(t, []int{}, result.Trends.Exceptions)
		assert.Equal(t, []float64{}, result.Trends.MatchRates)
	})

	t.Run("valid input converts correctly", func(t *testing.T) {
		t.Parallel()

		ruleID := "550e8400-e29b-41d4-a716-446655440000"
		input := &entities.MatcherDashboardMetrics{
			Summary: &entities.SummaryMetrics{
				TotalTransactions:  1000,
				TotalMatches:       450,
				MatchRate:          90.0,
				PendingExceptions:  25,
				CriticalExposure:   decimal.NewFromInt(50000),
				OldestExceptionAge: 48.5,
			},
			Trends: &entities.TrendMetrics{
				Dates:      []string{"2024-01-01", "2024-01-02"},
				Ingestion:  []int{100, 150},
				Matches:    []int{80, 120},
				Exceptions: []int{5, 10},
				MatchRates: []float64{80.0, 80.0},
			},
			Breakdowns: &entities.BreakdownMetrics{
				BySeverity: map[string]int{"CRITICAL": 5, "HIGH": 10},
				ByReason:   map[string]int{"Amount Mismatch": 8},
				ByRule: []entities.RuleMatchCount{
					{ID: parseUUID(ruleID), Name: "EXACT", Count: 150},
				},
				ByAge: []entities.AgeBucket{
					{Bucket: "<24h", Value: 50},
				},
			},
			UpdatedAt: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
		}

		result := MatcherDashboardMetricsToResponse(input)

		assert.NotNil(t, result)
		assert.NotNil(t, result.Summary)
		assert.Equal(t, 1000, result.Summary.TotalTransactions)
		assert.Equal(t, 450, result.Summary.TotalMatches)
		assert.InDelta(t, 90.0, result.Summary.MatchRate, 0.001)
		assert.Equal(t, 25, result.Summary.PendingExceptions)
		assert.Equal(t, "50000", result.Summary.CriticalExposure)
		assert.InDelta(t, 48.5, result.Summary.OldestExceptionAge, 0.001)

		assert.NotNil(t, result.Trends)
		assert.Equal(t, []string{"2024-01-01", "2024-01-02"}, result.Trends.Dates)
		assert.Equal(t, []int{100, 150}, result.Trends.Ingestion)

		assert.NotNil(t, result.Breakdowns)
		assert.Equal(t, 5, result.Breakdowns.BySeverity["CRITICAL"])
		assert.Len(t, result.Breakdowns.ByRule, 1)
		assert.Equal(t, ruleID, result.Breakdowns.ByRule[0].ID)
		assert.Equal(t, "EXACT", result.Breakdowns.ByRule[0].Name)

		assert.Equal(t, "2024-01-15T10:00:00Z", result.UpdatedAt)
	})

	t.Run("nil nested fields handled correctly", func(t *testing.T) {
		t.Parallel()

		input := &entities.MatcherDashboardMetrics{
			Summary:    nil,
			Trends:     nil,
			Breakdowns: nil,
			UpdatedAt:  time.Now().UTC(),
		}

		result := MatcherDashboardMetricsToResponse(input)

		assert.NotNil(t, result)
		assert.NotNil(t, result.Summary)
		assert.NotNil(t, result.Trends)
		assert.Equal(t, []string{}, result.Trends.Dates)
		assert.Equal(t, []int{}, result.Trends.Ingestion)
		assert.NotNil(t, result.Breakdowns)
		assert.NotNil(t, result.Breakdowns.BySeverity)
		assert.NotNil(t, result.Breakdowns.ByReason)
		assert.NotNil(t, result.Breakdowns.ByRule)
		assert.NotNil(t, result.Breakdowns.ByAge)
		assert.Empty(t, result.Breakdowns.BySeverity)
		assert.Empty(t, result.Breakdowns.ByReason)
		assert.Empty(t, result.Breakdowns.ByRule)
		assert.Empty(t, result.Breakdowns.ByAge)
	})
}

// --- MatchedItemsToResponse tests ---

func TestMatchedItemsToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty slice", func(t *testing.T) {
		t.Parallel()

		result := MatchedItemsToResponse(nil)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("valid items", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		groupID := uuid.New()
		sourceID := uuid.New()
		date := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)

		items := []*entities.MatchedItem{
			{
				TransactionID: txID,
				MatchGroupID:  groupID,
				SourceID:      sourceID,
				Amount:        decimal.NewFromFloat(1250.50),
				Currency:      "USD",
				Date:          date,
			},
		}

		result := MatchedItemsToResponse(items)

		require.Len(t, result, 1)
		assert.Equal(t, txID.String(), result[0].TransactionID)
		assert.Equal(t, groupID.String(), result[0].MatchGroupID)
		assert.Equal(t, sourceID.String(), result[0].SourceID)
		assert.Equal(t, "1250.5", result[0].Amount)
		assert.Equal(t, "USD", result[0].Currency)
		assert.Equal(t, "2025-01-15T10:30:00Z", result[0].Date)
	})

	t.Run("skips nil items", func(t *testing.T) {
		t.Parallel()

		items := []*entities.MatchedItem{nil, nil}

		result := MatchedItemsToResponse(items)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("empty slice returns empty slice", func(t *testing.T) {
		t.Parallel()

		items := make([]*entities.MatchedItem, 0)

		result := MatchedItemsToResponse(items)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

// --- UnmatchedItemsToResponse tests ---

func TestUnmatchedItemsToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty slice", func(t *testing.T) {
		t.Parallel()

		result := UnmatchedItemsToResponse(nil)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("valid items with optional fields", func(t *testing.T) {
		t.Parallel()

		txID := uuid.New()
		sourceID := uuid.New()
		exceptionID := uuid.New()
		date := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
		dueAt := time.Date(2025, 2, 15, 10, 30, 0, 0, time.UTC)

		items := []*entities.UnmatchedItem{
			{
				TransactionID: txID,
				SourceID:      sourceID,
				Amount:        decimal.NewFromFloat(500.25),
				Currency:      "EUR",
				Status:        "PENDING",
				ExceptionID:   &exceptionID,
				DueAt:         &dueAt,
				Date:          date,
			},
		}

		result := UnmatchedItemsToResponse(items)

		require.Len(t, result, 1)
		assert.Equal(t, txID.String(), result[0].TransactionID)
		assert.Equal(t, sourceID.String(), result[0].SourceID)
		assert.Equal(t, "500.25", result[0].Amount)
		assert.Equal(t, "EUR", result[0].Currency)
		assert.Equal(t, "PENDING", result[0].Status)
		assert.NotNil(t, result[0].ExceptionID)
		assert.Equal(t, exceptionID.String(), *result[0].ExceptionID)
		assert.NotNil(t, result[0].DueAt)
		assert.Equal(t, "2025-02-15T10:30:00Z", *result[0].DueAt)
	})

	t.Run("valid items without optional fields", func(t *testing.T) {
		t.Parallel()

		fixedTxID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		fixedSourceID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
		fixedDate := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

		items := []*entities.UnmatchedItem{
			{
				TransactionID: fixedTxID,
				SourceID:      fixedSourceID,
				Amount:        decimal.NewFromInt(100),
				Currency:      "USD",
				Status:        "OPEN",
				Date:          fixedDate,
			},
		}

		result := UnmatchedItemsToResponse(items)

		require.Len(t, result, 1)
		assert.Equal(t, fixedTxID.String(), result[0].TransactionID)
		assert.Equal(t, fixedSourceID.String(), result[0].SourceID)
		assert.Nil(t, result[0].ExceptionID)
		assert.Nil(t, result[0].DueAt)
	})

	t.Run("empty slice returns empty slice", func(t *testing.T) {
		t.Parallel()

		items := make([]*entities.UnmatchedItem, 0)

		result := UnmatchedItemsToResponse(items)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

// --- SummaryReportToResponse tests ---

func TestSummaryReportToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns zero-value response", func(t *testing.T) {
		t.Parallel()

		result := SummaryReportToResponse(nil)
		assert.NotNil(t, result)
		assert.Equal(t, "0", result.TotalAmount)
		assert.Equal(t, "0", result.MatchedAmount)
		assert.Equal(t, "0", result.UnmatchedAmount)
		assert.Equal(t, 0, result.MatchedCount)
		assert.Equal(t, 0, result.UnmatchedCount)
	})

	t.Run("valid summary", func(t *testing.T) {
		t.Parallel()

		summary := &entities.SummaryReport{
			MatchedCount:    80,
			UnmatchedCount:  20,
			TotalAmount:     decimal.NewFromInt(10000),
			MatchedAmount:   decimal.NewFromInt(8000),
			UnmatchedAmount: decimal.NewFromInt(2000),
		}

		result := SummaryReportToResponse(summary)

		assert.Equal(t, 80, result.MatchedCount)
		assert.Equal(t, 20, result.UnmatchedCount)
		assert.Equal(t, "10000", result.TotalAmount)
		assert.Equal(t, "8000", result.MatchedAmount)
		assert.Equal(t, "2000", result.UnmatchedAmount)
	})

	t.Run("zero-value input returns zero amounts", func(t *testing.T) {
		t.Parallel()

		summary := &entities.SummaryReport{
			MatchedCount:    0,
			UnmatchedCount:  0,
			TotalAmount:     decimal.Zero,
			MatchedAmount:   decimal.Zero,
			UnmatchedAmount: decimal.Zero,
		}

		result := SummaryReportToResponse(summary)

		assert.NotNil(t, result)
		assert.Equal(t, 0, result.MatchedCount)
		assert.Equal(t, 0, result.UnmatchedCount)
		assert.Equal(t, "0", result.TotalAmount)
		assert.Equal(t, "0", result.MatchedAmount)
		assert.Equal(t, "0", result.UnmatchedAmount)
	})
}

// --- VarianceRowsToResponse tests ---

func TestVarianceRowsToResponse(t *testing.T) {
	t.Parallel()

	t.Run("nil input returns empty slice", func(t *testing.T) {
		t.Parallel()

		result := VarianceRowsToResponse(nil)
		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("valid rows with variance percentage", func(t *testing.T) {
		t.Parallel()

		sourceID := uuid.New()
		pct := decimal.NewFromFloat(-4.0)

		rows := []*entities.VarianceReportRow{
			{
				SourceID:        sourceID,
				Currency:        "USD",
				FeeScheduleName: "INTERCHANGE",
				TotalExpected:   decimal.NewFromInt(5000),
				TotalActual:     decimal.NewFromInt(4800),
				NetVariance:     decimal.NewFromInt(-200),
				VariancePct:     &pct,
			},
		}

		result := VarianceRowsToResponse(rows)

		require.Len(t, result, 1)
		assert.Equal(t, sourceID.String(), result[0].SourceID)
		assert.Equal(t, "USD", result[0].Currency)
		assert.Equal(t, "INTERCHANGE", result[0].FeeScheduleName)
		assert.Equal(t, "5000", result[0].TotalExpected)
		assert.Equal(t, "4800", result[0].TotalActual)
		assert.Equal(t, "-200", result[0].NetVariance)
		assert.NotNil(t, result[0].VariancePct)
		assert.Equal(t, "-4", *result[0].VariancePct)
	})

	t.Run("valid rows without variance percentage", func(t *testing.T) {
		t.Parallel()

		rows := []*entities.VarianceReportRow{
			{
				SourceID:        uuid.New(),
				Currency:        "EUR",
				FeeScheduleName: "PROCESSING",
				TotalExpected:   decimal.Zero,
				TotalActual:     decimal.NewFromInt(100),
				NetVariance:     decimal.NewFromInt(100),
			},
		}

		result := VarianceRowsToResponse(rows)

		require.Len(t, result, 1)
		assert.Nil(t, result[0].VariancePct)
	})

	t.Run("skips nil rows", func(t *testing.T) {
		t.Parallel()

		rows := []*entities.VarianceReportRow{nil}

		result := VarianceRowsToResponse(rows)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})

	t.Run("empty slice returns empty slice", func(t *testing.T) {
		t.Parallel()

		rows := make([]*entities.VarianceReportRow, 0)

		result := VarianceRowsToResponse(rows)

		assert.NotNil(t, result)
		assert.Empty(t, result)
	})
}

func parseUUID(s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		panic("parseUUID: invalid UUID " + s + ": " + err.Error())
	}

	return id
}
