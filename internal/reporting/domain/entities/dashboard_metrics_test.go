//go:build unit

package entities

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEmptyTrendMetrics(t *testing.T) {
	t.Parallel()

	trends := NewEmptyTrendMetrics()

	require.NotNil(t, trends)
	assert.Empty(t, trends.Dates)
	assert.Empty(t, trends.Ingestion)
	assert.Empty(t, trends.Matches)
	assert.Empty(t, trends.Exceptions)
	assert.Empty(t, trends.MatchRates)
}

func TestNewEmptyBreakdownMetrics(t *testing.T) {
	t.Parallel()

	breakdown := NewEmptyBreakdownMetrics()

	require.NotNil(t, breakdown)
	assert.NotNil(t, breakdown.BySeverity)
	assert.NotNil(t, breakdown.ByReason)
	assert.NotNil(t, breakdown.ByRule)
	assert.NotNil(t, breakdown.ByAge)
	assert.Empty(t, breakdown.BySeverity)
	assert.Empty(t, breakdown.ByReason)
	assert.Empty(t, breakdown.ByRule)
	assert.Empty(t, breakdown.ByAge)
}

func TestBuildTrendMetrics(t *testing.T) {
	t.Parallel()

	t.Run("empty points returns empty metrics", func(t *testing.T) {
		t.Parallel()

		result := BuildTrendMetrics(nil)

		require.NotNil(t, result)
		assert.Empty(t, result.Dates)
	})

	t.Run("converts points to response structure", func(t *testing.T) {
		t.Parallel()

		points := []DailyTrendPoint{
			{
				Date:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				Ingested:   100,
				Matched:    80,
				Exceptions: 5,
				MatchRate:  80.0,
			},
			{
				Date:       time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				Ingested:   150,
				Matched:    120,
				Exceptions: 10,
				MatchRate:  80.0,
			},
		}

		result := BuildTrendMetrics(points)

		require.NotNil(t, result)
		require.Len(t, result.Dates, 2)
		assert.Equal(t, "2024-01-01", result.Dates[0])
		assert.Equal(t, "2024-01-02", result.Dates[1])
		assert.Equal(t, []int{100, 150}, result.Ingestion)
		assert.Equal(t, []int{80, 120}, result.Matches)
		assert.Equal(t, []int{5, 10}, result.Exceptions)
		assert.Equal(t, []float64{80.0, 80.0}, result.MatchRates)
	})
}

func TestClassifyExceptionAge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ageHours float64
		expected string
	}{
		{"less than 24h", 12.0, AgeBucketLessThan24h},
		{"exactly 24h", 24.0, AgeBucketOneToThreeD},
		{"between 24h and 72h", 48.0, AgeBucketOneToThreeD},
		{"exactly 72h", 72.0, AgeBucketMoreThan3d},
		{"more than 72h", 100.0, AgeBucketMoreThan3d},
		{"zero hours", 0.0, AgeBucketLessThan24h},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := ClassifyExceptionAge(tt.ageHours)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSummaryMetrics_OldestExceptionAgeClassification(t *testing.T) {
	t.Parallel()

	t.Run("classifies exception age less than 24h", func(t *testing.T) {
		t.Parallel()

		summary := &SummaryMetrics{
			OldestExceptionAge: 12.0,
		}

		bucket := ClassifyExceptionAge(summary.OldestExceptionAge)
		assert.Equal(t, AgeBucketLessThan24h, bucket)
	})

	t.Run("classifies exception age between 24h and 72h", func(t *testing.T) {
		t.Parallel()

		summary := &SummaryMetrics{
			OldestExceptionAge: 48.5,
		}

		bucket := ClassifyExceptionAge(summary.OldestExceptionAge)
		assert.Equal(t, AgeBucketOneToThreeD, bucket)
	})

	t.Run("classifies exception age more than 72h", func(t *testing.T) {
		t.Parallel()

		summary := &SummaryMetrics{
			OldestExceptionAge: 100.0,
		}

		bucket := ClassifyExceptionAge(summary.OldestExceptionAge)
		assert.Equal(t, AgeBucketMoreThan3d, bucket)
	})

	t.Run("zero critical exposure formatted correctly", func(t *testing.T) {
		t.Parallel()

		summary := &SummaryMetrics{
			CriticalExposure: decimal.Zero,
		}

		assert.True(t, summary.CriticalExposure.IsZero())
		assert.Equal(t, "0", summary.CriticalExposure.String())
	})
}

func TestRuleMatchCount(t *testing.T) {
	t.Parallel()

	ruleID := uuid.New()
	rule := RuleMatchCount{
		ID:    ruleID,
		Name:  "EXACT",
		Count: 150,
	}

	assert.Equal(t, ruleID, rule.ID)
	assert.Equal(t, "EXACT", rule.Name)
	assert.Equal(t, 150, rule.Count)
}

func TestAgeBucket(t *testing.T) {
	t.Parallel()

	bucket := AgeBucket{
		Bucket: AgeBucketLessThan24h,
		Value:  50,
	}

	assert.Equal(t, "<24h", bucket.Bucket)
	assert.Equal(t, 50, bucket.Value)
}

func TestMatcherDashboardMetrics(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	metrics := &MatcherDashboardMetrics{
		Summary: &SummaryMetrics{
			TotalTransactions: 1000,
			TotalMatches:      450,
		},
		Trends: &TrendMetrics{
			Dates:     []string{"2024-01-01"},
			Ingestion: []int{100},
		},
		Breakdowns: &BreakdownMetrics{
			BySeverity: map[string]int{"CRITICAL": 5},
		},
		UpdatedAt: now,
	}

	require.NotNil(t, metrics.Summary)
	require.NotNil(t, metrics.Trends)
	require.NotNil(t, metrics.Breakdowns)
	assert.Equal(t, 1000, metrics.Summary.TotalTransactions)
	assert.Equal(t, now, metrics.UpdatedAt)
}
