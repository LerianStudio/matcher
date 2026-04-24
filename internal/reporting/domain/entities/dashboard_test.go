// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestCalculateMatchRate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		volume         *VolumeStats
		expectedRate   float64
		expectedAmount float64
	}{
		{
			name:           "nil volume returns empty stats",
			volume:         nil,
			expectedRate:   0,
			expectedAmount: 0,
		},
		{
			name: "zero transactions returns zero rate",
			volume: &VolumeStats{
				TotalTransactions:   0,
				MatchedTransactions: 0,
			},
			expectedRate:   0,
			expectedAmount: 0,
		},
		{
			name: "100% match rate",
			volume: &VolumeStats{
				TotalTransactions:   100,
				MatchedTransactions: 100,
				UnmatchedCount:      0,
				TotalAmount:         decimal.NewFromInt(1000),
				MatchedAmount:       decimal.NewFromInt(1000),
				UnmatchedAmount:     decimal.Zero,
			},
			expectedRate:   100,
			expectedAmount: 100,
		},
		{
			name: "50% match rate",
			volume: &VolumeStats{
				TotalTransactions:   100,
				MatchedTransactions: 50,
				UnmatchedCount:      50,
				TotalAmount:         decimal.NewFromInt(1000),
				MatchedAmount:       decimal.NewFromInt(500),
				UnmatchedAmount:     decimal.NewFromInt(500),
			},
			expectedRate:   50,
			expectedAmount: 50,
		},
		{
			name: "75% match rate with different amount ratio",
			volume: &VolumeStats{
				TotalTransactions:   100,
				MatchedTransactions: 75,
				UnmatchedCount:      25,
				TotalAmount:         decimal.NewFromInt(1000),
				MatchedAmount:       decimal.NewFromInt(800),
				UnmatchedAmount:     decimal.NewFromInt(200),
			},
			expectedRate:   75,
			expectedAmount: 80,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CalculateMatchRate(tt.volume)

			assert.NotNil(t, result)
			assert.InDelta(t, tt.expectedRate, result.MatchRate, 0.01)
			assert.InDelta(t, tt.expectedAmount, result.MatchRateAmount, 0.01)
		})
	}
}

func TestCalculateSLACompliance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		stats    *SLAStats
		expected float64
	}{
		{
			name:     "nil stats returns 0",
			stats:    nil,
			expected: 0,
		},
		{
			name: "no resolved items without overdue returns 100%",
			stats: &SLAStats{
				ResolvedOnTime:   0,
				ResolvedLate:     0,
				PendingWithinSLA: 2,
				PendingOverdue:   0,
			},
			expected: 100,
		},
		{
			name: "no resolved items with overdue returns 0%",
			stats: &SLAStats{
				ResolvedOnTime:   0,
				ResolvedLate:     0,
				PendingWithinSLA: 0,
				PendingOverdue:   3,
			},
			expected: 0,
		},
		{
			name: "all resolved on time returns 100%",
			stats: &SLAStats{
				ResolvedOnTime: 10,
				ResolvedLate:   0,
			},
			expected: 100,
		},
		{
			name: "50% on time compliance",
			stats: &SLAStats{
				ResolvedOnTime: 5,
				ResolvedLate:   5,
			},
			expected: 50,
		},
		{
			name: "75% on time compliance",
			stats: &SLAStats{
				ResolvedOnTime: 75,
				ResolvedLate:   25,
			},
			expected: 75,
		},
		{
			name: "pending overdue reduces compliance",
			stats: &SLAStats{
				ResolvedOnTime: 1,
				ResolvedLate:   0,
				PendingOverdue: 2,
			},
			expected: 33.33,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := CalculateSLACompliance(tt.stats)
			assert.InDelta(t, tt.expected, result, 0.01)
		})
	}
}
