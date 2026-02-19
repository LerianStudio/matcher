//go:build unit

package services

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfidenceWeights(t *testing.T) {
	t.Parallel()

	weights := DefaultConfidenceWeights()

	assert.InDelta(t, 0.40, weights.Amount, 0.001)
	assert.InDelta(t, 0.30, weights.Currency, 0.001)
	assert.InDelta(t, 0.20, weights.Date, 0.001)
	assert.InDelta(t, 0.10, weights.Reference, 0.001)

	total := weights.Amount + weights.Currency + weights.Date + weights.Reference
	assert.InDelta(t, 1.0, total, 0.001)
}

func TestCalculateConfidenceScore_AllMatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    true,
		CurrencyMatch:  true,
		DateMatch:      true,
		ReferenceScore: 1.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 100, score)
}

func TestCalculateConfidenceScore_AmountMismatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    false,
		CurrencyMatch:  true,
		DateMatch:      true,
		ReferenceScore: 1.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 60, score)
}

func TestScoreToleranceConfidence_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ToleranceConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "currency mismatch",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				MatchReference:     true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("99999999-1111-2222-3333-444444444444"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("55555555-6666-7777-8888-999999999999"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "EUR",
				Date:           fixedTime,
			},
			expected: 70,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			score := ScoreToleranceConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(t, tt.expected, score)
		})
	}
}

func TestCalculateConfidenceScore_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    true,
		CurrencyMatch:  false,
		DateMatch:      true,
		ReferenceScore: 1.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 70, score)
}

func TestCalculateConfidenceScore_DateMismatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    true,
		CurrencyMatch:  true,
		DateMatch:      false,
		ReferenceScore: 1.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 80, score)
}

func TestCalculateConfidenceScore_NoReferenceMatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    true,
		CurrencyMatch:  true,
		DateMatch:      true,
		ReferenceScore: 0.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 90, score)
}

func TestCalculateConfidenceScore_OnlyAmountMatch(t *testing.T) {
	t.Parallel()

	components := ConfidenceComponents{
		AmountMatch:    true,
		CurrencyMatch:  false,
		DateMatch:      false,
		ReferenceScore: 0.0,
	}

	score := CalculateConfidenceScore(components, DefaultConfidenceWeights())

	assert.Equal(t, 40, score)
}

func TestScoreExactConfidence_NilConfig(t *testing.T) {
	t.Parallel()

	left := newTestTransactionWithID(fixedTestID1)
	right := newTestTransactionWithID(fixedTestID2)

	score := ScoreExactConfidence(nil, left, right)

	assert.Equal(t, 0, score)
}

func TestScoreExactConfidence_ExactMatch(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ExactConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "exact match",
			cfg: &ExactConfig{
				MatchAmount:    true,
				MatchCurrency:  true,
				MatchDate:      true,
				DatePrecision:  DatePrecisionDay,
				MatchReference: true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "REF123",
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "REF123",
			},
			expected: 100,
		},
		{
			name: "amount mismatch",
			cfg: &ExactConfig{
				MatchAmount:   true,
				MatchCurrency: true,
				MatchDate:     true,
				DatePrecision: DatePrecisionDay,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				Amount:         decimal.RequireFromString("200.00"),
				OriginalAmount: decimal.RequireFromString("200.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			expected: 60,
		},
		{
			name: "base currency missing",
			cfg: &ExactConfig{
				MatchAmount:       true,
				MatchCurrency:     true,
				MatchDate:         true,
				DatePrecision:     DatePrecisionDay,
				MatchReference:    true,
				MatchBaseCurrency: true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("11111111-2222-3333-4444-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("66666666-7777-8888-9999-aaaaaaaaaaaa"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			expected: 70,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreExactConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(t, tt.expected, score)
		})
	}
}

func TestScoreToleranceConfidence_NilConfig(t *testing.T) {
	t.Parallel()

	left := newTestTransactionWithID(fixedTestID1)
	right := newTestTransactionWithID(fixedTestID2)

	score := ScoreToleranceConfidence(nil, left, right)

	assert.Equal(t, 0, score)
}

func TestScoreToleranceConfidence_WithinTolerance(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ToleranceConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "within tolerance",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("5.00"),
				PercentTolerance:   decimal.Zero,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				Amount:         decimal.RequireFromString("101.00"),
				OriginalAmount: decimal.RequireFromString("101.00"),
				Currency:       "USD",
				Date:           fixedTime,
			},
			expected: 90,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreToleranceConfidence(tt.cfg, tt.left, tt.right)
			require.Equal(t, tt.expected, score)
		})
	}
}

func TestScoreToleranceConfidence_MatchingReferencesScore100(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ToleranceConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "matching references",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				MatchReference:     true,
				CaseInsensitive:    true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "REF123",
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "REF123",
			},
			expected: 100,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreToleranceConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(
				t,
				tt.expected,
				score,
				"tolerance with matching references should reach 100%",
			)
		})
	}
}

func TestScoreToleranceConfidence_DisabledReferenceMatchingScore100(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ToleranceConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "reference matching disabled",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				MatchReference:     false,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "REF123",
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      "DIFFERENT",
			},
			expected: 90,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreToleranceConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(
				t,
				tt.expected,
				score,
				"tolerance with MatchReference=false should not award reference score",
			)
		})
	}
}

func TestScoreToleranceConfidence_MissingCurrency(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *ToleranceConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "missing currency",
			cfg: &ToleranceConfig{
				MatchCurrency:      false,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				MatchReference:     true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Date:           fixedTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("ffffffff-1111-2222-3333-444444444444"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Date:           fixedTime,
			},
			expected: 70,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreToleranceConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(t, tt.expected, score)
		})
	}
}

func TestScoreToleranceConfidence_WithReferenceMatchEnabled(t *testing.T) {
	t.Parallel()

	fixedTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		leftRef       string
		rightRef      string
		caseInsens    bool
		mustSet       bool
		expectedScore int
	}{
		{
			name:          "references match exactly",
			leftRef:       "REF123",
			rightRef:      "REF123",
			expectedScore: 100,
		},
		{
			name:          "references match case-insensitive",
			leftRef:       "REF123",
			rightRef:      "ref123",
			caseInsens:    true,
			expectedScore: 100,
		},
		{
			name:          "references differ case-sensitive",
			leftRef:       "REF123",
			rightRef:      "ref123",
			caseInsens:    false,
			expectedScore: 90, // Amount(40) + Currency(30) + Date(20) = 90, no reference
		},
		{
			name:          "references differ completely",
			leftRef:       "REF123",
			rightRef:      "REF456",
			expectedScore: 90,
		},
		{
			name:          "references differ completely with mustSet=true",
			leftRef:       "REF123",
			rightRef:      "REF456",
			mustSet:       true,
			expectedScore: 90,
		},
		{
			name:          "both references empty - matches",
			leftRef:       "",
			rightRef:      "",
			expectedScore: 100,
		},
		{
			name:          "one reference empty with mustSet=false - no match",
			leftRef:       "REF123",
			rightRef:      "",
			mustSet:       false,
			expectedScore: 90,
		},
		{
			name:          "one reference empty with mustSet=true - no match",
			leftRef:       "REF123",
			rightRef:      "",
			mustSet:       true,
			expectedScore: 90,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			left := CandidateTransaction{
				ID:             uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      tt.leftRef,
			}
			right := CandidateTransaction{
				ID:             uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           fixedTime,
				Reference:      tt.rightRef,
			}

			cfg := &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				MatchReference:     true,
				CaseInsensitive:    tt.caseInsens,
				ReferenceMustSet:   tt.mustSet,
			}

			score := ScoreToleranceConfidence(cfg, left, right)

			assert.Equal(t, tt.expectedScore, score)
		})
	}
}

func TestScoreDateLagConfidence_NilConfig(t *testing.T) {
	t.Parallel()

	left := newTestTransactionWithID(fixedTestID1)
	right := newTestTransactionWithID(fixedTestID2)

	score := ScoreDateLagConfidence(nil, left, right)

	assert.Equal(t, 0, score)
}

func TestScoreDateLagConfidence_AmountWithinTolerance(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		cfg      *DateLagConfig
		left     CandidateTransaction
		right    CandidateTransaction
		expected int
	}{
		{
			name: "amount within tolerance",
			cfg: &DateLagConfig{
				MinDays:       1,
				MaxDays:       5,
				Inclusive:     true,
				FeeTolerance:  decimal.RequireFromString("2.00"),
				MatchCurrency: true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				Amount:         decimal.RequireFromString("98.50"),
				OriginalAmount: decimal.RequireFromString("98.50"),
				Currency:       "USD",
				Date:           baseTime.AddDate(0, 0, 3),
			},
			expected: 90,
		},
		{
			name: "amount outside tolerance",
			cfg: &DateLagConfig{
				MinDays:       1,
				MaxDays:       5,
				Inclusive:     true,
				FeeTolerance:  decimal.RequireFromString("2.00"),
				MatchCurrency: true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				Amount:         decimal.RequireFromString("95.00"),
				OriginalAmount: decimal.RequireFromString("95.00"),
				Currency:       "USD",
				Date:           baseTime.AddDate(0, 0, 3),
			},
			expected: 50,
		},
		{
			name: "date outside range",
			cfg: &DateLagConfig{
				MinDays:       1,
				MaxDays:       5,
				Inclusive:     true,
				FeeTolerance:  decimal.RequireFromString("2.00"),
				MatchCurrency: true,
			},
			left: CandidateTransaction{
				ID:             uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime,
			},
			right: CandidateTransaction{
				ID:             uuid.MustParse("66666666-6666-6666-6666-666666666666"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime.AddDate(0, 0, 10),
			},
			expected: 70,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			score := ScoreDateLagConfidence(tt.cfg, tt.left, tt.right)
			assert.Equal(t, tt.expected, score)
		})
	}
}

func TestScoreDateLagConfidence_DateAtBoundary(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name          string
		dayDiff       int
		inclusive     bool
		maxDays       int
		expectedScore int
	}{
		{
			name:          "at MaxDays with Inclusive true",
			dayDiff:       5,
			inclusive:     true,
			maxDays:       5,
			expectedScore: 90,
		},
		{
			name:          "at MaxDays with Inclusive false",
			dayDiff:       5,
			inclusive:     false,
			maxDays:       5,
			expectedScore: 70, // Amount(40) + Currency(30) = 70 (date fails)
		},
		{
			name:          "one below MaxDays with Inclusive false",
			dayDiff:       4,
			inclusive:     false,
			maxDays:       5,
			expectedScore: 90,
		},
		{
			name:          "at MinDays boundary",
			dayDiff:       1,
			inclusive:     true,
			maxDays:       5,
			expectedScore: 90,
		},
		{
			name:          "below MinDays boundary",
			dayDiff:       0,
			inclusive:     true,
			maxDays:       5,
			expectedScore: 70, // Amount(40) + Currency(30) = 70 (date fails)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			left := CandidateTransaction{
				ID:             uuid.MustParse("77777777-7777-7777-7777-777777777777"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime,
			}
			right := CandidateTransaction{
				ID:             uuid.MustParse("88888888-8888-8888-8888-888888888888"),
				Amount:         decimal.RequireFromString("100.00"),
				OriginalAmount: decimal.RequireFromString("100.00"),
				Currency:       "USD",
				Date:           baseTime.AddDate(0, 0, tt.dayDiff),
			}

			cfg := &DateLagConfig{
				MinDays:       1,
				MaxDays:       tt.maxDays,
				Inclusive:     tt.inclusive,
				FeeTolerance:  decimal.RequireFromString("2.00"),
				MatchCurrency: true,
			}

			score := ScoreDateLagConfidence(cfg, left, right)

			assert.Equal(t, tt.expectedScore, score)
		})
	}
}

func TestScoreDateLagConfidence_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	left := CandidateTransaction{
		ID:             uuid.MustParse("99999999-9999-9999-9999-999999999999"),
		Amount:         decimal.RequireFromString("100.00"),
		OriginalAmount: decimal.RequireFromString("100.00"),
		Currency:       "USD",
		Date:           baseTime,
	}
	right := CandidateTransaction{
		ID:             uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
		Amount:         decimal.RequireFromString("100.00"),
		OriginalAmount: decimal.RequireFromString("100.00"),
		Currency:       "EUR",
		Date:           baseTime.AddDate(0, 0, 3),
	}

	cfg := &DateLagConfig{
		MinDays:       1,
		MaxDays:       5,
		Inclusive:     true,
		FeeTolerance:  decimal.RequireFromString("2.00"),
		MatchCurrency: true,
	}

	score := ScoreDateLagConfidence(cfg, left, right)

	// Amount(40) + Date(20) = 60 (currency failed, no reference)
	assert.Equal(t, 60, score)
}

func TestScoreDateLagConfidence_MissingCurrency(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)
	left := CandidateTransaction{
		ID:             uuid.MustParse("bbbbbbbb-cccc-dddd-eeee-ffffffffffff"),
		Amount:         decimal.RequireFromString("100.00"),
		OriginalAmount: decimal.RequireFromString("100.00"),
		Date:           baseTime,
	}
	right := CandidateTransaction{
		ID:             uuid.MustParse("11111111-2222-3333-4444-555555555556"),
		Amount:         decimal.RequireFromString("100.00"),
		OriginalAmount: decimal.RequireFromString("100.00"),
		Date:           baseTime.AddDate(0, 0, 3),
	}

	cfg := &DateLagConfig{
		MinDays:       1,
		MaxDays:       5,
		Inclusive:     true,
		FeeTolerance:  decimal.RequireFromString("2.00"),
		MatchCurrency: false,
	}

	score := ScoreDateLagConfidence(cfg, left, right)

	assert.Equal(t, 60, score)
}

func TestIsWithinTolerance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		left     string
		right    string
		absTol   string
		pctTol   string
		expected bool
	}{
		{
			name:     "exact match",
			left:     "100.00",
			right:    "100.00",
			absTol:   "0.00",
			pctTol:   "0.00",
			expected: true,
		},
		{
			name:     "within absolute tolerance",
			left:     "100.00",
			right:    "99.50",
			absTol:   "1.00",
			pctTol:   "0.00",
			expected: true,
		},
		{
			name:     "outside absolute tolerance",
			left:     "100.00",
			right:    "98.00",
			absTol:   "1.00",
			pctTol:   "0.00",
			expected: false,
		},
		{
			name:     "within percent tolerance",
			left:     "100.00",
			right:    "95.00",
			absTol:   "0.00",
			pctTol:   "0.10",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			left := decimal.RequireFromString(tt.left)
			right := decimal.RequireFromString(tt.right)
			absTol := decimal.RequireFromString(tt.absTol)
			pctTol := decimal.RequireFromString(tt.pctTol)

			result := isWithinTolerance(left, right, absTol, pctTol, TolerancePercentageBaseMax)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsWithinFeeTolerance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		left     string
		right    string
		feeTol   string
		expected bool
	}{
		{
			name:     "exact match",
			left:     "100.00",
			right:    "100.00",
			feeTol:   "0.00",
			expected: true,
		},
		{
			name:     "within fee tolerance",
			left:     "100.00",
			right:    "99.50",
			feeTol:   "1.00",
			expected: true,
		},
		{
			name:     "outside fee tolerance",
			left:     "100.00",
			right:    "98.00",
			feeTol:   "1.00",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			left := decimal.RequireFromString(tt.left)
			right := decimal.RequireFromString(tt.right)
			feeTol := decimal.RequireFromString(tt.feeTol)

			result := isWithinFeeTolerance(left, right, feeTol)

			assert.Equal(t, tt.expected, result)
		})
	}
}

var (
	fixedTestTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	fixedTestID1  = uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	fixedTestID2  = uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
)

func newTestTransactionWithID(id uuid.UUID) CandidateTransaction {
	return CandidateTransaction{
		ID:             id,
		Amount:         decimal.RequireFromString("100.00"),
		OriginalAmount: decimal.RequireFromString("100.00"),
		Currency:       "USD",
		Date:           fixedTestTime,
	}
}
