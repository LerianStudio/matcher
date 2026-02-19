//go:build unit

package services

import (
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestEngineWithLimit(t *testing.T) {
	t.Parallel()

	e := NewEngineWithLimit(0)
	require.Equal(t, DefaultMaxCandidates, e.maxAllowed())

	e = NewEngineWithLimit(5)
	require.Equal(t, 5, e.maxAllowed())
}

func TestExecute1vNAndNv1(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	rule := RuleDefinition{
		ID:       uuid.New(),
		Priority: 1,
		Type:     "EXACT",
		Exact: &ExactConfig{
			MatchAmount:     true,
			MatchCurrency:   true,
			MatchDate:       true,
			DatePrecision:   DatePrecisionDay,
			MatchReference:  true,
			CaseInsensitive: true,
			MatchScore:      100,
		},
	}
	left := []CandidateTransaction{
		{
			ID:             uuid.New(),
			Amount:         decimal.RequireFromString("10"),
			OriginalAmount: decimal.RequireFromString("10"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "a",
		},
	}
	right := []CandidateTransaction{
		{
			ID:             uuid.New(),
			Amount:         decimal.RequireFromString("10"),
			OriginalAmount: decimal.RequireFromString("10"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "a",
		},
	}

	result, err := engine.Execute1vN([]RuleDefinition{rule}, left, right)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "1:N", result[0].Mode)

	result, err = engine.ExecuteNv1([]RuleDefinition{rule}, left, right)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "N:1", result[0].Mode)
}

func TestExecute1vN_AllocationFailureSkipsDirectMatch(t *testing.T) {
	t.Parallel()

	engine := NewEngine()
	base := decimal.RequireFromString("100")
	left := []CandidateTransaction{{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		AmountBase:     &base,
		Currency:       "EUR",
		CurrencyBase:   "USD",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}}
	right := []CandidateTransaction{{
		ID:             uuid.New(),
		Amount:         decimal.RequireFromString("100"),
		OriginalAmount: decimal.RequireFromString("100"),
		AmountBase:     &base,
		Currency:       "EUR",
		CurrencyBase:   "EUR",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
		Reference:      "ref",
	}}

	rule := RuleDefinition{
		ID:       uuid.New(),
		Priority: 1,
		Type:     "EXACT",
		Exact: &ExactConfig{
			MatchAmount:     true,
			MatchCurrency:   true,
			MatchDate:       true,
			DatePrecision:   DatePrecisionDay,
			MatchReference:  true,
			CaseInsensitive: true,
			MatchScore:      100,
		},
		Allocation: &AllocationConfig{
			AllowPartial:   false,
			Direction:      AllocationDirectionLeftToRight,
			ToleranceMode:  AllocationToleranceAbsolute,
			ToleranceValue: decimal.Zero,
			UseBaseAmount:  true,
		},
	}

	result, err := engine.Execute1vN([]RuleDefinition{rule}, left, right)
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestScoreHelpers_FinancialFirstScoring(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	amount := decimal.RequireFromString("10")
	base := decimal.RequireFromString("10")
	left := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         amount,
		OriginalAmount: amount,
		AmountBase:     &base,
		Currency:       "USD",
		CurrencyBase:   "USD",
		Date:           now,
		Reference:      "Ref",
	}
	right := CandidateTransaction{
		ID:             uuid.New(),
		Amount:         amount,
		OriginalAmount: amount,
		AmountBase:     &base,
		Currency:       "USD",
		CurrencyBase:   "USD",
		Date:           now,
		Reference:      "Ref",
	}

	cfgExact := &ExactConfig{
		MatchAmount:       true,
		MatchCurrency:     true,
		MatchDate:         true,
		DatePrecision:     DatePrecisionDay,
		MatchReference:    true,
		CaseInsensitive:   true,
		MatchBaseAmount:   true,
		MatchBaseCurrency: true,
	}
	score := ScoreExactConfidence(cfgExact, left, right)
	require.Equal(
		t,
		100,
		score,
		"perfect exact match should score 100 with financial-first scoring",
	)

	cfgTol := &ToleranceConfig{
		MatchCurrency:     true,
		DateWindowDays:    0,
		MatchBaseAmount:   true,
		MatchBaseCurrency: true,
	}
	score = ScoreToleranceConfidence(cfgTol, left, right)
	// When MatchReference=false (default), reference score is excluded.
	// Amount (40) + Currency (30) + Date (20) = 90
	require.Equal(
		t,
		90,
		score,
		"perfect tolerance match should score 90 when reference matching disabled",
	)

	cfgDateLag := &DateLagConfig{
		MatchCurrency: true,
		MinDays:       0,
		MaxDays:       1,
		Inclusive:     true,
		FeeTolerance:  decimal.Zero,
	}
	score = ScoreDateLagConfidence(cfgDateLag, left, right)
	// DateLag rules don't match reference, so max is 90 (amount 40 + currency 30 + date 20)
	require.Equal(t, 90, score, "perfect date lag match should score 90 (no reference matching)")
}

func TestEngine_Determinism(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000401"),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:     true,
				MatchCurrency:   true,
				MatchDate:       true,
				DatePrecision:   DatePrecisionDay,
				MatchReference:  true,
				CaseInsensitive: true,
				MatchScore:      100,
			},
		},
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000402"),
			Priority: 2,
			Type:     "TOLERANCE",
			Tolerance: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     1,
				AbsAmountTolerance: decimal.RequireFromString("1.00"),
				PercentTolerance:   decimal.RequireFromString("0.01"),
				RoundingScale:      2,
				RoundingMode:       RoundingHalfUp,
				MatchScore:         90,
			},
		},
	}

	left := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000501"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "REF-A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000502"),
			Amount:         decimal.RequireFromString("11.00"),
			OriginalAmount: decimal.RequireFromString("11.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
			Reference:      "REF-B",
		},
	}

	right := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000601"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 13, 0, 0, 0, time.UTC),
			Reference:      "ref-a",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000602"),
			Amount:         decimal.RequireFromString("11.50"),
			OriginalAmount: decimal.RequireFromString("11.50"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC),
			Reference:      "REF-C",
		},
	}

	first, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		testutil.CloneTransactions(left),
		testutil.CloneTransactions(right),
	)
	require.NoError(t, err)
	second, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		testutil.CloneTransactions(left),
		testutil.CloneTransactions(right),
	)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, first, 2)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000401"), first[0].RuleID)
	require.Equal(
		t,
		[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000501")},
		first[0].LeftIDs,
	)
	require.Equal(
		t,
		[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000601")},
		first[0].RightIDs,
	)
	require.Equal(t, "1:1", first[0].Mode)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000402"), first[1].RuleID)
	require.Equal(
		t,
		[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000502")},
		first[1].LeftIDs,
	)
	require.Equal(
		t,
		[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000602")},
		first[1].RightIDs,
	)
	require.Equal(t, "1:1", first[1].Mode)
}

func TestEngine_Determinism_PermutedInputs(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000701"),
			Priority: 2,
			Type:     "TOLERANCE",
			Tolerance: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     2,
				AbsAmountTolerance: decimal.RequireFromString("0.50"),
				PercentTolerance:   decimal.RequireFromString("0.02"),
				RoundingScale:      2,
				RoundingMode:       RoundingHalfUp,
				MatchScore:         85,
			},
		},
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000702"),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:     true,
				MatchCurrency:   true,
				MatchDate:       true,
				DatePrecision:   DatePrecisionDay,
				MatchReference:  true,
				CaseInsensitive: true,
				MatchScore:      100,
			},
		},
	}

	left := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000801"),
			Amount:         decimal.RequireFromString("20.00"),
			OriginalAmount: decimal.RequireFromString("20.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 3, 10, 0, 0, 0, time.UTC),
			Reference:      "A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000802"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "B",
		},
	}

	right := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000901"),
			Amount:         decimal.RequireFromString("20.20"),
			OriginalAmount: decimal.RequireFromString("20.20"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 3, 10, 0, 0, 0, time.UTC),
			Reference:      "C",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000902"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "b",
		},
	}

	rng := rand.New(rand.NewSource(99))
	for i := 0; i < 20; i++ {
		permutedRules := testutil.PermuteRules(rules, rng)
		permutedLeft := testutil.PermuteTransactions(left, rng)
		permutedRight := testutil.PermuteTransactions(right, rng)

		result, err := engine.Execute1v1(permutedRules, permutedLeft, permutedRight)
		require.NoError(t, err)
		require.Len(t, result, 2)
		require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000702"), result[0].RuleID)
		require.Equal(
			t,
			[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000802")},
			result[0].LeftIDs,
		)
		require.Equal(
			t,
			[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000902")},
			result[0].RightIDs,
		)
		require.Equal(t, "1:1", result[0].Mode)
		require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000701"), result[1].RuleID)
		require.Equal(
			t,
			[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000801")},
			result[1].LeftIDs,
		)
		require.Equal(
			t,
			[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000901")},
			result[1].RightIDs,
		)
		require.Equal(t, "1:1", result[1].Mode)
	}
}

func TestEngine_Determinism_TiedCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000711"),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:     true,
				MatchCurrency:   true,
				MatchDate:       true,
				DatePrecision:   DatePrecisionDay,
				MatchReference:  true,
				CaseInsensitive: true,
				MatchScore:      100,
			},
		},
	}

	left := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000821"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "A",
		},
	}

	right := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000911"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
			Reference:      "a",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000912"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
			Reference:      "a",
		},
	}

	result, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		testutil.CloneTransactions(left),
		testutil.CloneTransactions(right),
	)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(
		t,
		[]uuid.UUID{uuid.MustParse("00000000-0000-0000-0000-000000000911")},
		result[0].RightIDs,
	)
	require.Equal(t, "1:1", result[0].Mode)
}

func TestEngine_PriorityConsumesCandidate(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000715"),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:     true,
				MatchCurrency:   true,
				MatchDate:       true,
				DatePrecision:   DatePrecisionDay,
				MatchReference:  true,
				CaseInsensitive: true,
				MatchScore:      100,
			},
		},
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000716"),
			Priority: 2,
			Type:     "TOLERANCE",
			Tolerance: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("1.00"),
				PercentTolerance:   decimal.RequireFromString("0.01"),
				RoundingScale:      2,
				RoundingMode:       RoundingHalfUp,
				MatchScore:         80,
			},
		},
	}

	left := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000823"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
			Reference:      "REF",
		},
	}

	right := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000913"),
			Amount:         decimal.RequireFromString("10.00"),
			OriginalAmount: decimal.RequireFromString("10.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC),
			Reference:      "ref",
		},
	}

	result, err := engine.Execute1v1(
		testutil.CloneRules(rules),
		testutil.CloneTransactions(left),
		testutil.CloneTransactions(right),
	)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000715"), result[0].RuleID)
}

func TestEngine_RejectsOversizedCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	left := make([]CandidateTransaction, DefaultMaxCandidates+1)
	left[0] = CandidateTransaction{ID: uuid.MustParse("00000000-0000-0000-0000-000000000951")}
	_, err := engine.Execute1v1(nil, left, nil)
	require.ErrorIs(t, err, ErrCandidateSetTooLarge)
}

func TestEngine_RejectsOversizedRules(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := make([]RuleDefinition, DefaultMaxCandidates+1)
	_, err := engine.Execute1v1(rules, nil, nil)
	require.ErrorIs(t, err, ErrCandidateSetTooLarge)
}

func TestEngine_RejectsOversizedRight(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	right := make([]CandidateTransaction, DefaultMaxCandidates+1)
	right[0] = CandidateTransaction{ID: uuid.MustParse("00000000-0000-0000-0000-000000000953")}
	_, err := engine.Execute1v1(nil, nil, right)
	require.ErrorIs(t, err, ErrCandidateSetTooLarge)
}

func TestEngine_AcceptsMaxCandidates(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	left := make([]CandidateTransaction, DefaultMaxCandidates)
	left[0] = CandidateTransaction{ID: uuid.MustParse("00000000-0000-0000-0000-000000000952")}
	_, err := engine.Execute1v1(nil, left, nil)
	require.NoError(t, err)
	require.Len(t, left, DefaultMaxCandidates)
}

func TestEngine_NilReceiver(t *testing.T) {
	t.Parallel()

	var engine *Engine

	_, err := engine.Execute1v1(nil, nil, nil)
	require.EqualError(t, err, "engine is nil")
}

func TestEngine_NilRuleConfig(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	rules := []RuleDefinition{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000954"), Priority: 1, Type: "EXACT"},
	}
	left := []CandidateTransaction{{ID: uuid.MustParse("00000000-0000-0000-0000-000000000955")}}
	right := []CandidateTransaction{{ID: uuid.MustParse("00000000-0000-0000-0000-000000000956")}}
	_, err := engine.Execute1v1(rules, left, right)
	require.ErrorIs(t, err, ErrNilRuleConfig)
}

func TestEngine_Execute1v1_SingleRuleMultiplePairs(t *testing.T) {
	t.Parallel()

	engine := NewEngine()

	// Single rule that matches on amount, currency, date (day precision)
	rules := []RuleDefinition{
		{
			ID:       uuid.MustParse("00000000-0000-0000-0000-000000000960"),
			Priority: 1,
			Type:     "EXACT",
			Exact: &ExactConfig{
				MatchAmount:     true,
				MatchCurrency:   true,
				MatchDate:       true,
				DatePrecision:   DatePrecisionDay,
				MatchReference:  true,
				CaseInsensitive: true,
				MatchScore:      100,
			},
		},
	}

	// Three left transactions with distinct references
	left := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000961"),
			Amount:         decimal.RequireFromString("100.00"),
			OriginalAmount: decimal.RequireFromString("100.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC),
			Reference:      "PAIR-A",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000962"),
			Amount:         decimal.RequireFromString("200.00"),
			OriginalAmount: decimal.RequireFromString("200.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 11, 10, 0, 0, 0, time.UTC),
			Reference:      "PAIR-B",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000963"),
			Amount:         decimal.RequireFromString("300.00"),
			OriginalAmount: decimal.RequireFromString("300.00"),
			Currency:       "EUR",
			Date:           time.Date(2026, 1, 12, 10, 0, 0, 0, time.UTC),
			Reference:      "PAIR-C",
		},
	}

	// Three right transactions matching each left (case insensitive references)
	right := []CandidateTransaction{
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000971"),
			Amount:         decimal.RequireFromString("100.00"),
			OriginalAmount: decimal.RequireFromString("100.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 10, 14, 0, 0, 0, time.UTC),
			Reference:      "pair-a",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000972"),
			Amount:         decimal.RequireFromString("200.00"),
			OriginalAmount: decimal.RequireFromString("200.00"),
			Currency:       "USD",
			Date:           time.Date(2026, 1, 11, 14, 0, 0, 0, time.UTC),
			Reference:      "pair-b",
		},
		{
			ID:             uuid.MustParse("00000000-0000-0000-0000-000000000973"),
			Amount:         decimal.RequireFromString("300.00"),
			OriginalAmount: decimal.RequireFromString("300.00"),
			Currency:       "EUR",
			Date:           time.Date(2026, 1, 12, 14, 0, 0, 0, time.UTC),
			Reference:      "pair-c",
		},
	}

	result, err := engine.Execute1v1(rules, left, right)
	require.NoError(t, err)

	// Must produce exactly 3 matches from the single rule
	require.Len(t, result, 3, "expected 3 matches from single rule with 3 valid pairs")

	// All matches should use the same rule
	for _, proposal := range result {
		require.Equal(t, uuid.MustParse("00000000-0000-0000-0000-000000000960"), proposal.RuleID)
		require.Equal(t, "1:1", proposal.Mode)
		require.Equal(t, 100, proposal.Score)
	}

	// Verify no transaction ID is reused (each left and right ID appears exactly once)
	usedLeft := make(map[uuid.UUID]bool)
	usedRight := make(map[uuid.UUID]bool)

	for _, proposal := range result {
		for _, lid := range proposal.LeftIDs {
			require.False(t, usedLeft[lid], "left ID %s reused across matches", lid)
			usedLeft[lid] = true
		}

		for _, rid := range proposal.RightIDs {
			require.False(t, usedRight[rid], "right ID %s reused across matches", rid)
			usedRight[rid] = true
		}
	}

	// Verify all 6 transactions were consumed
	require.Len(t, usedLeft, 3, "all 3 left transactions should be matched")
	require.Len(t, usedRight, 3, "all 3 right transactions should be matched")
}
