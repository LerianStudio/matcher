// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package services

import (
	"math/rand"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func defaultToleranceConfig() *ToleranceConfig {
	return &ToleranceConfig{
		MatchCurrency:      true,
		DateWindowDays:     2,
		AbsAmountTolerance: decimal.RequireFromString("0.50"),
		PercentTolerance:   decimal.RequireFromString("0.10"),
		RoundingScale:      2,
		RoundingMode:       RoundingHalfUp,
		MatchBaseAmount:    false,
		MatchBaseCurrency:  false,
	}
}

func makeTx(
	idSuffix, amount, currency string,
	date time.Time,
	amountBase *decimal.Decimal,
	currencyBase string,
) CandidateTransaction {
	id := uuid.MustParse("00000000-0000-0000-0000-0000000000" + idSuffix)

	return CandidateTransaction{
		ID:             id,
		Amount:         decimal.RequireFromString(amount),
		OriginalAmount: decimal.RequireFromString(amount),
		Currency:       currency,
		Date:           date,
		AmountBase:     amountBase,
		CurrencyBase:   currencyBase,
	}
}

type toleranceTestCase struct {
	name        string
	l           CandidateTransaction
	r           CandidateTransaction
	cfgOverride func(*ToleranceConfig)
	want        bool
}

func TestToleranceRule_Table(t *testing.T) {
	t.Parallel()

	cfg := defaultToleranceConfig()
	tests := buildToleranceTestCases()

	runToleranceTests(t, cfg, tests)
}

func buildToleranceTestCases() []toleranceTestCase {
	jan1 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	jan1Z := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	jan2 := time.Date(2026, 1, 2, 10, 0, 0, 0, time.UTC)
	jan3Z := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	jan10 := time.Date(2026, 1, 10, 10, 0, 0, 0, time.UTC)

	cases := make([]toleranceTestCase, 0, 20)
	cases = append(cases, buildAmountToleranceCases(jan1, jan2)...)
	cases = append(cases, buildCurrencyMatchingCases(jan1)...)
	cases = append(cases, buildBaseAmountCases(jan1)...)
	cases = append(cases, buildDateWindowCases(jan1Z, jan3Z, jan1, jan10)...)
	cases = append(cases, buildSpecialAmountCases(jan1, jan2)...)

	return cases
}

func buildAmountToleranceCases(jan1, jan2 time.Time) []toleranceTestCase {
	return []toleranceTestCase{
		{
			name: "amount within absolute tolerance",
			l:    makeTx("11", "10.00", "USD", jan1, nil, ""),
			r:    makeTx("12", "10.40", "USD", jan2, nil, ""),
			want: true,
		},
		{
			name: "amount outside tolerance",
			l:    makeTx("13", "10.00", "USD", jan1, nil, ""),
			r:    makeTx("14", "12.00", "USD", jan1, nil, ""),
			want: false,
		},
		{
			name: "amount at absolute tolerance",
			l:    makeTx("21", "10.00", "USD", jan1, nil, ""),
			r:    makeTx("22", "10.50", "USD", jan1, nil, ""),
			want: true,
		},
		{
			name: "percent tolerance dominates",
			l:    makeTx("19", "100.00", "USD", jan1, nil, ""),
			r:    makeTx("20", "109.00", "USD", jan1, nil, ""),
			want: true,
		},
	}
}

func buildCurrencyMatchingCases(jan1 time.Time) []toleranceTestCase {
	return []toleranceTestCase{
		{
			name: "currency mismatch fails",
			l:    makeTx("15", "10.00", "USD", jan1, nil, ""),
			r:    makeTx("16", "10.10", "EUR", jan1, nil, ""),
			want: false,
		},
		{
			name: "currency mismatch allowed when match disabled", l: makeTx("41", "10.00", "USD", jan1, nil, ""), r: makeTx("42", "10.00", "EUR", jan1, nil, ""),
			cfgOverride: func(c *ToleranceConfig) {
				c.MatchCurrency, c.AbsAmountTolerance, c.PercentTolerance = false, decimal.RequireFromString(
					"0.50",
				), decimal.Zero
			}, want: true,
		},
	}
}

func buildBaseAmountCases(jan1 time.Time) []toleranceTestCase {
	baseCfg := func(c *ToleranceConfig) {
		c.MatchBaseAmount, c.MatchBaseCurrency, c.MatchCurrency = true, true, false
		c.AbsAmountTolerance, c.PercentTolerance = decimal.RequireFromString("0.10"), decimal.Zero
	}

	return []toleranceTestCase{
		{
			name: "base matching allows currency mismatch",
			l: makeTx(
				"45",
				"10.00",
				"USD",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.00")),
				"USD",
			),
			r: makeTx(
				"46",
				"10.00",
				"EUR",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.05")),
				"USD",
			), cfgOverride: baseCfg, want: true,
		},
		{
			name: "base amount with base currency",
			l: makeTx(
				"23",
				"10.00",
				"USD",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.00")),
				"USD",
			),
			r: makeTx(
				"24",
				"10.00",
				"EUR",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.05")),
				"USD",
			), cfgOverride: baseCfg, want: true,
		},
		{
			name: "base currency mismatch",
			l: makeTx(
				"25",
				"10.00",
				"USD",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.00")),
				"USD",
			),
			r: makeTx(
				"26",
				"10.00",
				"EUR",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.10")),
				"EUR",
			), cfgOverride: baseCfg, want: false,
		},
		{
			name: "base amount missing",
			l:    makeTx("27", "10.00", "USD", jan1, nil, "USD"),
			r: makeTx(
				"28",
				"10.00",
				"EUR",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.10")),
				"USD",
			), cfgOverride: baseCfg, want: false,
		},
		{
			name: "base currency missing",
			l: makeTx(
				"29",
				"10.00",
				"USD",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.00")),
				"",
			),
			r: makeTx(
				"30",
				"10.00",
				"EUR",
				jan1,
				testutil.DecimalPtr(decimal.RequireFromString("12.10")),
				"USD",
			), cfgOverride: baseCfg, want: false,
		},
	}
}

func buildDateWindowCases(jan1Z, jan3Z, jan1, jan10 time.Time) []toleranceTestCase {
	return []toleranceTestCase{
		{
			name: "date window boundary inclusive", l: makeTx("43", "10.00", "USD", jan1Z, nil, ""), r: makeTx("44", "10.00", "USD", jan3Z, nil, ""),
			cfgOverride: func(c *ToleranceConfig) { c.DateWindowDays = 2 }, want: true,
		},
		{
			name: "date window exceeded fails",
			l:    makeTx("17", "10.00", "USD", jan1, nil, ""),
			r:    makeTx("18", "10.10", "USD", jan10, nil, ""),
			want: false,
		},
	}
}

func buildSpecialAmountCases(jan1, jan2 time.Time) []toleranceTestCase {
	return []toleranceTestCase{
		{
			name: "zero amounts",
			l:    makeTx("31", "0", "USD", jan1, nil, ""),
			r:    makeTx("32", "0", "USD", jan1, nil, ""),
			want: true,
		},
		{
			name: "negative amounts",
			l:    makeTx("33", "-10.00", "USD", jan1, nil, ""),
			r:    makeTx("34", "-10.40", "USD", jan2, nil, ""),
			want: true,
		},
	}
}

func runToleranceTests(t *testing.T, baseCfg *ToleranceConfig, tests []toleranceTestCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localCfg := *baseCfg
			if tt.cfgOverride != nil {
				tt.cfgOverride(&localCfg)
			}

			got, err := ToleranceMatch(tt.l, tt.r, &localCfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestToleranceRule_RoundingModes(t *testing.T) {
	t.Parallel()

	left := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-000000000031"),
		Amount:         decimal.RequireFromString("10.005"),
		OriginalAmount: decimal.RequireFromString("10.005"),
		Currency:       "USD",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}
	right := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-000000000032"),
		Amount:         decimal.RequireFromString("10.004"),
		OriginalAmount: decimal.RequireFromString("10.004"),
		Currency:       "USD",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}

	cases := []struct {
		name string
		cfg  *ToleranceConfig
		want bool
	}{
		{
			name: "half-up",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("0.00"),
				PercentTolerance:   decimal.RequireFromString("0.00"),
				RoundingScale:      2,
				RoundingMode:       RoundingHalfUp,
			},
			want: false,
		},
		{
			name: "bankers",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("0.00"),
				PercentTolerance:   decimal.RequireFromString("0.00"),
				RoundingScale:      2,
				RoundingMode:       RoundingBankers,
			},
			want: true,
		},
		{
			name: "floor",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("0.00"),
				PercentTolerance:   decimal.RequireFromString("0.00"),
				RoundingScale:      2,
				RoundingMode:       RoundingFloor,
			},
			want: true,
		},
		{
			name: "ceil",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("0.00"),
				PercentTolerance:   decimal.RequireFromString("0.00"),
				RoundingScale:      2,
				RoundingMode:       RoundingCeil,
			},
			want: true,
		},
		{
			name: "truncate",
			cfg: &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.RequireFromString("0.00"),
				PercentTolerance:   decimal.RequireFromString("0.00"),
				RoundingScale:      2,
				RoundingMode:       RoundingTruncate,
			},
			want: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ToleranceMatch(left, right, tt.cfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestToleranceRule_InvalidConfig(t *testing.T) {
	t.Parallel()

	left := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-000000000035"),
		Amount:         decimal.RequireFromString("10.00"),
		OriginalAmount: decimal.RequireFromString("10.00"),
		Currency:       "USD",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}
	right := CandidateTransaction{
		ID:             uuid.MustParse("00000000-0000-0000-0000-000000000036"),
		Amount:         decimal.RequireFromString("10.00"),
		OriginalAmount: decimal.RequireFromString("10.00"),
		Currency:       "USD",
		Date:           time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC),
	}

	_, err := ToleranceMatch(left, right, nil)
	require.ErrorIs(t, err, ErrToleranceConfigRequired)

	_, err = ToleranceMatch(left, right, &ToleranceConfig{DateWindowDays: -1})
	require.ErrorIs(t, err, ErrInvalidDateWindowDays)

	_, err = ToleranceMatch(
		left,
		right,
		&ToleranceConfig{DateWindowDays: 0, AbsAmountTolerance: decimal.RequireFromString("-1")},
	)
	require.ErrorIs(t, err, ErrInvalidTolerance)

	_, err = ToleranceMatch(
		left,
		right,
		&ToleranceConfig{DateWindowDays: 0, PercentTolerance: decimal.RequireFromString("-0.1")},
	)
	require.ErrorIs(t, err, ErrInvalidTolerance)

	_, err = ToleranceMatch(
		left,
		right,
		&ToleranceConfig{DateWindowDays: 0, RoundingScale: -1, RoundingMode: RoundingHalfUp},
	)
	require.ErrorIs(t, err, ErrInvalidRoundingScale)

	_, err = ToleranceMatch(
		left,
		right,
		&ToleranceConfig{DateWindowDays: 0, RoundingScale: 2, RoundingMode: RoundingMode("BAD")},
	)
	require.ErrorIs(t, err, ErrInvalidRoundingMode)
}

func TestToleranceRule_ReferenceMatching(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		leftRef        string
		rightRef       string
		matchReference bool
		caseInsens     bool
		mustSet        bool
		want           bool
	}{
		{
			name:           "reference matching disabled - always matches",
			leftRef:        "REF123",
			rightRef:       "REF456",
			matchReference: false,
			want:           true,
		},
		{
			name:           "references match exactly",
			leftRef:        "REF123",
			rightRef:       "REF123",
			matchReference: true,
			want:           true,
		},
		{
			name:           "references match case-insensitive",
			leftRef:        "REF123",
			rightRef:       "ref123",
			matchReference: true,
			caseInsens:     true,
			want:           true,
		},
		{
			name:           "references differ case-sensitive",
			leftRef:        "REF123",
			rightRef:       "ref123",
			matchReference: true,
			caseInsens:     false,
			want:           false,
		},
		{
			name:           "references differ completely",
			leftRef:        "REF123",
			rightRef:       "REF456",
			matchReference: true,
			want:           false,
		},
		{
			name:           "both references empty - matches",
			leftRef:        "",
			rightRef:       "",
			matchReference: true,
			want:           true,
		},
		{
			name:           "one reference empty with mustSet=false - no match",
			leftRef:        "REF123",
			rightRef:       "",
			matchReference: true,
			mustSet:        false,
			want:           false,
		},
		{
			name:           "one reference empty with mustSet=true - no match",
			leftRef:        "REF123",
			rightRef:       "",
			matchReference: true,
			mustSet:        true,
			want:           false,
		},
		{
			name:           "both empty with mustSet=true - no match",
			leftRef:        "",
			rightRef:       "",
			matchReference: true,
			mustSet:        true,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			left := CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000037"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           baseTime,
				Reference:      tt.leftRef,
			}
			right := CandidateTransaction{
				ID:             uuid.MustParse("00000000-0000-0000-0000-000000000038"),
				Amount:         decimal.RequireFromString("10.00"),
				OriginalAmount: decimal.RequireFromString("10.00"),
				Currency:       "USD",
				Date:           baseTime,
				Reference:      tt.rightRef,
			}

			cfg := &ToleranceConfig{
				MatchCurrency:      true,
				DateWindowDays:     0,
				AbsAmountTolerance: decimal.Zero,
				PercentTolerance:   decimal.Zero,
				RoundingScale:      2,
				RoundingMode:       RoundingHalfUp,
				MatchReference:     tt.matchReference,
				CaseInsensitive:    tt.caseInsens,
				ReferenceMustSet:   tt.mustSet,
			}

			got, err := ToleranceMatch(left, right, cfg)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestToleranceRule_Determinism_PropertyLike(t *testing.T) {
	t.Parallel()

	cfg := &ToleranceConfig{
		MatchCurrency:      true,
		DateWindowDays:     1,
		AbsAmountTolerance: decimal.RequireFromString("1.00"),
		PercentTolerance:   decimal.RequireFromString("0.01"),
		RoundingScale:      2,
		RoundingMode:       RoundingHalfUp,
	}

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 500; i++ {
		amt := decimal.NewFromInt(int64(rng.Intn(10000)))
		ts := time.Unix(int64(rng.Intn(2000000)), 0).UTC()
		leftID := testutil.MustDeterministicUUID("left-" + ts.Format(time.RFC3339Nano))
		rightID := testutil.MustDeterministicUUID("right-" + ts.Format(time.RFC3339Nano))

		l := CandidateTransaction{ID: leftID, Amount: amt, OriginalAmount: amt, Currency: "USD", Date: ts}
		r := CandidateTransaction{
			ID:             rightID,
			Amount:         amt,
			OriginalAmount: amt,
			Currency:       "USD",
			Date:           ts.Add(12 * time.Hour),
		}

		a1, err := ToleranceMatch(l, r, cfg)
		require.NoError(t, err)
		a2, err := ToleranceMatch(l, r, cfg)
		require.NoError(t, err)
		require.Equal(t, a1, a2)
	}
}

func TestToleranceCalc_ThresholdChoosesAbsWhenDominant(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("1.00")
	pctTol := decimal.RequireFromString("0.10")
	left := decimal.RequireFromString("5.00")
	right := decimal.RequireFromString("6.00")

	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMax)
	require.Equal(t, decimal.RequireFromString("1.00"), th)
}

func TestToleranceCalc_ThresholdTiePrefersAbs(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("1.00")
	pctTol := decimal.RequireFromString("0.10")
	left := decimal.RequireFromString("10")
	right := decimal.RequireFromString("10")

	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMax)
	require.True(t, th.Equal(absTol))
}

func TestToleranceCalc_ThresholdChoosesPercentWhenDominant(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.10")
	left := decimal.RequireFromString("100")
	right := decimal.RequireFromString("101")

	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMax)
	require.True(t, th.Equal(decimal.RequireFromString("10.10")))
}

func TestToleranceCalc_ThresholdWithMinBase(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.05") // 5%
	left := decimal.RequireFromString("10.00")
	right := decimal.RequireFromString("1000.00")
	// MIN base: 5% of 10 = 0.50, same as absTol, returns absTol
	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMin)
	require.True(t, th.Equal(decimal.RequireFromString("0.50")))
}

func TestToleranceCalc_ThresholdWithMaxBase(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.05") // 5%
	left := decimal.RequireFromString("10.00")
	right := decimal.RequireFromString("1000.00")
	// MAX base: 5% of 1000 = 50.00
	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMax)
	require.True(t, th.Equal(decimal.RequireFromString("50.00")))
}

func TestToleranceCalc_ThresholdWithAvgBase(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.05") // 5%
	left := decimal.RequireFromString("10.00")
	right := decimal.RequireFromString("1000.00")
	// AVG base: 5% of 505 = 25.25
	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseAvg)
	require.True(t, th.Equal(decimal.RequireFromString("25.25")))
}

func TestToleranceCalc_ThresholdWithLeftBase(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.05")
	left := decimal.RequireFromString("10.00")
	right := decimal.RequireFromString("1000.00")
	// LEFT base: 5% of 10 = 0.50
	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseLeft)
	require.True(t, th.Equal(decimal.RequireFromString("0.50")))
}

func TestToleranceCalc_ThresholdWithRightBase(t *testing.T) {
	t.Parallel()

	absTol := decimal.RequireFromString("0.50")
	pctTol := decimal.RequireFromString("0.05")
	left := decimal.RequireFromString("10.00")
	right := decimal.RequireFromString("1000.00")
	// RIGHT base: 5% of 1000 = 50.00
	th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseRight)
	require.True(t, th.Equal(decimal.RequireFromString("50.00")))
}

func TestToleranceCalc_Property_NonNegative(t *testing.T) {
	t.Parallel()

	cfg := &quick.Config{Rand: rand.New(rand.NewSource(42)), MaxCount: 1000}

	prop := func(absTolInt, pctTolInt, leftInt, rightInt int64) bool {
		absTol := decimal.NewFromInt(absTolInt).Abs()
		pctTol := decimal.NewFromInt(pctTolInt).Abs().Div(decimal.NewFromInt(1000))
		left := decimal.NewFromInt(leftInt)
		right := decimal.NewFromInt(rightInt)

		th := toleranceThreshold(absTol, pctTol, left, right, TolerancePercentageBaseMax)

		return !th.IsNegative()
	}

	require.NoError(t, quick.Check(prop, cfg))
}

func TestToleranceCalc_Property_Monotonic(t *testing.T) {
	t.Parallel()

	cfg := &quick.Config{Rand: rand.New(rand.NewSource(43)), MaxCount: 1000}

	prop := func(a1, a2, p1, p2, leftInt, rightInt int64) bool {
		abs1 := decimal.NewFromInt(a1).Abs()
		abs2 := abs1.Add(decimal.NewFromInt(a2).Abs())

		pct1 := decimal.NewFromInt(p1).Abs().Div(decimal.NewFromInt(1000))
		pct2 := pct1.Add(decimal.NewFromInt(p2).Abs().Div(decimal.NewFromInt(1000)))

		left := decimal.NewFromInt(leftInt)
		right := decimal.NewFromInt(rightInt)

		th1 := toleranceThreshold(abs1, pct1, left, right, TolerancePercentageBaseMax)
		th2 := toleranceThreshold(abs2, pct2, left, right, TolerancePercentageBaseMax)

		return th2.GreaterThanOrEqual(th1)
	}

	require.NoError(t, quick.Check(prop, cfg))
}
