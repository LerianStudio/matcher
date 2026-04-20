//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type feeVarianceTestInputs struct {
	contextID               uuid.UUID
	runID                   uuid.UUID
	matchGroupID            uuid.UUID
	transactionID           uuid.UUID
	feeScheduleID           uuid.UUID
	feeScheduleNameSnapshot string
	currency                string
	expected                decimal.Decimal
	actual                  decimal.Decimal
	tolAbs                  decimal.Decimal
	tolPct                  decimal.Decimal
	varianceType            string
}

func validFeeVarianceInputs() feeVarianceTestInputs {
	return feeVarianceTestInputs{
		contextID:               uuid.New(),
		runID:                   uuid.New(),
		matchGroupID:            uuid.New(),
		transactionID:           uuid.New(),
		feeScheduleID:           uuid.New(),
		feeScheduleNameSnapshot: "Visa Domestic",
		currency:                "USD",
		expected:                decimal.NewFromFloat(100.00),
		actual:                  decimal.NewFromFloat(95.00),
		tolAbs:                  decimal.NewFromFloat(10.00),
		tolPct:                  decimal.NewFromFloat(0.05),
		varianceType:            "within_tolerance",
	}
}

func TestNewFeeVariance(t *testing.T) {
	t.Parallel()

	inputs := validFeeVarianceInputs()

	t.Run("valid inputs creates FeeVariance successfully", func(t *testing.T) {
		t.Parallel()

		fv, err := NewFeeVariance(
			context.Background(),
			inputs.contextID,
			inputs.runID,
			inputs.matchGroupID,
			inputs.transactionID,
			inputs.feeScheduleID,
			inputs.feeScheduleNameSnapshot,
			inputs.currency,
			inputs.expected,
			inputs.actual,
			inputs.tolAbs,
			inputs.tolPct,
			inputs.varianceType,
		)

		require.NoError(t, err)
		require.NotNil(t, fv)
		assertFeeVarianceFields(t, fv, inputs)
	})

	t.Run("zero fees are valid", func(t *testing.T) {
		t.Parallel()

		fv, err := NewFeeVariance(
			context.Background(),
			inputs.contextID,
			inputs.runID,
			inputs.matchGroupID,
			inputs.transactionID,
			inputs.feeScheduleID,
			inputs.feeScheduleNameSnapshot,
			inputs.currency,
			decimal.Zero,
			decimal.Zero,
			decimal.Zero,
			decimal.Zero,
			inputs.varianceType,
		)

		require.NoError(t, err)
		require.NotNil(t, fv)
		assert.True(t, fv.ExpectedFee.IsZero())
		assert.True(t, fv.ActualFee.IsZero())
	})

	runFeeVarianceValidationTests(t, inputs)
}

func assertFeeVarianceFields(t *testing.T, fv *FeeVariance, inputs feeVarianceTestInputs) {
	t.Helper()

	assert.NotEqual(t, uuid.Nil, fv.ID)
	assert.Equal(t, inputs.contextID, fv.ContextID)
	assert.Equal(t, inputs.runID, fv.RunID)
	assert.Equal(t, inputs.matchGroupID, fv.MatchGroupID)
	assert.Equal(t, inputs.transactionID, fv.TransactionID)
	assert.Equal(t, inputs.feeScheduleID, fv.FeeScheduleID)
	assert.Equal(t, inputs.feeScheduleNameSnapshot, fv.FeeScheduleNameSnapshot)
	assert.Equal(t, inputs.currency, fv.Currency)
	assert.True(t, inputs.expected.Equal(fv.ExpectedFee))
	assert.True(t, inputs.actual.Equal(fv.ActualFee))
	// Delta is computed internally as |expected - actual|
	expectedDelta := inputs.expected.Sub(inputs.actual).Abs()
	assert.True(t, expectedDelta.Equal(fv.Delta))
	assert.True(t, inputs.tolAbs.Equal(fv.ToleranceAbs))
	assert.True(t, inputs.tolPct.Equal(fv.TolerancePct))
	assert.Equal(t, inputs.varianceType, fv.VarianceType)
	assert.False(t, fv.CreatedAt.IsZero())
	assert.False(t, fv.UpdatedAt.IsZero())
}

func runFeeVarianceValidationTests(t *testing.T, valid feeVarianceTestInputs) {
	t.Helper()

	tests := buildFeeVarianceValidationCases(valid)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fv, err := NewFeeVariance(
				context.Background(),
				tc.inputs.contextID, tc.inputs.runID, tc.inputs.matchGroupID,
				tc.inputs.transactionID, tc.inputs.feeScheduleID,
				tc.inputs.feeScheduleNameSnapshot,
				tc.inputs.currency,
				tc.inputs.expected, tc.inputs.actual,
				tc.inputs.tolAbs, tc.inputs.tolPct,
				tc.inputs.varianceType,
			)

			require.Error(t, err)
			assert.Nil(t, fv)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func buildFeeVarianceValidationCases(valid feeVarianceTestInputs) []struct {
	name        string
	inputs      feeVarianceTestInputs
	errContains string
} {
	return []struct {
		name        string
		inputs      feeVarianceTestInputs
		errContains string
	}{
		{
			name:        "nil context ID returns error",
			inputs:      withContextID(valid, uuid.Nil),
			errContains: "context id",
		},
		{
			name:        "nil run ID returns error",
			inputs:      withRunID(valid, uuid.Nil),
			errContains: "run id",
		},
		{
			name:        "nil match group ID returns error",
			inputs:      withMatchGroupID(valid, uuid.Nil),
			errContains: "match group id",
		},
		{
			name:        "nil transaction ID returns error",
			inputs:      withTransactionID(valid, uuid.Nil),
			errContains: "transaction id",
		},
		{
			name:        "nil fee schedule ID returns error",
			inputs:      withFeeScheduleID(valid, uuid.Nil),
			errContains: "fee schedule id",
		},
		{
			name:        "empty fee schedule name snapshot returns error",
			inputs:      withFeeScheduleNameSnapshot(valid, ""),
			errContains: "fee schedule name snapshot",
		},
		{
			name:        "empty currency returns error",
			inputs:      withCurrency(valid, ""),
			errContains: "currency",
		},
		{
			name:        "empty variance type returns error",
			inputs:      withVarianceType(valid, ""),
			errContains: "variance type",
		},
		{
			name:        "negative expected fee returns error",
			inputs:      withExpected(valid, decimal.NewFromFloat(-100.00)),
			errContains: "amounts",
		},
		{
			name:        "negative actual fee returns error",
			inputs:      withActual(valid, decimal.NewFromFloat(-50.00)),
			errContains: "amounts",
		},
		{
			name: "both negative fees returns error",
			inputs: withExpectedActual(
				valid,
				decimal.NewFromFloat(-100.00),
				decimal.NewFromFloat(-50.00),
			),
			errContains: "amounts",
		},
		{
			name:        "negative absolute tolerance returns error",
			inputs:      withTolAbs(valid, decimal.NewFromFloat(-1.00)),
			errContains: "tolerances",
		},
		{
			name:        "negative percentage tolerance returns error",
			inputs:      withTolPct(valid, decimal.NewFromFloat(-0.05)),
			errContains: "tolerances",
		},
		{
			name: "both negative tolerances returns error",
			inputs: withTolerances(
				valid,
				decimal.NewFromFloat(-1.00),
				decimal.NewFromFloat(-0.05),
			),
			errContains: "tolerances",
		},
	}
}

func withContextID(inputs feeVarianceTestInputs, id uuid.UUID) feeVarianceTestInputs {
	inputs.contextID = id
	return inputs
}

func withRunID(inputs feeVarianceTestInputs, id uuid.UUID) feeVarianceTestInputs {
	inputs.runID = id
	return inputs
}

func withMatchGroupID(inputs feeVarianceTestInputs, id uuid.UUID) feeVarianceTestInputs {
	inputs.matchGroupID = id
	return inputs
}

func withTransactionID(inputs feeVarianceTestInputs, id uuid.UUID) feeVarianceTestInputs {
	inputs.transactionID = id
	return inputs
}

func withFeeScheduleID(inputs feeVarianceTestInputs, id uuid.UUID) feeVarianceTestInputs {
	inputs.feeScheduleID = id
	return inputs
}

func withCurrency(inputs feeVarianceTestInputs, currency string) feeVarianceTestInputs {
	inputs.currency = currency
	return inputs
}

func withFeeScheduleNameSnapshot(inputs feeVarianceTestInputs, snapshot string) feeVarianceTestInputs {
	inputs.feeScheduleNameSnapshot = snapshot
	return inputs
}

func withVarianceType(inputs feeVarianceTestInputs, varianceType string) feeVarianceTestInputs {
	inputs.varianceType = varianceType
	return inputs
}

func withExpected(inputs feeVarianceTestInputs, expected decimal.Decimal) feeVarianceTestInputs {
	inputs.expected = expected
	return inputs
}

func withActual(inputs feeVarianceTestInputs, actual decimal.Decimal) feeVarianceTestInputs {
	inputs.actual = actual
	return inputs
}

func withExpectedActual(
	inputs feeVarianceTestInputs,
	expected, actual decimal.Decimal,
) feeVarianceTestInputs {
	inputs.expected = expected
	inputs.actual = actual

	return inputs
}

func withTolAbs(inputs feeVarianceTestInputs, tolAbs decimal.Decimal) feeVarianceTestInputs {
	inputs.tolAbs = tolAbs
	return inputs
}

func withTolPct(inputs feeVarianceTestInputs, tolPct decimal.Decimal) feeVarianceTestInputs {
	inputs.tolPct = tolPct
	return inputs
}

func withTolerances(
	inputs feeVarianceTestInputs,
	tolAbs, tolPct decimal.Decimal,
) feeVarianceTestInputs {
	inputs.tolAbs = tolAbs
	inputs.tolPct = tolPct

	return inputs
}

func TestNewFeeVarianceGeneratesUniqueIDs(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	runID := uuid.New()
	matchGroupID := uuid.New()
	transactionID := uuid.New()
	feeScheduleID := uuid.New()

	fv1, err := NewFeeVariance(
		context.Background(),
		contextID, runID, matchGroupID, transactionID, feeScheduleID,
		"Visa Domestic",
		"USD",
		decimal.NewFromFloat(100.00), decimal.NewFromFloat(95.00),
		decimal.NewFromFloat(10.00), decimal.NewFromFloat(0.05),
		"within_tolerance",
	)
	require.NoError(t, err)

	fv2, err := NewFeeVariance(
		context.Background(),
		contextID, runID, matchGroupID, transactionID, feeScheduleID,
		"Visa Domestic",
		"USD",
		decimal.NewFromFloat(100.00), decimal.NewFromFloat(95.00),
		decimal.NewFromFloat(10.00), decimal.NewFromFloat(0.05),
		"within_tolerance",
	)
	require.NoError(t, err)

	assert.NotEqual(t, fv1.ID, fv2.ID)
}

func TestNewFeeVarianceTimestamps(t *testing.T) {
	t.Parallel()

	fv, err := NewFeeVariance(
		context.Background(),
		uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(),
		"Visa Domestic",
		"USD",
		decimal.NewFromFloat(100.00), decimal.NewFromFloat(95.00),
		decimal.NewFromFloat(10.00), decimal.NewFromFloat(0.05),
		"within_tolerance",
	)
	require.NoError(t, err)

	assert.Equal(t, fv.CreatedAt, fv.UpdatedAt)
	assert.Equal(t, "UTC", fv.CreatedAt.Location().String())
}
