// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/entities"
)

func TestAdjustmentType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		adjType  entities.AdjustmentType
		expected bool
	}{
		{"BANK_FEE is valid", entities.AdjustmentTypeBankFee, true},
		{"FX_DIFFERENCE is valid", entities.AdjustmentTypeFXDifference, true},
		{"ROUNDING is valid", entities.AdjustmentTypeRounding, true},
		{"WRITE_OFF is valid", entities.AdjustmentTypeWriteOff, true},
		{"MISCELLANEOUS is valid", entities.AdjustmentTypeMiscellaneous, true},
		{"empty string is invalid", entities.AdjustmentType(""), false},
		{"unknown type is invalid", entities.AdjustmentType("UNKNOWN"), false},
		{"lowercase is invalid", entities.AdjustmentType("bank_fee"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.adjType.IsValid())
		})
	}
}

func TestAdjustmentType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		adjType  entities.AdjustmentType
		expected string
	}{
		{entities.AdjustmentTypeBankFee, "BANK_FEE"},
		{entities.AdjustmentTypeFXDifference, "FX_DIFFERENCE"},
		{entities.AdjustmentTypeRounding, "ROUNDING"},
		{entities.AdjustmentTypeWriteOff, "WRITE_OFF"},
		{entities.AdjustmentTypeMiscellaneous, "MISCELLANEOUS"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.adjType.String())
		})
	}
}

func TestNewAdjustment_Success(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	amount := decimal.NewFromFloat(10.50)

	adjustment, err := entities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		entities.AdjustmentTypeBankFee,
		entities.AdjustmentDirectionDebit,
		amount,
		"USD",
		"Bank wire fee adjustment",
		"Variance due to bank processing fee",
		"user@example.com",
	)

	require.NoError(t, err)
	require.NotNil(t, adjustment)
	assert.NotEqual(t, uuid.Nil, adjustment.ID)
	assert.Equal(t, contextID, adjustment.ContextID)
	assert.Equal(t, &matchGroupID, adjustment.MatchGroupID)
	assert.Nil(t, adjustment.TransactionID)
	assert.Equal(t, entities.AdjustmentTypeBankFee, adjustment.Type)
	assert.True(t, amount.Equal(adjustment.Amount))
	assert.Equal(t, "USD", adjustment.Currency)
	assert.Equal(t, "Bank wire fee adjustment", adjustment.Description)
	assert.Equal(t, "Variance due to bank processing fee", adjustment.Reason)
	assert.Equal(t, "user@example.com", adjustment.CreatedBy)
	assert.False(t, adjustment.CreatedAt.IsZero())
	assert.False(t, adjustment.UpdatedAt.IsZero())
	assert.Equal(t, adjustment.CreatedAt, adjustment.UpdatedAt)
}

func TestNewAdjustment_WithTransactionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	transactionID := uuid.New()

	adjustment, err := entities.NewAdjustment(
		ctx,
		contextID,
		nil,
		&transactionID,
		entities.AdjustmentTypeRounding,
		entities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(0.01),
		"EUR",
		"Rounding adjustment",
		"Sub-cent rounding",
		"system",
	)

	require.NoError(t, err)
	require.NotNil(t, adjustment)
	assert.Nil(t, adjustment.MatchGroupID)
	assert.Equal(t, &transactionID, adjustment.TransactionID)
}

func TestNewAdjustment_WithBothIDs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	transactionID := uuid.New()

	adjustment, err := entities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		&transactionID,
		entities.AdjustmentTypeFXDifference,
		entities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(5.25),
		"GBP",
		"FX adjustment",
		"Currency conversion variance",
		"fx-service",
	)

	require.NoError(t, err)
	require.NotNil(t, adjustment)
	assert.Equal(t, &matchGroupID, adjustment.MatchGroupID)
	assert.Equal(t, &transactionID, adjustment.TransactionID)
}

type adjustmentValidationTestCase struct {
	name          string
	contextID     uuid.UUID
	matchGroupID  *uuid.UUID
	transactionID *uuid.UUID
	adjType       entities.AdjustmentType
	direction     entities.AdjustmentDirection
	amount        decimal.Decimal
	currency      string
	description   string
	reason        string
	createdBy     string
	errContains   string
}

func TestNewAdjustment_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	validAmount := decimal.NewFromFloat(10.0)

	tests := buildAdjustmentValidationCases(contextID, matchGroupID, validAmount)

	runAdjustmentValidationTests(ctx, t, tests)
}

func buildAdjustmentValidationCases(
	contextID, matchGroupID uuid.UUID,
	validAmount decimal.Decimal,
) []adjustmentValidationTestCase {
	mgID := &matchGroupID
	cases := make([]adjustmentValidationTestCase, 0, 10)

	cases = append(cases, buildIDValidationCases(contextID, mgID, validAmount)...)
	cases = append(cases, buildTypeAndDirectionCases(contextID, mgID, validAmount)...)
	cases = append(cases, buildAmountCases(contextID, mgID, validAmount)...)
	cases = append(cases, buildStringFieldCases(contextID, mgID, validAmount)...)

	return cases
}

func buildIDValidationCases(
	contextID uuid.UUID,
	mgID *uuid.UUID,
	validAmount decimal.Decimal,
) []adjustmentValidationTestCase {
	return []adjustmentValidationTestCase{
		{
			name: "nil context id", contextID: uuid.Nil, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "context id",
		},
		{
			name: "no match group or transaction id", contextID: contextID, matchGroupID: nil, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "target",
		},
	}
}

func buildTypeAndDirectionCases(
	contextID uuid.UUID,
	mgID *uuid.UUID,
	validAmount decimal.Decimal,
) []adjustmentValidationTestCase {
	return []adjustmentValidationTestCase{
		{
			name: "invalid adjustment type", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentType(
				"INVALID",
			), direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "type",
		},
		{
			name: "invalid direction", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirection("INVALID"),
			amount: validAmount, currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "direction",
		},
	}
}

func buildAmountCases(
	contextID uuid.UUID,
	mgID *uuid.UUID,
	_ decimal.Decimal,
) []adjustmentValidationTestCase {
	return []adjustmentValidationTestCase{
		{
			name: "zero amount", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: decimal.Zero, currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "amount",
		},
		{
			name: "negative amount", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: decimal.NewFromFloat(
				-10.0,
			), currency: "USD", description: "desc", reason: "reason",
			createdBy: "user", errContains: "amount",
		},
	}
}

func buildStringFieldCases(
	contextID uuid.UUID,
	mgID *uuid.UUID,
	validAmount decimal.Decimal,
) []adjustmentValidationTestCase {
	return []adjustmentValidationTestCase{
		{
			name: "empty currency", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "", description: "desc", reason: "reason",
			createdBy: "user", errContains: "currency",
		},
		{
			name: "empty description", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "", reason: "reason",
			createdBy: "user", errContains: "description",
		},
		{
			name: "empty reason", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "desc", reason: "",
			createdBy: "user", errContains: "reason",
		},
		{
			name: "empty created by", contextID: contextID, matchGroupID: mgID, transactionID: nil,
			adjType: entities.AdjustmentTypeBankFee, direction: entities.AdjustmentDirectionDebit,
			amount: validAmount, currency: "USD", description: "desc", reason: "reason",
			createdBy: "", errContains: "created by",
		},
	}
}

func runAdjustmentValidationTests(
	ctx context.Context,
	t *testing.T,
	tests []adjustmentValidationTestCase,
) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			adjustment, err := entities.NewAdjustment(
				ctx,
				tt.contextID,
				tt.matchGroupID,
				tt.transactionID,
				tt.adjType,
				tt.direction,
				tt.amount,
				tt.currency,
				tt.description,
				tt.reason,
				tt.createdBy,
			)

			require.Error(t, err)
			assert.Nil(t, adjustment)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

func TestNewAdjustment_AllTypes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	types := []entities.AdjustmentType{
		entities.AdjustmentTypeBankFee,
		entities.AdjustmentTypeFXDifference,
		entities.AdjustmentTypeRounding,
		entities.AdjustmentTypeWriteOff,
		entities.AdjustmentTypeMiscellaneous,
	}

	for _, adjType := range types {
		t.Run(string(adjType), func(t *testing.T) {
			t.Parallel()

			adjustment, err := entities.NewAdjustment(
				ctx,
				contextID,
				&matchGroupID,
				nil,
				adjType,
				entities.AdjustmentDirectionDebit,
				decimal.NewFromFloat(1.0),
				"USD",
				"Test adjustment",
				"Test reason",
				"user",
			)

			require.NoError(t, err)
			require.NotNil(t, adjustment)
			assert.Equal(t, adjType, adjustment.Type)
		})
	}
}

func TestNewAdjustment_EmptyCreatedBy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()

	adjustment, err := entities.NewAdjustment(
		ctx,
		contextID,
		&matchGroupID,
		nil,
		entities.AdjustmentTypeBankFee,
		entities.AdjustmentDirectionDebit,
		decimal.NewFromFloat(10.0),
		"USD",
		"desc",
		"reason",
		"",
	)

	require.Error(t, err)
	require.Nil(t, adjustment)
	assert.Contains(t, err.Error(), "created by")
}

func TestAdjustmentDirection_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		direction entities.AdjustmentDirection
		expected  bool
	}{
		{"DEBIT is valid", entities.AdjustmentDirectionDebit, true},
		{"CREDIT is valid", entities.AdjustmentDirectionCredit, true},
		{"empty string is invalid", entities.AdjustmentDirection(""), false},
		{"unknown direction is invalid", entities.AdjustmentDirection("UNKNOWN"), false},
		{"lowercase is invalid", entities.AdjustmentDirection("debit"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.direction.IsValid())
		})
	}
}

func TestAdjustmentDirection_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		direction entities.AdjustmentDirection
		expected  string
	}{
		{entities.AdjustmentDirectionDebit, "DEBIT"},
		{entities.AdjustmentDirectionCredit, "CREDIT"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.direction.String())
		})
	}
}

func TestAdjustment_SignedAmount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := uuid.New()
	matchGroupID := uuid.New()
	amount := decimal.NewFromFloat(100.50)

	t.Run("debit returns positive amount", func(t *testing.T) {
		t.Parallel()

		adjustment, err := entities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			entities.AdjustmentTypeBankFee,
			entities.AdjustmentDirectionDebit,
			amount,
			"USD",
			"Debit adjustment",
			"Test reason",
			"user",
		)
		require.NoError(t, err)

		signed := adjustment.SignedAmount()
		assert.True(t, signed.IsPositive())
		assert.True(t, signed.Equal(amount))
	})

	t.Run("credit returns negative amount", func(t *testing.T) {
		t.Parallel()

		adjustment, err := entities.NewAdjustment(
			ctx,
			contextID,
			&matchGroupID,
			nil,
			entities.AdjustmentTypeBankFee,
			entities.AdjustmentDirectionCredit,
			amount,
			"USD",
			"Credit adjustment",
			"Test reason",
			"user",
		)
		require.NoError(t, err)

		signed := adjustment.SignedAmount()
		assert.True(t, signed.IsNegative())
		assert.True(t, signed.Equal(amount.Neg()))
	})
}
