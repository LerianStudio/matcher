// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestResolutionExecutor_InterfaceCompliance(t *testing.T) {
	t.Parallel()

	var _ ResolutionExecutor = (*mockResolutionExecutor)(nil)
}

type mockResolutionExecutor struct {
	forceMatchErr   error
	adjustEntryErr  error
	forceMatchCalls []forceMatchCall
	adjustCalls     []adjustCall
}

type forceMatchCall struct {
	exceptionID    uuid.UUID
	notes          string
	overrideReason value_objects.OverrideReason
}

type adjustCall struct {
	exceptionID uuid.UUID
	input       AdjustmentInput
}

func (mock *mockResolutionExecutor) ForceMatch(
	_ context.Context,
	exceptionID uuid.UUID,
	notes string,
	overrideReason value_objects.OverrideReason,
) error {
	mock.forceMatchCalls = append(mock.forceMatchCalls, forceMatchCall{
		exceptionID:    exceptionID,
		notes:          notes,
		overrideReason: overrideReason,
	})

	return mock.forceMatchErr
}

func (mock *mockResolutionExecutor) AdjustEntry(
	_ context.Context,
	exceptionID uuid.UUID,
	input AdjustmentInput,
) error {
	mock.adjustCalls = append(mock.adjustCalls, adjustCall{
		exceptionID: exceptionID,
		input:       input,
	})

	return mock.adjustEntryErr
}

func TestResolutionExecutor_MockImplementation(t *testing.T) {
	t.Parallel()

	t.Run("force match succeeds", func(t *testing.T) {
		t.Parallel()

		executor := &mockResolutionExecutor{}
		ctx := t.Context()

		exceptionID := uuid.New()
		notes := "Approved by manager"
		reason := value_objects.OverrideReasonOpsApproval

		err := executor.ForceMatch(ctx, exceptionID, notes, reason)

		assert.NoError(t, err)
		assert.Len(t, executor.forceMatchCalls, 1)
		assert.Equal(t, exceptionID, executor.forceMatchCalls[0].exceptionID)
		assert.Equal(t, notes, executor.forceMatchCalls[0].notes)
		assert.Equal(t, reason, executor.forceMatchCalls[0].overrideReason)
	})

	t.Run("force match returns error", func(t *testing.T) {
		t.Parallel()

		executor := &mockResolutionExecutor{forceMatchErr: assert.AnError}
		ctx := t.Context()

		err := executor.ForceMatch(ctx, uuid.New(), "", value_objects.OverrideReasonPolicyException)

		require.Error(t, err)
	})

	t.Run("adjust entry succeeds", func(t *testing.T) {
		t.Parallel()

		executor := &mockResolutionExecutor{}
		ctx := t.Context()

		exceptionID := uuid.New()
		input := AdjustmentInput{
			Amount:      decimal.NewFromFloat(100.50),
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Correcting amount",
		}

		err := executor.AdjustEntry(ctx, exceptionID, input)

		assert.NoError(t, err)
		assert.Len(t, executor.adjustCalls, 1)
		assert.Equal(t, exceptionID, executor.adjustCalls[0].exceptionID)
		assert.True(t, input.Amount.Equal(executor.adjustCalls[0].input.Amount))
	})

	t.Run("adjust entry returns error", func(t *testing.T) {
		t.Parallel()

		executor := &mockResolutionExecutor{adjustEntryErr: assert.AnError}
		ctx := t.Context()

		err := executor.AdjustEntry(ctx, uuid.New(), AdjustmentInput{})

		require.Error(t, err)
	})
}

func TestAdjustmentInput_Fields(t *testing.T) {
	t.Parallel()

	t.Run("creates input with all fields", func(t *testing.T) {
		t.Parallel()

		effectiveAt := time.Now()
		amount := decimal.NewFromFloat(250.75)

		input := AdjustmentInput{
			Amount:      amount,
			Currency:    "EUR",
			EffectiveAt: effectiveAt,
			Reason:      value_objects.AdjustmentReasonCurrencyCorrection,
			Notes:       "Currency mismatch fix",
		}

		assert.True(t, amount.Equal(input.Amount))
		assert.Equal(t, "EUR", input.Currency)
		assert.Equal(t, effectiveAt, input.EffectiveAt)
		assert.Equal(t, value_objects.AdjustmentReasonCurrencyCorrection, input.Reason)
		assert.Equal(t, "Currency mismatch fix", input.Notes)
	})

	t.Run("creates input with zero amount", func(t *testing.T) {
		t.Parallel()

		input := AdjustmentInput{
			Amount:   decimal.Zero,
			Currency: "BRL",
			Reason:   value_objects.AdjustmentReasonOther,
		}

		assert.True(t, decimal.Zero.Equal(input.Amount))
		assert.Equal(t, "BRL", input.Currency)
		assert.Equal(t, value_objects.AdjustmentReasonOther, input.Reason)
	})

	t.Run("creates input with negative amount", func(t *testing.T) {
		t.Parallel()

		amount := decimal.NewFromFloat(-50.00)

		input := AdjustmentInput{
			Amount:   amount,
			Currency: "USD",
			Reason:   value_objects.AdjustmentReasonAmountCorrection,
		}

		assert.True(t, amount.Equal(input.Amount))
		assert.True(t, input.Amount.IsNegative())
		assert.Equal(t, "USD", input.Currency)
		assert.Equal(t, value_objects.AdjustmentReasonAmountCorrection, input.Reason)
	})

	t.Run("creates input with date correction reason", func(t *testing.T) {
		t.Parallel()

		amount := decimal.NewFromInt(1000)
		effectiveAt := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)

		input := AdjustmentInput{
			Amount:      amount,
			Currency:    "GBP",
			EffectiveAt: effectiveAt,
			Reason:      value_objects.AdjustmentReasonDateCorrection,
			Notes:       "Backdating transaction",
		}

		assert.True(t, amount.Equal(input.Amount))
		assert.Equal(t, "GBP", input.Currency)
		assert.Equal(t, effectiveAt, input.EffectiveAt)
		assert.Equal(t, value_objects.AdjustmentReasonDateCorrection, input.Reason)
		assert.Equal(t, "Backdating transaction", input.Notes)
	})
}
