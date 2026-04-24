// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package ports

import (
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

// TestForceMatchInput_FieldAccess verifies field assignment and access for ForceMatchInput.
// This test catches accidental field renames or type changes during refactoring.
func TestForceMatchInput_FieldAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		exceptionID    uuid.UUID
		transactionID  uuid.UUID
		notes          string
		overrideReason string
		actor          string
	}{
		{
			name:           "valid input with all fields populated",
			exceptionID:    uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			transactionID:  uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			notes:          "test notes",
			overrideReason: "test reason",
			actor:          "test-actor",
		},
		{
			name:           "empty strings and zero UUIDs",
			exceptionID:    uuid.Nil,
			transactionID:  uuid.Nil,
			notes:          "",
			overrideReason: "",
			actor:          "",
		},
		{
			name:           "only exception ID populated",
			exceptionID:    uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			transactionID:  uuid.Nil,
			notes:          "",
			overrideReason: "",
			actor:          "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := ForceMatchInput{
				ExceptionID:    tt.exceptionID,
				TransactionID:  tt.transactionID,
				Notes:          tt.notes,
				OverrideReason: tt.overrideReason,
				Actor:          tt.actor,
			}

			assert.Equal(t, tt.exceptionID, input.ExceptionID)
			assert.Equal(t, tt.transactionID, input.TransactionID)
			assert.Equal(t, tt.notes, input.Notes)
			assert.Equal(t, tt.overrideReason, input.OverrideReason)
			assert.Equal(t, tt.actor, input.Actor)
		})
	}
}

// TestCreateAdjustmentInput_FieldAccess verifies field assignment and access for CreateAdjustmentInput.
// This test catches accidental field renames or type changes during refactoring, and covers edge cases
// for decimal amounts including zero, negative, and boundary values.
func TestCreateAdjustmentInput_FieldAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		exceptionID   uuid.UUID
		transactionID uuid.UUID
		amount        decimal.Decimal
		currency      string
		reason        string
		notes         string
		actor         string
	}{
		{
			name:          "valid input with positive amount",
			exceptionID:   uuid.MustParse("11111111-1111-1111-1111-111111111111"),
			transactionID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
			amount:        decimal.NewFromFloat(100.50),
			currency:      "USD",
			reason:        "adjustment reason",
			notes:         "adjustment notes",
			actor:         "admin-user",
		},
		{
			name:          "zero amount",
			exceptionID:   uuid.MustParse("33333333-3333-3333-3333-333333333333"),
			transactionID: uuid.MustParse("44444444-4444-4444-4444-444444444444"),
			amount:        decimal.Zero,
			currency:      "EUR",
			reason:        "zero adjustment",
			notes:         "",
			actor:         "system",
		},
		{
			name:          "negative amount",
			exceptionID:   uuid.MustParse("55555555-5555-5555-5555-555555555555"),
			transactionID: uuid.MustParse("66666666-6666-6666-6666-666666666666"),
			amount:        decimal.NewFromFloat(-50.25),
			currency:      "GBP",
			reason:        "credit adjustment",
			notes:         "refund",
			actor:         "finance-team",
		},
		{
			name:          "empty currency",
			exceptionID:   uuid.MustParse("77777777-7777-7777-7777-777777777777"),
			transactionID: uuid.MustParse("88888888-8888-8888-8888-888888888888"),
			amount:        decimal.NewFromFloat(1.00),
			currency:      "",
			reason:        "missing currency",
			notes:         "",
			actor:         "",
		},
		{
			name:          "large boundary value",
			exceptionID:   uuid.MustParse("99999999-9999-9999-9999-999999999999"),
			transactionID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
			amount:        decimal.NewFromFloat(999999999999.99),
			currency:      "JPY",
			reason:        "large transaction",
			notes:         "boundary test",
			actor:         "test-actor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input := CreateAdjustmentInput{
				ExceptionID:   tt.exceptionID,
				TransactionID: tt.transactionID,
				Amount:        tt.amount,
				Currency:      tt.currency,
				Reason:        tt.reason,
				Notes:         tt.notes,
				Actor:         tt.actor,
			}

			assert.Equal(t, tt.exceptionID, input.ExceptionID)
			assert.Equal(t, tt.transactionID, input.TransactionID)
			assert.True(
				t,
				tt.amount.Equal(input.Amount),
				"amount mismatch: expected %s, got %s",
				tt.amount,
				input.Amount,
			)
			assert.Equal(t, tt.currency, input.Currency)
			assert.Equal(t, tt.reason, input.Reason)
			assert.Equal(t, tt.notes, input.Notes)
			assert.Equal(t, tt.actor, input.Actor)
		})
	}
}
