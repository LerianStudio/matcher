// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateManualMatchInput_TableDriven(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000210001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000210002")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000210003")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000210004")

	tests := []struct {
		name    string
		input   ManualMatchInput
		wantErr error
	}{
		{
			name: "valid input passes",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			},
			wantErr: nil,
		},
		{
			name: "nil tenant id",
			input: ManualMatchInput{
				TenantID:       uuid.Nil,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			},
			wantErr: ErrTenantIDRequired,
		},
		{
			name: "nil context id",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      uuid.Nil,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			},
			wantErr: ErrRunMatchContextIDRequired,
		},
		{
			name: "fewer than two transactions",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1},
			},
			wantErr: ErrMinimumTransactionsRequired,
		},
		{
			name: "empty transaction IDs",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{},
			},
			wantErr: ErrMinimumTransactionsRequired,
		},
		{
			name: "duplicate transaction IDs",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID1},
			},
			wantErr: ErrDuplicateTransactionIDs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			uc := &UseCase{}
			err := uc.validateManualMatchInput(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestManualMatchSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		message string
	}{
		{
			name:    "ErrMinimumTransactionsRequired",
			err:     ErrMinimumTransactionsRequired,
			message: "at least two transactions are required",
		},
		{
			name:    "ErrDuplicateTransactionIDs",
			err:     ErrDuplicateTransactionIDs,
			message: "duplicate transaction IDs provided",
		},
		{
			name:    "ErrTransactionNotFound",
			err:     ErrTransactionNotFound,
			message: "one or more transactions not found",
		},
		{
			name:    "ErrTransactionNotUnmatched",
			err:     ErrTransactionNotUnmatched,
			message: "one or more transactions are not unmatched",
		},
		{
			name:    "ErrManualMatchCreatingRun",
			err:     ErrManualMatchCreatingRun,
			message: "failed to create manual match run",
		},
		{
			name:    "ErrManualMatchNoGroupCreated",
			err:     ErrManualMatchNoGroupCreated,
			message: "no match group created",
		},
		{
			name:    "ErrManualMatchSourcesNotDiverse",
			err:     ErrManualMatchSourcesNotDiverse,
			message: "transactions must come from at least two different sources for reconciliation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			assert.Equal(t, tt.message, tt.err.Error())
		})
	}
}

func TestManualMatchConstants(t *testing.T) {
	t.Parallel()

	t.Run("minManualMatchTransactions is 2", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 2, minManualMatchTransactions)
	})

	t.Run("manualMatchConfidence is 100", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 100, manualMatchConfidence)
	})
}
