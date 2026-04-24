// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package transaction

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestNewTransactionPostgreSQLModelAndToEntity(t *testing.T) {
	t.Parallel()

	tx := &shared.Transaction{
		ID:               uuid.New(),
		IngestionJobID:   uuid.New(),
		SourceID:         uuid.New(),
		ExternalID:       "ext",
		Amount:           decimal.RequireFromString("10"),
		Currency:         "USD",
		Date:             time.Now().UTC(),
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusMatched,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	model, err := NewTransactionPostgreSQLModel(tx)
	require.NoError(t, err)
	require.Equal(t, tx.ID, model.ID)

	entity, err := transactionModelToEntity(model)
	require.NoError(t, err)
	require.Equal(t, tx.ID, entity.ID)
	require.Equal(t, tx.ExternalID, entity.ExternalID)
}
