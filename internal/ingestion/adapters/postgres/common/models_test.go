//go:build unit

package common

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobPostgreSQLModel_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	completedAt := sql.NullTime{Time: now, Valid: true}
	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	ctxID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440001")
	srcID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440002")

	model := JobPostgreSQLModel{
		ID:          id,
		ContextID:   ctxID,
		SourceID:    srcID,
		Status:      "RUNNING",
		StartedAt:   now,
		CompletedAt: completedAt,
		Metadata:    []byte(`{"key":"value"}`),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	require.Equal(t, id, model.ID)
	require.Equal(t, ctxID, model.ContextID)
	require.Equal(t, srcID, model.SourceID)
	require.Equal(t, "RUNNING", model.Status)
	require.Equal(t, now, model.StartedAt)
	require.True(t, model.CompletedAt.Valid)
	require.Equal(t, now, model.CompletedAt.Time)
	require.JSONEq(t, `{"key":"value"}`, string(model.Metadata))
	require.Equal(t, now, model.CreatedAt)
	require.Equal(t, now, model.UpdatedAt)
}

func TestJobPostgreSQLModel_NullCompletedAt(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	model := JobPostgreSQLModel{
		ID:          uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		ContextID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440001"),
		SourceID:    uuid.MustParse("550e8400-e29b-41d4-a716-446655440002"),
		Status:      "PENDING",
		StartedAt:   now,
		CompletedAt: sql.NullTime{Valid: false},
		Metadata:    nil,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	assert.False(t, model.CompletedAt.Valid)
	assert.Nil(t, model.Metadata)
	assert.Equal(t, "PENDING", model.Status)
}

func TestJobPostgreSQLModel_EmptyMetadata(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	model := JobPostgreSQLModel{
		ID:        uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		ContextID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440001"),
		SourceID:  uuid.MustParse("550e8400-e29b-41d4-a716-446655440002"),
		Status:    "COMPLETED",
		StartedAt: now,
		Metadata:  []byte(`{}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	assert.JSONEq(t, `{}`, string(model.Metadata))
}

func TestJobPostgreSQLModel_ZeroValues(t *testing.T) {
	t.Parallel()

	model := JobPostgreSQLModel{}

	assert.Equal(t, uuid.Nil, model.ID)
	assert.Equal(t, uuid.Nil, model.ContextID)
	assert.Equal(t, uuid.Nil, model.SourceID)
	assert.Empty(t, model.Status)
	assert.True(t, model.StartedAt.IsZero())
	assert.False(t, model.CompletedAt.Valid)
	assert.Nil(t, model.Metadata)
	assert.True(t, model.CreatedAt.IsZero())
	assert.True(t, model.UpdatedAt.IsZero())
}

func TestTransactionPostgreSQLModel_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	amount := decimal.NewFromFloat(100.50)
	fxRate := decimal.NewNullDecimal(decimal.NewFromFloat(1.25))
	id := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	jobID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440001")
	srcID := uuid.MustParse("550e8400-e29b-41d4-a716-446655440002")

	model := TransactionPostgreSQLModel{
		ID:             id,
		IngestionJobID: jobID,
		SourceID:       srcID,
		ExternalID:     "ext-123",
		Amount:         amount,
		Currency:       "USD",
		AmountBase: decimal.NullDecimal{
			Decimal: decimal.NewFromFloat(125.625),
			Valid:   true,
		},
		BaseCurrency:        sql.NullString{String: "EUR", Valid: true},
		FXRate:              fxRate,
		FXRateSource:        sql.NullString{String: "ECB", Valid: true},
		FXRateEffectiveDate: sql.NullTime{Time: now, Valid: true},
		ExtractionStatus:    "COMPLETE",
		Date:                now,
		Description:         sql.NullString{String: "Test transaction", Valid: true},
		Status:              "UNMATCHED",
		Metadata:            []byte(`{"tag":"test"}`),
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	require.Equal(t, id, model.ID)
	require.Equal(t, jobID, model.IngestionJobID)
	require.Equal(t, srcID, model.SourceID)
	require.Equal(t, "ext-123", model.ExternalID)
	require.True(t, model.Amount.Equal(amount))
	require.Equal(t, "USD", model.Currency)
	require.True(t, model.AmountBase.Valid)
	require.True(t, model.BaseCurrency.Valid)
	require.True(t, model.FXRate.Valid)
	require.True(t, model.FXRateSource.Valid)
	require.True(t, model.FXRateEffectiveDate.Valid)
	require.Equal(t, "COMPLETE", model.ExtractionStatus)
	require.Equal(t, now, model.Date)
	require.True(t, model.Description.Valid)
	require.Equal(t, "UNMATCHED", model.Status)
	require.JSONEq(t, `{"tag":"test"}`, string(model.Metadata))
	require.Equal(t, now, model.CreatedAt)
	require.Equal(t, now, model.UpdatedAt)
}

func TestTransactionPostgreSQLModel_NullableFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	amount := decimal.NewFromFloat(50.00)

	model := TransactionPostgreSQLModel{
		ID:               uuid.MustParse("550e8400-e29b-41d4-a716-446655440000"),
		IngestionJobID:   uuid.MustParse("550e8400-e29b-41d4-a716-446655440001"),
		SourceID:         uuid.MustParse("550e8400-e29b-41d4-a716-446655440002"),
		ExternalID:       "ext-456",
		Amount:           amount,
		Currency:         "GBP",
		ExtractionStatus: "PENDING",
		Date:             now,
		Status:           "UNMATCHED",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	require.False(t, model.AmountBase.Valid)
	require.False(t, model.BaseCurrency.Valid)
	require.False(t, model.FXRate.Valid)
	require.False(t, model.FXRateSource.Valid)
	require.False(t, model.FXRateEffectiveDate.Valid)
	require.False(t, model.Description.Valid)
}
