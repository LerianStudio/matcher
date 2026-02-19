//go:build unit

package common

import (
	"database/sql"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobPostgreSQLModel_Fields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	completedAt := sql.NullTime{Time: now, Valid: true}

	model := JobPostgreSQLModel{
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		ContextID:   "550e8400-e29b-41d4-a716-446655440001",
		SourceID:    "550e8400-e29b-41d4-a716-446655440002",
		Status:      "RUNNING",
		StartedAt:   now,
		CompletedAt: completedAt,
		Metadata:    []byte(`{"key":"value"}`),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", model.ID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440001", model.ContextID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440002", model.SourceID)
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
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		ContextID:   "550e8400-e29b-41d4-a716-446655440001",
		SourceID:    "550e8400-e29b-41d4-a716-446655440002",
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
		ID:        "550e8400-e29b-41d4-a716-446655440000",
		ContextID: "550e8400-e29b-41d4-a716-446655440001",
		SourceID:  "550e8400-e29b-41d4-a716-446655440002",
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

	assert.Empty(t, model.ID)
	assert.Empty(t, model.ContextID)
	assert.Empty(t, model.SourceID)
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

	model := TransactionPostgreSQLModel{
		ID:             "550e8400-e29b-41d4-a716-446655440000",
		IngestionJobID: "550e8400-e29b-41d4-a716-446655440001",
		SourceID:       "550e8400-e29b-41d4-a716-446655440002",
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

	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", model.ID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440001", model.IngestionJobID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440002", model.SourceID)
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
		ID:               "550e8400-e29b-41d4-a716-446655440000",
		IngestionJobID:   "550e8400-e29b-41d4-a716-446655440001",
		SourceID:         "550e8400-e29b-41d4-a716-446655440002",
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

	require.Equal(t, "550e8400-e29b-41d4-a716-446655440000", model.ID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440001", model.IngestionJobID)
	require.Equal(t, "550e8400-e29b-41d4-a716-446655440002", model.SourceID)
	require.Equal(t, "ext-456", model.ExternalID)
	require.True(t, model.Amount.Equal(amount))
	require.Equal(t, "GBP", model.Currency)
	require.Equal(t, "PENDING", model.ExtractionStatus)
	require.Equal(t, now, model.Date)
	require.Equal(t, "UNMATCHED", model.Status)
	require.Equal(t, now, model.CreatedAt)
	require.Equal(t, now, model.UpdatedAt)
}

func TestTransactionPostgreSQLModel_ZeroValues(t *testing.T) {
	t.Parallel()

	model := TransactionPostgreSQLModel{}

	assert.Empty(t, model.ID)
	assert.Empty(t, model.IngestionJobID)
	assert.Empty(t, model.SourceID)
	assert.Empty(t, model.ExternalID)
	assert.True(t, model.Amount.IsZero())
	assert.Empty(t, model.Currency)
	assert.False(t, model.AmountBase.Valid)
	assert.False(t, model.BaseCurrency.Valid)
	assert.False(t, model.FXRate.Valid)
	assert.False(t, model.FXRateSource.Valid)
	assert.False(t, model.FXRateEffectiveDate.Valid)
	assert.Empty(t, model.ExtractionStatus)
	assert.True(t, model.Date.IsZero())
	assert.False(t, model.Description.Valid)
	assert.Empty(t, model.Status)
	assert.Nil(t, model.Metadata)
	assert.True(t, model.CreatedAt.IsZero())
	assert.True(t, model.UpdatedAt.IsZero())
}

func TestTransactionPostgreSQLModel_DecimalPrecision(t *testing.T) {
	t.Parallel()

	highPrecisionAmount, err := decimal.NewFromString("123456789.123456789")
	require.NoError(t, err)
	now := time.Now().UTC()

	model := TransactionPostgreSQLModel{
		ID:               "550e8400-e29b-41d4-a716-446655440000",
		IngestionJobID:   "550e8400-e29b-41d4-a716-446655440001",
		SourceID:         "550e8400-e29b-41d4-a716-446655440002",
		ExternalID:       "ext-precision",
		Amount:           highPrecisionAmount,
		Currency:         "USD",
		ExtractionStatus: "COMPLETE",
		Date:             now,
		Status:           "MATCHED",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	assert.True(t, model.Amount.Equal(highPrecisionAmount))
	assert.Equal(t, "123456789.123456789", model.Amount.String())
}

func TestTransactionPostgreSQLModel_FXRateFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	fxDate := now.AddDate(0, 0, -1)
	amount := decimal.NewFromFloat(100.00)
	amountBase := decimal.NewFromFloat(85.50)
	fxRate := decimal.NewFromFloat(0.855)

	model := TransactionPostgreSQLModel{
		ID:                  "550e8400-e29b-41d4-a716-446655440000",
		IngestionJobID:      "550e8400-e29b-41d4-a716-446655440001",
		SourceID:            "550e8400-e29b-41d4-a716-446655440002",
		ExternalID:          "ext-fx",
		Amount:              amount,
		Currency:            "USD",
		AmountBase:          decimal.NullDecimal{Decimal: amountBase, Valid: true},
		BaseCurrency:        sql.NullString{String: "EUR", Valid: true},
		FXRate:              decimal.NullDecimal{Decimal: fxRate, Valid: true},
		FXRateSource:        sql.NullString{String: "ECB", Valid: true},
		FXRateEffectiveDate: sql.NullTime{Time: fxDate, Valid: true},
		ExtractionStatus:    "COMPLETE",
		Date:                now,
		Status:              "UNMATCHED",
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	assert.True(t, model.AmountBase.Valid)
	assert.True(t, model.AmountBase.Decimal.Equal(amountBase))
	assert.True(t, model.BaseCurrency.Valid)
	assert.Equal(t, "EUR", model.BaseCurrency.String)
	assert.True(t, model.FXRate.Valid)
	assert.True(t, model.FXRate.Decimal.Equal(fxRate))
	assert.True(t, model.FXRateSource.Valid)
	assert.Equal(t, "ECB", model.FXRateSource.String)
	assert.True(t, model.FXRateEffectiveDate.Valid)
	assert.Equal(t, fxDate, model.FXRateEffectiveDate.Time)
}

func TestTransactionPostgreSQLModel_MetadataVariations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata []byte
	}{
		{
			name:     "nil metadata",
			metadata: nil,
		},
		{
			name:     "empty object",
			metadata: []byte(`{}`),
		},
		{
			name:     "empty array",
			metadata: []byte(`[]`),
		},
		{
			name:     "complex nested object",
			metadata: []byte(`{"level1":{"level2":{"value":123}}}`),
		},
		{
			name:     "array with objects",
			metadata: []byte(`[{"key":"value1"},{"key":"value2"}]`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			model := TransactionPostgreSQLModel{
				ID:               "550e8400-e29b-41d4-a716-446655440000",
				IngestionJobID:   "550e8400-e29b-41d4-a716-446655440001",
				SourceID:         "550e8400-e29b-41d4-a716-446655440002",
				ExternalID:       "ext-meta",
				Amount:           decimal.NewFromFloat(100.00),
				Currency:         "USD",
				ExtractionStatus: "COMPLETE",
				Date:             now,
				Status:           "UNMATCHED",
				Metadata:         tt.metadata,
				CreatedAt:        now,
				UpdatedAt:        now,
			}

			if tt.metadata == nil {
				assert.Nil(t, model.Metadata)
			} else {
				assert.Equal(t, tt.metadata, model.Metadata)
			}
		})
	}
}
