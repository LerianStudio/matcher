//go:build unit

package context

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestContextPostgreSQLModelRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctxEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Context",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)
	require.NoError(t, err)
	require.Equal(t, ctxEntity.ID, model.ID)

	entity, err := model.ToEntity()
	require.NoError(t, err)
	require.Equal(t, ctxEntity.ID, entity.ID)
	require.Equal(t, ctxEntity.TenantID, entity.TenantID)
	require.Equal(t, ctxEntity.Name, entity.Name)
	require.Equal(t, ctxEntity.Type, entity.Type)
	require.Equal(t, ctxEntity.Status, entity.Status)
}

func TestContextPostgreSQLModelDefaults(t *testing.T) {
	t.Parallel()

	ctxEntity := &entities.ReconciliationContext{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Name:     "Context",
		Type:     shared.ContextTypeOneToOne,
		Interval: "daily",
		Status:   value_objects.ContextStatusActive,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)
	require.NoError(t, err)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
}

func TestNewContextPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewContextPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrContextEntityRequired)
}

func TestNewContextPostgreSQLModel_NilTenantID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctxEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.Nil,
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestNewContextPostgreSQLModel_GeneratesIDWhenNil(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctxEntity := &entities.ReconciliationContext{
		ID:        uuid.Nil,
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEqual(t, uuid.Nil, model.ID)
}

func TestNewContextPostgreSQLModel_WithFeeTolerances(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctxEntity := &entities.ReconciliationContext{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Name:            "Test",
		Type:            shared.ContextTypeManyToMany,
		Interval:        "monthly",
		Status:          value_objects.ContextStatusActive,
		FeeToleranceAbs: decimal.NewFromFloat(10.50),
		FeeTolerancePct: decimal.NewFromFloat(5.5),
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, "10.5", model.FeeToleranceAbs)
	assert.Equal(t, "5.5", model.FeeTolerancePct)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *ContextPostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrContextModelRequired)
}

func TestToEntity_InvalidType(t *testing.T) {
	t.Parallel()

	model := &ContextPostgreSQLModel{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Type:     "INVALID_TYPE",
		Status:   "ACTIVE",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Type")
}

func TestToEntity_InvalidStatus(t *testing.T) {
	t.Parallel()

	model := &ContextPostgreSQLModel{
		ID:       uuid.New(),
		TenantID: uuid.New(),
		Type:     "1:1",
		Status:   "INVALID_STATUS",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing Status")
}

func TestToEntity_InvalidFeeToleranceAbs(t *testing.T) {
	t.Parallel()

	model := &ContextPostgreSQLModel{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Type:            "1:1",
		Status:          "ACTIVE",
		FeeToleranceAbs: "not-a-number",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing FeeToleranceAbs")
}

func TestToEntity_InvalidFeeTolerancePct(t *testing.T) {
	t.Parallel()

	model := &ContextPostgreSQLModel{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Type:            "1:1",
		Status:          "ACTIVE",
		FeeToleranceAbs: "10.5",
		FeeTolerancePct: "invalid",
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing FeeTolerancePct")
}

func TestToEntity_InvalidFeeNormalization(t *testing.T) {
	t.Parallel()

	invalidNormalization := "INVALID"
	model := &ContextPostgreSQLModel{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Type:             "1:1",
		Status:           "ACTIVE",
		FeeNormalization: &invalidNormalization,
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, entities.ErrFeeNormalizationInvalid)
}

func TestToEntity_ValidWithOptionalFields(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &ContextPostgreSQLModel{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Name:            "Test Context",
		Type:            "1:N",
		Interval:        "weekly",
		Status:          "PAUSED",
		FeeToleranceAbs: "25.50",
		FeeTolerancePct: "2.5",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, "Test Context", entity.Name)
	assert.Equal(t, shared.ContextTypeOneToMany, entity.Type)
	assert.Equal(t, "weekly", entity.Interval)
	assert.Equal(t, value_objects.ContextStatusPaused, entity.Status)
	assert.True(t, entity.FeeToleranceAbs.Equal(decimal.NewFromFloat(25.50)))
	assert.True(t, entity.FeeTolerancePct.Equal(decimal.NewFromFloat(2.5)))
}

func TestParseDecimalField(t *testing.T) {
	t.Parallel()

	t.Run("empty string returns zero", func(t *testing.T) {
		t.Parallel()

		result, err := parseDecimalField("", "testField")

		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.Zero))
	})

	t.Run("valid decimal", func(t *testing.T) {
		t.Parallel()

		result, err := parseDecimalField("123.45", "testField")

		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.NewFromFloat(123.45)))
	})

	t.Run("integer value", func(t *testing.T) {
		t.Parallel()

		result, err := parseDecimalField("100", "testField")

		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.NewFromInt(100)))
	})

	t.Run("invalid decimal", func(t *testing.T) {
		t.Parallel()

		_, err := parseDecimalField("not-a-number", "testField")

		require.Error(t, err)
		require.Contains(t, err.Error(), "parsing testField")
	})

	t.Run("negative decimal", func(t *testing.T) {
		t.Parallel()

		result, err := parseDecimalField("-50.25", "testField")

		require.NoError(t, err)
		assert.True(t, result.Equal(decimal.NewFromFloat(-50.25)))
	})
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrContextEntityRequired", ErrContextEntityRequired},
		{"ErrContextModelRequired", ErrContextModelRequired},
		{"ErrTenantIDRequired", ErrTenantIDRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrTransactionRequired", ErrTransactionRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestNewContextPostgreSQLModel_ZeroUpdatedAt(t *testing.T) {
	t.Parallel()

	createdAt := time.Now().UTC()
	ctxEntity := &entities.ReconciliationContext{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Name:      "Test",
		Type:      shared.ContextTypeOneToOne,
		Interval:  "daily",
		Status:    value_objects.ContextStatusActive,
		CreatedAt: createdAt,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, createdAt, model.CreatedAt)
	require.Equal(t, createdAt, model.UpdatedAt)
}

func TestNewContextPostgreSQLModel_AllTypes(t *testing.T) {
	t.Parallel()

	contextTypes := []struct {
		ctxType     shared.ContextType
		expectedStr string
	}{
		{shared.ContextTypeOneToOne, "1:1"},
		{shared.ContextTypeOneToMany, "1:N"},
		{shared.ContextTypeManyToMany, "N:M"},
	}

	for _, tt := range contextTypes {
		t.Run(tt.expectedStr, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			ctxEntity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      "Test",
				Type:      tt.ctxType,
				Interval:  "daily",
				Status:    value_objects.ContextStatusActive,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(ctxEntity)

			require.NoError(t, err)
			require.NotNil(t, model)
			require.Equal(t, tt.expectedStr, model.Type)
		})
	}
}

func TestNewContextPostgreSQLModel_AllStatuses(t *testing.T) {
	t.Parallel()

	statuses := []struct {
		status      value_objects.ContextStatus
		expectedStr string
	}{
		{value_objects.ContextStatusActive, "ACTIVE"},
		{value_objects.ContextStatusPaused, "PAUSED"},
	}

	for _, tt := range statuses {
		t.Run(tt.expectedStr, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			ctxEntity := &entities.ReconciliationContext{
				ID:        uuid.New(),
				TenantID:  uuid.New(),
				Name:      "Test",
				Type:      shared.ContextTypeOneToOne,
				Interval:  "daily",
				Status:    tt.status,
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewContextPostgreSQLModel(ctxEntity)

			require.NoError(t, err)
			require.NotNil(t, model)
			require.Equal(t, tt.expectedStr, model.Status)
		})
	}
}

func TestToEntity_EmptyFeeTolerances(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &ContextPostgreSQLModel{
		ID:              uuid.New(),
		TenantID:        uuid.New(),
		Name:            "Test",
		Type:            "1:1",
		Interval:        "daily",
		Status:          "ACTIVE",
		FeeToleranceAbs: "",
		FeeTolerancePct: "",
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.True(t, entity.FeeToleranceAbs.Equal(decimal.Zero))
	assert.True(t, entity.FeeTolerancePct.Equal(decimal.Zero))
}

func TestModelPreservesAllFields(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 8, 20, 14, 45, 30, 0, time.UTC)
	entityID := uuid.New()
	tenantID := uuid.New()

	ctxEntity := &entities.ReconciliationContext{
		ID:              entityID,
		TenantID:        tenantID,
		Name:            "Full Field Test",
		Type:            shared.ContextTypeManyToMany,
		Interval:        "monthly",
		Status:          value_objects.ContextStatusPaused,
		FeeToleranceAbs: decimal.NewFromFloat(100.25),
		FeeTolerancePct: decimal.NewFromFloat(5.5),
		CreatedAt:       createdAt,
		UpdatedAt:       updatedAt,
	}

	model, err := NewContextPostgreSQLModel(ctxEntity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, entityID, resultEntity.ID)
	require.Equal(t, tenantID, resultEntity.TenantID)
	require.Equal(t, "Full Field Test", resultEntity.Name)
	require.Equal(t, shared.ContextTypeManyToMany, resultEntity.Type)
	require.Equal(t, "monthly", resultEntity.Interval)
	require.Equal(t, value_objects.ContextStatusPaused, resultEntity.Status)
	assert.True(t, resultEntity.FeeToleranceAbs.Equal(decimal.NewFromFloat(100.25)))
	assert.True(t, resultEntity.FeeTolerancePct.Equal(decimal.NewFromFloat(5.5)))
	require.Equal(t, createdAt, resultEntity.CreatedAt)
	require.Equal(t, updatedAt, resultEntity.UpdatedAt)
}

func TestParseDecimalField_ZeroValue(t *testing.T) {
	t.Parallel()

	result, err := parseDecimalField("0", "testField")

	require.NoError(t, err)
	assert.True(t, result.Equal(decimal.Zero))
}

func TestParseDecimalField_HighPrecision(t *testing.T) {
	t.Parallel()

	result, err := parseDecimalField("123.456789012345", "testField")

	require.NoError(t, err)
	expected := decimal.RequireFromString("123.456789012345")
	assert.True(t, result.Equal(expected))
}

func TestParseDecimalField_VeryLargeNumber(t *testing.T) {
	t.Parallel()

	result, err := parseDecimalField("999999999999999.99", "testField")

	require.NoError(t, err)
	expected := decimal.RequireFromString("999999999999999.99")
	assert.True(t, result.Equal(expected))
}

func TestParseDecimalField_ScientificNotation(t *testing.T) {
	t.Parallel()

	result, err := parseDecimalField("1.5e2", "testField")

	require.NoError(t, err)
	expected := decimal.NewFromFloat(150)
	assert.True(t, result.Equal(expected))
}
