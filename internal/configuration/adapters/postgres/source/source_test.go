// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package source

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
)

func TestSourcePostgreSQLModelRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Source",
		Type:      value_objects.SourceTypeLedger,
		Config:    map[string]any{"region": "us"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewSourcePostgreSQLModel(entity)
	require.NoError(t, err)

	out, err := model.ToEntity()
	require.NoError(t, err)
	require.Equal(t, entity.ID, out.ID)
	require.Equal(t, entity.ContextID, out.ContextID)
	require.Equal(t, entity.Name, out.Name)
	require.Equal(t, entity.Type, out.Type)
	require.Equal(t, entity.Config["region"], out.Config["region"])
}

func TestSourcePostgreSQLModelDefaults(t *testing.T) {
	t.Parallel()

	entity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Source",
		Type:      value_objects.SourceTypeLedger,
		Config:    map[string]any{},
	}

	model, err := NewSourcePostgreSQLModel(entity)
	require.NoError(t, err)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
}

func TestNewSourcePostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewSourcePostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrSourceEntityRequired)
}

func TestNewSourcePostgreSQLModel_NilID(t *testing.T) {
	t.Parallel()

	entity := &entities.ReconciliationSource{
		ID:        uuid.Nil,
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      value_objects.SourceTypeLedger,
	}

	model, err := NewSourcePostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrSourceEntityIDRequired)
}

func TestNewSourcePostgreSQLModel_NilConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      value_objects.SourceTypeLedger,
		Config:    nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewSourcePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Config)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *SourcePostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrSourceModelRequired)
}

func TestToEntity_InvalidType(t *testing.T) {
	t.Parallel()

	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Type:      "INVALID_TYPE",
		Config:    []byte(`{}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parse source type")
}

func TestToEntity_InvalidConfig(t *testing.T) {
	t.Parallel()

	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Type:      "LEDGER",
		Config:    []byte(`{invalid json}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "unmarshal config")
}

func TestToEntity_EmptyConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      "GATEWAY",
		Config:    []byte{},
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config)
	require.Empty(t, entity.Config)
}

func TestToEntity_NilConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      "CUSTOM",
		Config:    nil,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config)
	require.Empty(t, entity.Config)
}

func TestToEntity_ValidWithConfig(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test Source",
		Type:      "BANK",
		Config:    []byte(`{"key":"value","number":42}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, "Test Source", entity.Name)
	require.Equal(t, value_objects.SourceTypeBank, entity.Type)
	require.Len(t, entity.Config, 2)
	require.Equal(t, "value", entity.Config["key"])
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrSourceEntityRequired", ErrSourceEntityRequired},
		{"ErrSourceEntityIDRequired", ErrSourceEntityIDRequired},
		{"ErrSourceModelRequired", ErrSourceModelRequired},
		{"ErrRepoNotInitialized", ErrRepoNotInitialized},
		{"ErrConnectionRequired", ErrConnectionRequired},
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

func TestNewSourcePostgreSQLModel_ZeroUpdatedAt(t *testing.T) {
	t.Parallel()

	createdAt := time.Now().UTC()
	entity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      value_objects.SourceTypeLedger,
		Config:    map[string]any{},
		CreatedAt: createdAt,
	}

	model, err := NewSourcePostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, createdAt, model.CreatedAt)
	require.Equal(t, createdAt, model.UpdatedAt)
}

func TestNewSourcePostgreSQLModel_AllSourceTypes(t *testing.T) {
	t.Parallel()

	sourceTypes := []struct {
		sourceType  value_objects.SourceType
		expectedStr string
	}{
		{value_objects.SourceTypeLedger, "LEDGER"},
		{value_objects.SourceTypeGateway, "GATEWAY"},
		{value_objects.SourceTypeBank, "BANK"},
		{value_objects.SourceTypeCustom, "CUSTOM"},
	}

	for _, tt := range sourceTypes {
		t.Run(tt.expectedStr, func(t *testing.T) {
			t.Parallel()

			now := time.Now().UTC()
			entity := &entities.ReconciliationSource{
				ID:        uuid.New(),
				ContextID: uuid.New(),
				Name:      "Test Source",
				Type:      tt.sourceType,
				Config:    map[string]any{},
				CreatedAt: now,
				UpdatedAt: now,
			}

			model, err := NewSourcePostgreSQLModel(entity)

			require.NoError(t, err)
			require.NotNil(t, model)
			require.Equal(t, tt.expectedStr, model.Type)
		})
	}
}

func TestToEntity_ConfigWithNullValue(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      "LEDGER",
		Config:    []byte(`{"key":null,"nested":{"value":null}}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Nil(t, entity.Config["key"])
}

func TestToEntity_ConfigWithArrayOfObjects(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &SourcePostgreSQLModel{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Test",
		Type:      "GATEWAY",
		Config:    []byte(`{"items":[{"id":1,"name":"first"},{"id":2,"name":"second"}]}`),
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Config["items"])
}

func TestModelPreservesAllFields(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 8, 20, 14, 45, 30, 0, time.UTC)
	entityID := uuid.New()
	contextID := uuid.New()

	entity := &entities.ReconciliationSource{
		ID:        entityID,
		ContextID: contextID,
		Name:      "Full Field Test",
		Type:      value_objects.SourceTypeBank,
		Config: map[string]any{
			"api_key":  "secret",
			"endpoint": "https://api.example.com",
			"timeout":  float64(30),
		},
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	model, err := NewSourcePostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, entityID, resultEntity.ID)
	require.Equal(t, contextID, resultEntity.ContextID)
	require.Equal(t, "Full Field Test", resultEntity.Name)
	require.Equal(t, value_objects.SourceTypeBank, resultEntity.Type)
	require.Equal(t, createdAt, resultEntity.CreatedAt)
	require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	require.Equal(t, "secret", resultEntity.Config["api_key"])
	require.Equal(t, "https://api.example.com", resultEntity.Config["endpoint"])
	require.InDelta(t, float64(30), resultEntity.Config["timeout"], 0.01)
}

func TestToEntity_LargeConfigPayload(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	largeConfig := make(map[string]any)
	for i := 0; i < 100; i++ {
		key := "field_" + string(rune('a'+i%26)) + "_" + string(rune('0'+i/26))
		largeConfig[key] = "value_" + key
	}

	entity := &entities.ReconciliationSource{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		Name:      "Large Config Test",
		Type:      value_objects.SourceTypeCustom,
		Config:    largeConfig,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewSourcePostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)
	require.Len(t, resultEntity.Config, 100)
}
