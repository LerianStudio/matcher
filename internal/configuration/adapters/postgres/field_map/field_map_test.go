//go:build unit

package field_map

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestFieldMapPostgreSQLModelRoundTrip(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"external_id": "id"},
		Version:   2,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)
	require.NoError(t, err)

	out, err := model.ToEntity()
	require.NoError(t, err)
	require.Equal(t, entity.ID, out.ID)
	require.Equal(t, entity.ContextID, out.ContextID)
	require.Equal(t, entity.SourceID, out.SourceID)
	require.Equal(t, entity.Mapping["external_id"], out.Mapping["external_id"])
}

func TestFieldMapPostgreSQLModelDefaults(t *testing.T) {
	t.Parallel()

	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"external_id": "id"},
		Version:   1,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)
	require.NoError(t, err)
	require.False(t, model.CreatedAt.IsZero())
	require.False(t, model.UpdatedAt.IsZero())
}

func TestNewFieldMapPostgreSQLModel_NilEntity(t *testing.T) {
	t.Parallel()

	model, err := NewFieldMapPostgreSQLModel(nil)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrFieldMapEntityRequired)
}

func TestNewFieldMapPostgreSQLModel_NilEntityID(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.Nil,
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{"field": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)

	require.Error(t, err)
	require.Nil(t, model)
	require.ErrorIs(t, err, ErrFieldMapEntityIDRequired)
}

func TestNewFieldMapPostgreSQLModel_EmptyMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Mapping)
}

func TestNewFieldMapPostgreSQLModel_ComplexMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping: map[string]any{
			"external_id": "transaction_id",
			"amount":      "payment_amount",
			"date":        "transaction_date",
			"nested": map[string]any{
				"key": "value",
			},
		},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.NotEmpty(t, model.Mapping)
}

func TestToEntity_NilModel(t *testing.T) {
	t.Parallel()

	var model *FieldMapPostgreSQLModel
	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.ErrorIs(t, err, ErrFieldMapModelRequired)
}

func TestToEntity_InvalidID(t *testing.T) {
	t.Parallel()

	model := &FieldMapPostgreSQLModel{
		ID:        "not-a-uuid",
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ID")
}

func TestToEntity_InvalidContextID(t *testing.T) {
	t.Parallel()

	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: "invalid",
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing ContextID")
}

func TestToEntity_InvalidSourceID(t *testing.T) {
	t.Parallel()

	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  "invalid",
		Mapping:   []byte(`{}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "parsing SourceID")
}

func TestToEntity_InvalidMappingJSON(t *testing.T) {
	t.Parallel()

	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{invalid json}`),
	}

	entity, err := model.ToEntity()

	require.Error(t, err)
	require.Nil(t, entity)
	require.Contains(t, err.Error(), "unmarshal mapping")
}

func TestToEntity_EmptyMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte{},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Mapping)
	require.Empty(t, entity.Mapping)
}

func TestToEntity_NilMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   nil,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Mapping)
	require.Empty(t, entity.Mapping)
}

func TestToEntity_ValidWithVersion(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{"field1":"value1","field2":"value2"}`),
		Version:   5,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, 5, entity.Version)
	require.Len(t, entity.Mapping, 2)
	require.Equal(t, "value1", entity.Mapping["field1"])
	require.Equal(t, "value2", entity.Mapping["field2"])
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrFieldMapEntityRequired", ErrFieldMapEntityRequired},
		{"ErrFieldMapEntityIDRequired", ErrFieldMapEntityIDRequired},
		{"ErrFieldMapModelRequired", ErrFieldMapModelRequired},
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

func TestNewFieldMapPostgreSQLModel_ZeroUpdatedAt(t *testing.T) {
	t.Parallel()

	createdAt := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   map[string]any{},
		Version:   1,
		CreatedAt: createdAt,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, createdAt, model.CreatedAt)
	require.Equal(t, createdAt, model.UpdatedAt)
}

func TestNewFieldMapPostgreSQLModel_NilMapping(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   nil,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	require.Equal(t, []byte("null"), model.Mapping)
}

func TestToEntity_MappingWithNullValue(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{"key":null,"nested":{"value":null}}`),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Nil(t, entity.Mapping["key"])
}

func TestModelPreservesAllFields(t *testing.T) {
	t.Parallel()

	createdAt := time.Date(2023, 5, 15, 10, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 8, 20, 14, 45, 30, 0, time.UTC)
	entityID := uuid.New()
	contextID := uuid.New()
	sourceID := uuid.New()

	entity := &shared.FieldMap{
		ID:        entityID,
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping: map[string]any{
			"field1": "value1",
			"field2": "value2",
		},
		Version:   5,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)

	require.Equal(t, entityID, resultEntity.ID)
	require.Equal(t, contextID, resultEntity.ContextID)
	require.Equal(t, sourceID, resultEntity.SourceID)
	require.Equal(t, 5, resultEntity.Version)
	require.Equal(t, createdAt, resultEntity.CreatedAt)
	require.Equal(t, updatedAt, resultEntity.UpdatedAt)
	require.Equal(t, "value1", resultEntity.Mapping["field1"])
	require.Equal(t, "value2", resultEntity.Mapping["field2"])
}

func TestToEntity_MappingWithComplexTypes(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{"array":[1,2,3],"nested":{"deep":{"value":true}},"number":42.5}`),
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.NotNil(t, entity.Mapping["array"])
	require.NotNil(t, entity.Mapping["nested"])
	require.InDelta(t, 42.5, entity.Mapping["number"], 0.01)
}

func TestToEntity_LargeMappingPayload(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	largeMapping := make(map[string]any)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("field_%c_%c", 'a'+i%26, '0'+i/26)
		largeMapping[key] = "value_" + key
	}

	entity := &shared.FieldMap{
		ID:        uuid.New(),
		ContextID: uuid.New(),
		SourceID:  uuid.New(),
		Mapping:   largeMapping,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	model, err := NewFieldMapPostgreSQLModel(entity)
	require.NoError(t, err)

	resultEntity, err := model.ToEntity()
	require.NoError(t, err)
	require.Len(t, resultEntity.Mapping, 100)
}

func TestToEntity_VersionZero(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	model := &FieldMapPostgreSQLModel{
		ID:        uuid.New().String(),
		ContextID: uuid.New().String(),
		SourceID:  uuid.New().String(),
		Mapping:   []byte(`{}`),
		Version:   0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	entity, err := model.ToEntity()

	require.NoError(t, err)
	require.NotNil(t, entity)
	require.Equal(t, 0, entity.Version)
}
