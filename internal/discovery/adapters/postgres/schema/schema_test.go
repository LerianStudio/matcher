// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package schema

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

// validSchemaModel returns a fully-populated SchemaModel for test fixtures.
func validSchemaModel() *SchemaModel {
	columns := []entities.ColumnInfo{
		{Name: "id", Type: "uuid", Nullable: false},
		{Name: "amount", Type: "numeric(18,2)", Nullable: false},
		{Name: "memo", Type: "text", Nullable: true},
	}

	columnsJSON, _ := json.Marshal(columns) //nolint:errcheck // test fixture

	return &SchemaModel{
		ID:           uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		ConnectionID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		TableName:    "transactions",
		Columns:      columnsJSON,
		DiscoveredAt: time.Date(2026, 3, 8, 10, 0, 0, 0, time.UTC),
	}
}

// validSchemaEntity returns a fully-populated DiscoveredSchema entity for test fixtures.
func validSchemaEntity() *entities.DiscoveredSchema {
	return &entities.DiscoveredSchema{
		ID:           uuid.MustParse("11111111-2222-3333-4444-555555555555"),
		ConnectionID: uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
		TableName:    "transactions",
		Columns: []entities.ColumnInfo{
			{Name: "id", Type: "uuid", Nullable: false},
			{Name: "amount", Type: "numeric(18,2)", Nullable: false},
			{Name: "memo", Type: "text", Nullable: true},
		},
		DiscoveredAt: time.Date(2026, 3, 8, 10, 0, 0, 0, time.UTC),
	}
}

func TestSchemaModel_ToDomain_ValidModel(t *testing.T) {
	t.Parallel()

	model := validSchemaModel()
	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Equal(t, model.ID, entity.ID)
	assert.Equal(t, model.ConnectionID, entity.ConnectionID)
	assert.Equal(t, model.TableName, entity.TableName)
	assert.Equal(t, model.DiscoveredAt, entity.DiscoveredAt)
	require.Len(t, entity.Columns, 3)
	assert.Equal(t, "id", entity.Columns[0].Name)
	assert.Equal(t, "uuid", entity.Columns[0].Type)
	assert.False(t, entity.Columns[0].Nullable)
	assert.Equal(t, "memo", entity.Columns[2].Name)
	assert.True(t, entity.Columns[2].Nullable)
}

func TestSchemaModel_ToDomain_NilModel_ReturnsError(t *testing.T) {
	t.Parallel()

	var model *SchemaModel
	entity, err := model.ToDomain()

	assert.ErrorIs(t, err, ErrModelRequired)
	assert.Nil(t, entity)
}

func TestSchemaModel_ToDomain_EmptyColumns(t *testing.T) {
	t.Parallel()

	model := validSchemaModel()
	model.Columns = nil

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Empty(t, entity.Columns)
}

func TestSchemaModel_ToDomain_EmptyJSONArray(t *testing.T) {
	t.Parallel()

	model := validSchemaModel()
	model.Columns = []byte("[]")

	entity, err := model.ToDomain()

	require.NoError(t, err)
	require.NotNil(t, entity)
	assert.Empty(t, entity.Columns)
}

func TestSchemaModel_ToDomain_InvalidColumnsJSON(t *testing.T) {
	t.Parallel()

	model := validSchemaModel()
	model.Columns = []byte("{not valid json")

	entity, err := model.ToDomain()

	assert.Error(t, err)
	assert.Nil(t, entity)
	assert.Contains(t, err.Error(), "unmarshal columns")
}

func TestSchemaFromDomain_ValidEntity(t *testing.T) {
	t.Parallel()

	entity := validSchemaEntity()
	model, err := FromDomain(entity)

	require.NoError(t, err)
	require.NotNil(t, model)
	assert.Equal(t, entity.ID, model.ID)
	assert.Equal(t, entity.ConnectionID, model.ConnectionID)
	assert.Equal(t, entity.TableName, model.TableName)
	assert.Equal(t, entity.DiscoveredAt, model.DiscoveredAt)

	// Verify columns were serialized as valid JSON.
	var columns []entities.ColumnInfo
	err = json.Unmarshal(model.Columns, &columns)

	require.NoError(t, err)
	assert.Len(t, columns, 3)
}

func TestSchemaFromDomain_NilEntity_ReturnsError(t *testing.T) {
	t.Parallel()

	model, err := FromDomain(nil)

	assert.ErrorIs(t, err, ErrEntityRequired)
	assert.Nil(t, model)
}

func TestSchemaModel_RoundTrip_PreservesAllFields(t *testing.T) {
	t.Parallel()

	original := validSchemaEntity()

	model, err := FromDomain(original)
	require.NoError(t, err)
	require.NotNil(t, model)

	roundTripped, err := model.ToDomain()
	require.NoError(t, err)
	require.NotNil(t, roundTripped)

	assert.Equal(t, original.ID, roundTripped.ID)
	assert.Equal(t, original.ConnectionID, roundTripped.ConnectionID)
	assert.Equal(t, original.TableName, roundTripped.TableName)
	assert.Equal(t, original.DiscoveredAt, roundTripped.DiscoveredAt)
	require.Len(t, roundTripped.Columns, len(original.Columns))

	for i, col := range original.Columns {
		assert.Equal(t, col.Name, roundTripped.Columns[i].Name)
		assert.Equal(t, col.Type, roundTripped.Columns[i].Type)
		assert.Equal(t, col.Nullable, roundTripped.Columns[i].Nullable)
	}
}

func TestSchemaModel_RoundTrip_EmptyColumns(t *testing.T) {
	t.Parallel()

	original := validSchemaEntity()
	original.Columns = []entities.ColumnInfo{}

	model, err := FromDomain(original)
	require.NoError(t, err)
	require.NotNil(t, model)

	roundTripped, err := model.ToDomain()
	require.NoError(t, err)
	require.NotNil(t, roundTripped)
	assert.Empty(t, roundTripped.Columns)
}
