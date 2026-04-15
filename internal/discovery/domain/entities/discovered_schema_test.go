//go:build unit

package entities_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/entities"
)

func TestNewDiscoveredSchema_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()
	columns := []entities.ColumnInfo{
		{Name: "id", Type: "uuid", Nullable: false},
		{Name: "amount", Type: "numeric", Nullable: true},
	}

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "transactions", columns)
	require.NoError(t, err)
	require.NotNil(t, schema)

	assert.NotEmpty(t, schema.ID)
	assert.Equal(t, connID, schema.ConnectionID)
	assert.Equal(t, "transactions", schema.TableName)
	assert.Len(t, schema.Columns, 2)
	assert.Equal(t, "id", schema.Columns[0].Name)
	assert.Equal(t, "uuid", schema.Columns[0].Type)
	assert.False(t, schema.Columns[0].Nullable)
	assert.Equal(t, "amount", schema.Columns[1].Name)
	assert.True(t, schema.Columns[1].Nullable)
	assert.False(t, schema.DiscoveredAt.IsZero())
}

func TestNewDiscoveredSchema_NilConnectionID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	schema, err := entities.NewDiscoveredSchema(ctx, uuid.Nil, "transactions", nil)
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.Contains(t, err.Error(), "connection id")
}

func TestNewDiscoveredSchema_EmptyTableName(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "", nil)
	require.Error(t, err)
	assert.Nil(t, schema)
	assert.Contains(t, err.Error(), "table name")
}

func TestNewDiscoveredSchema_NilColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "transactions", nil)
	require.NoError(t, err)
	require.NotNil(t, schema)
	// nil columns are initialized to empty slice
	assert.NotNil(t, schema.Columns)
	assert.Empty(t, schema.Columns)
}

func TestNewDiscoveredSchema_EmptyColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "transactions", []entities.ColumnInfo{})
	require.NoError(t, err)
	require.NotNil(t, schema)
	assert.Empty(t, schema.Columns)
}

func TestDiscoveredSchema_ColumnsJSON(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()
	columns := []entities.ColumnInfo{
		{Name: "id", Type: "uuid", Nullable: false},
		{Name: "name", Type: "varchar", Nullable: true},
	}

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "users", columns)
	require.NoError(t, err)

	data, err := schema.ColumnsJSON()
	require.NoError(t, err)
	require.NotNil(t, data)

	var parsed []entities.ColumnInfo
	err = json.Unmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Len(t, parsed, 2)
	assert.Equal(t, "id", parsed[0].Name)
	assert.Equal(t, "uuid", parsed[0].Type)
	assert.False(t, parsed[0].Nullable)
	assert.Equal(t, "name", parsed[1].Name)
	assert.True(t, parsed[1].Nullable)
}

func TestDiscoveredSchema_ColumnsJSON_NilColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	connID := uuid.New()

	schema, err := entities.NewDiscoveredSchema(ctx, connID, "users", nil)
	require.NoError(t, err)

	data, err := schema.ColumnsJSON()
	require.NoError(t, err)
	// nil columns are initialized to empty slice, so ColumnsJSON returns "[]"
	assert.Equal(t, "[]", string(data))
}

func TestDiscoveredSchema_ColumnsJSON_NilReceiver(t *testing.T) {
	t.Parallel()

	var schema *entities.DiscoveredSchema

	data, err := schema.ColumnsJSON()
	require.NoError(t, err)
	assert.Equal(t, "[]", string(data))
}

func TestColumnInfo_JSON_OmitsEmptyTypeAndNullable(t *testing.T) {
	t.Parallel()

	t.Run("name only omits type and nullable", func(t *testing.T) {
		t.Parallel()

		col := entities.ColumnInfo{Name: "id"}
		data, err := json.Marshal(col)
		require.NoError(t, err)
		assert.JSONEq(t, `{"name":"id"}`, string(data))
	})

	t.Run("all fields present", func(t *testing.T) {
		t.Parallel()

		col := entities.ColumnInfo{Name: "id", Type: "uuid", Nullable: true}
		data, err := json.Marshal(col)
		require.NoError(t, err)
		assert.JSONEq(t, `{"name":"id","type":"uuid","nullable":true}`, string(data))
	})
}
