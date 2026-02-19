//go:build unit

package entities

import (
	"context"
	"testing"

	"github.com/LerianStudio/matcher/internal/testutil"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFieldMap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "1")
	sourceID := testutil.MustDeterministicUUID(t, "2")

	t.Run("creates valid field map", func(t *testing.T) {
		t.Parallel()

		input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
		fieldMap, err := NewFieldMap(ctx, contextID, sourceID, input)
		require.NoError(t, err)
		assert.NotEqual(t, uuid.Nil, fieldMap.ID)
		assert.Equal(t, contextID, fieldMap.ContextID)
		assert.Equal(t, sourceID, fieldMap.SourceID)
		assert.Equal(t, 1, fieldMap.Version)
	})

	t.Run("fails with nil context", func(t *testing.T) {
		t.Parallel()

		input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
		_, err := NewFieldMap(ctx, uuid.Nil, sourceID, input)
		require.Error(t, err)
	})

	t.Run("fails with nil source", func(t *testing.T) {
		t.Parallel()

		input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
		_, err := NewFieldMap(ctx, contextID, uuid.Nil, input)
		require.Error(t, err)
	})

	t.Run("fails with empty mapping", func(t *testing.T) {
		t.Parallel()

		input := CreateFieldMapInput{Mapping: map[string]any{}}
		_, err := NewFieldMap(ctx, contextID, sourceID, input)
		require.Error(t, err)
	})
}

func TestFieldMap_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "3")
	sourceID := testutil.MustDeterministicUUID(t, "4")
	createFieldMap := func(t *testing.T) *FieldMap {
		t.Helper()

		input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
		fieldMap, err := NewFieldMap(ctx, contextID, sourceID, input)
		require.NoError(t, err)

		return fieldMap
	}

	t.Run("updates mapping and increments version", func(t *testing.T) {
		t.Parallel()

		fieldMap := createFieldMap(t)
		update := UpdateFieldMapInput{Mapping: map[string]any{"currency": "txn_currency"}}
		err := fieldMap.Update(ctx, update)
		require.NoError(t, err)
		assert.Equal(t, 2, fieldMap.Version)
		assert.Equal(t, "txn_currency", fieldMap.Mapping["currency"])
	})

	t.Run("fails with empty mapping and does not mutate entity", func(t *testing.T) {
		t.Parallel()

		fieldMap := createFieldMap(t)

		// Capture original state before attempting update
		originalID := fieldMap.ID
		originalContextID := fieldMap.ContextID
		originalSourceID := fieldMap.SourceID
		originalMapping := make(map[string]any)
		for k, v := range fieldMap.Mapping {
			originalMapping[k] = v
		}
		originalVersion := fieldMap.Version
		originalCreatedAt := fieldMap.CreatedAt
		originalUpdatedAt := fieldMap.UpdatedAt

		update := UpdateFieldMapInput{Mapping: map[string]any{}}
		err := fieldMap.Update(ctx, update)

		// Verify error is returned
		require.Error(t, err)
		assert.Equal(t, ErrFieldMapMappingRequired, err)

		// Verify entity was not mutated
		assert.Equal(t, originalID, fieldMap.ID, "ID should not be mutated on error")
		assert.Equal(t, originalContextID, fieldMap.ContextID, "ContextID should not be mutated on error")
		assert.Equal(t, originalSourceID, fieldMap.SourceID, "SourceID should not be mutated on error")
		assert.Equal(t, originalMapping, fieldMap.Mapping, "Mapping should not be mutated on error")
		assert.Equal(t, originalVersion, fieldMap.Version, "Version should not be mutated on error")
		assert.Equal(t, originalCreatedAt, fieldMap.CreatedAt, "CreatedAt should not be mutated on error")
		assert.Equal(t, originalUpdatedAt, fieldMap.UpdatedAt, "UpdatedAt should not be mutated on error")
	})
}

func TestFieldMap_Update_NilReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	var nilFieldMap *FieldMap

	update := UpdateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
	err := nilFieldMap.Update(ctx, update)
	require.Error(t, err)
	assert.Equal(t, ErrFieldMapNil, err)
}

func TestFieldMap_MappingJSON_NilReceiver(t *testing.T) {
	t.Parallel()

	var nilFieldMap *FieldMap

	data, err := nilFieldMap.MappingJSON()
	require.NoError(t, err)
	assert.Equal(t, []byte("null"), data)
}

func TestFieldMap_Update_NilMapping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "5")
	sourceID := testutil.MustDeterministicUUID(t, "6")
	input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
	fieldMap, err := NewFieldMap(ctx, contextID, sourceID, input)
	require.NoError(t, err)

	// Capture original state before attempting update
	originalID := fieldMap.ID
	originalContextID := fieldMap.ContextID
	originalSourceID := fieldMap.SourceID
	originalMapping := make(map[string]any)
	for k, v := range fieldMap.Mapping {
		originalMapping[k] = v
	}
	originalVersion := fieldMap.Version
	originalCreatedAt := fieldMap.CreatedAt
	originalUpdatedAt := fieldMap.UpdatedAt

	update := UpdateFieldMapInput{Mapping: nil}
	err = fieldMap.Update(ctx, update)

	// Verify error is returned
	require.Error(t, err)
	assert.Equal(t, ErrFieldMapMappingRequired, err)

	// Verify entity was not mutated
	assert.Equal(t, originalID, fieldMap.ID, "ID should not be mutated on error")
	assert.Equal(t, originalContextID, fieldMap.ContextID, "ContextID should not be mutated on error")
	assert.Equal(t, originalSourceID, fieldMap.SourceID, "SourceID should not be mutated on error")
	assert.Equal(t, originalMapping, fieldMap.Mapping, "Mapping should not be mutated on error")
	assert.Equal(t, originalVersion, fieldMap.Version, "Version should not be mutated on error")
	assert.Equal(t, originalCreatedAt, fieldMap.CreatedAt, "CreatedAt should not be mutated on error")
	assert.Equal(t, originalUpdatedAt, fieldMap.UpdatedAt, "UpdatedAt should not be mutated on error")
}

func TestNewFieldMap_EmptyStringValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "7")
	sourceID := testutil.MustDeterministicUUID(t, "8")

	input := CreateFieldMapInput{Mapping: map[string]any{"amount": ""}}
	_, err := NewFieldMap(ctx, contextID, sourceID, input)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFieldMapMappingValueEmpty)
	assert.Contains(t, err.Error(), "amount")
}

func TestNewFieldMap_NonStringValuesAllowed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "9")
	sourceID := testutil.MustDeterministicUUID(t, "10")

	input := CreateFieldMapInput{Mapping: map[string]any{
		"amount":   "txn_amount",
		"active":   true,
		"priority": 42,
		"nested":   map[string]any{"key": "value"},
	}}
	fieldMap, err := NewFieldMap(ctx, contextID, sourceID, input)
	require.NoError(t, err)
	assert.NotNil(t, fieldMap)
}

func TestFieldMap_Update_EmptyStringValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	contextID := testutil.MustDeterministicUUID(t, "11")
	sourceID := testutil.MustDeterministicUUID(t, "12")
	input := CreateFieldMapInput{Mapping: map[string]any{"amount": "txn_amount"}}
	fieldMap, err := NewFieldMap(ctx, contextID, sourceID, input)
	require.NoError(t, err)

	originalVersion := fieldMap.Version

	update := UpdateFieldMapInput{Mapping: map[string]any{"currency": ""}}
	err = fieldMap.Update(ctx, update)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFieldMapMappingValueEmpty)
	assert.Contains(t, err.Error(), "currency")
	assert.Equal(t, originalVersion, fieldMap.Version, "Version should not change on error")
}
