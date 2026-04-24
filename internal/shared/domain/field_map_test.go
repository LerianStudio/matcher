// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package shared_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestFieldMapCreation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		fieldMap shared.FieldMap
		validate func(t *testing.T, fm shared.FieldMap)
	}{
		{
			name: "all fields populated",
			fieldMap: shared.FieldMap{
				ID:        uuid.MustParse("11111111-1111-1111-1111-111111111111"),
				ContextID: uuid.MustParse("22222222-2222-2222-2222-222222222222"),
				SourceID:  uuid.MustParse("33333333-3333-3333-3333-333333333333"),
				Mapping:   map[string]any{"field1": "value1", "field2": 123},
				Version:   1,
				CreatedAt: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
				UpdatedAt: time.Date(2024, 1, 16, 11, 45, 0, 0, time.UTC),
			},
			validate: func(t *testing.T, fm shared.FieldMap) {
				t.Helper()
				assert.Equal(t, "11111111-1111-1111-1111-111111111111", fm.ID.String())
				assert.Equal(t, "22222222-2222-2222-2222-222222222222", fm.ContextID.String())
				assert.Equal(t, "33333333-3333-3333-3333-333333333333", fm.SourceID.String())
				assert.Equal(t, "value1", fm.Mapping["field1"])
				assert.Equal(t, 123, fm.Mapping["field2"])
				assert.Equal(t, 1, fm.Version)
				assert.Equal(t, 2024, fm.CreatedAt.Year())
				assert.Equal(t, 2024, fm.UpdatedAt.Year())
			},
		},
		{
			name:     "zero values",
			fieldMap: shared.FieldMap{},
			validate: func(t *testing.T, fm shared.FieldMap) {
				t.Helper()
				assert.Equal(t, uuid.Nil, fm.ID)
				assert.Equal(t, uuid.Nil, fm.ContextID)
				assert.Equal(t, uuid.Nil, fm.SourceID)
				assert.Nil(t, fm.Mapping)
				assert.Equal(t, 0, fm.Version)
				assert.True(t, fm.CreatedAt.IsZero())
				assert.True(t, fm.UpdatedAt.IsZero())
			},
		},
		{
			name: "nil mapping",
			fieldMap: shared.FieldMap{
				ID:      uuid.New(),
				Mapping: nil,
				Version: 5,
			},
			validate: func(t *testing.T, fm shared.FieldMap) {
				t.Helper()
				assert.Nil(t, fm.Mapping)
				assert.NotEqual(t, uuid.Nil, fm.ID)
				assert.Equal(t, 5, fm.Version)
			},
		},
		{
			name: "empty mapping",
			fieldMap: shared.FieldMap{
				Mapping: map[string]any{},
			},
			validate: func(t *testing.T, fm shared.FieldMap) {
				t.Helper()
				require.NotNil(t, fm.Mapping)
				assert.Empty(t, fm.Mapping)
			},
		},
		{
			name: "complex nested mapping",
			fieldMap: shared.FieldMap{
				ID: uuid.New(),
				Mapping: map[string]any{
					"simple":  "value",
					"number":  42,
					"float":   3.14,
					"boolean": true,
					"nested": map[string]any{
						"level2": map[string]any{
							"level3": "deep value",
						},
					},
					"array": []any{"item1", "item2", 123},
					"mixed": []any{
						map[string]any{"key": "val"},
						"string",
						999,
					},
				},
				Version: 10,
			},
			validate: func(t *testing.T, fm shared.FieldMap) {
				t.Helper()
				assert.Equal(t, "value", fm.Mapping["simple"])
				assert.Equal(t, 42, fm.Mapping["number"])
				assert.InDelta(t, 3.14, fm.Mapping["float"], 0.0001)
				assert.Equal(t, true, fm.Mapping["boolean"])

				nested, ok := fm.Mapping["nested"].(map[string]any)
				require.True(t, ok)
				level2, ok := nested["level2"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "deep value", level2["level3"])

				arr, ok := fm.Mapping["array"].([]any)
				require.True(t, ok)
				assert.Len(t, arr, 3)
				assert.Equal(t, "item1", arr[0])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.validate(t, tc.fieldMap)
		})
	}
}

func TestReconciliationSourceCreation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		source   shared.ReconciliationSource
		validate func(t *testing.T, rs shared.ReconciliationSource)
	}{
		{
			name: "all fields populated",
			source: shared.ReconciliationSource{
				ID:        uuid.MustParse("44444444-4444-4444-4444-444444444444"),
				ContextID: uuid.MustParse("55555555-5555-5555-5555-555555555555"),
				Name:      "Bank Statement Source",
				Type:      shared.SourceType("csv"),
				Config:    map[string]any{"delimiter": ",", "encoding": "UTF-8"},
				CreatedAt: time.Date(2024, 2, 20, 14, 0, 0, 0, time.UTC),
				UpdatedAt: time.Date(2024, 2, 21, 15, 30, 0, 0, time.UTC),
			},
			validate: func(t *testing.T, rs shared.ReconciliationSource) {
				t.Helper()
				assert.Equal(t, "44444444-4444-4444-4444-444444444444", rs.ID.String())
				assert.Equal(t, "55555555-5555-5555-5555-555555555555", rs.ContextID.String())
				assert.Equal(t, "Bank Statement Source", rs.Name)
				assert.Equal(t, shared.SourceType("csv"), rs.Type)
				assert.Equal(t, ",", rs.Config["delimiter"])
				assert.Equal(t, "UTF-8", rs.Config["encoding"])
				assert.Equal(t, 2024, rs.CreatedAt.Year())
			},
		},
		{
			name:   "zero values",
			source: shared.ReconciliationSource{},
			validate: func(t *testing.T, rs shared.ReconciliationSource) {
				t.Helper()
				assert.Equal(t, uuid.Nil, rs.ID)
				assert.Equal(t, uuid.Nil, rs.ContextID)
				assert.Empty(t, rs.Name)
				assert.Empty(t, rs.Type)
				assert.Nil(t, rs.Config)
				assert.True(t, rs.CreatedAt.IsZero())
				assert.True(t, rs.UpdatedAt.IsZero())
			},
		},
		{
			name: "nil config",
			source: shared.ReconciliationSource{
				ID:     uuid.New(),
				Name:   "Test Source",
				Type:   shared.SourceType("api"),
				Config: nil,
			},
			validate: func(t *testing.T, rs shared.ReconciliationSource) {
				t.Helper()
				assert.Nil(t, rs.Config)
				assert.Equal(t, "Test Source", rs.Name)
				assert.Equal(t, shared.SourceType("api"), rs.Type)
			},
		},
		{
			name: "empty config",
			source: shared.ReconciliationSource{
				Config: map[string]any{},
			},
			validate: func(t *testing.T, rs shared.ReconciliationSource) {
				t.Helper()
				require.NotNil(t, rs.Config)
				assert.Empty(t, rs.Config)
			},
		},
		{
			name: "complex config",
			source: shared.ReconciliationSource{
				ID:   uuid.New(),
				Name: "Complex Source",
				Type: shared.SourceType("webhook"),
				Config: map[string]any{
					"endpoint":    "https://api.example.com/webhook",
					"timeout_ms":  30000,
					"retry_count": 3,
					"headers": map[string]any{
						"Authorization": "Bearer token",
						"Content-Type":  "application/json",
					},
					"mapping": map[string]any{
						"fields": []any{"id", "amount", "date"},
						"transforms": map[string]any{
							"amount": "parseFloat",
							"date":   "parseDate",
						},
					},
					"enabled": true,
				},
			},
			validate: func(t *testing.T, rs shared.ReconciliationSource) {
				t.Helper()
				assert.Equal(t, "https://api.example.com/webhook", rs.Config["endpoint"])
				assert.Equal(t, 30000, rs.Config["timeout_ms"])
				assert.Equal(t, true, rs.Config["enabled"])

				headers, ok := rs.Config["headers"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Bearer token", headers["Authorization"])

				mapping, ok := rs.Config["mapping"].(map[string]any)
				require.True(t, ok)
				fields, ok := mapping["fields"].([]any)
				require.True(t, ok)
				assert.Len(t, fields, 3)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.validate(t, tc.source)
		})
	}
}

func TestFieldMapEquality(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	contextID := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
	sourceID := uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")
	now := time.Now().UTC()

	fm1 := shared.FieldMap{
		ID:        id,
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping:   map[string]any{"key": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	fm2 := shared.FieldMap{
		ID:        id,
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping:   map[string]any{"key": "value"},
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}

	assert.Equal(t, fm1.ID, fm2.ID)
	assert.Equal(t, fm1.ContextID, fm2.ContextID)
	assert.Equal(t, fm1.SourceID, fm2.SourceID)
	assert.Equal(t, fm1.Version, fm2.Version)
	assert.Equal(t, fm1.CreatedAt, fm2.CreatedAt)
	assert.Equal(t, fm1.UpdatedAt, fm2.UpdatedAt)
	assert.Equal(t, fm1.Mapping["key"], fm2.Mapping["key"])

	fm3 := shared.FieldMap{
		ID:      uuid.New(),
		Version: 2,
	}
	assert.NotEqual(t, fm1.ID, fm3.ID)
	assert.NotEqual(t, fm1.Version, fm3.Version)
}

func TestReconciliationSourceEquality(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("dddddddd-dddd-dddd-dddd-dddddddddddd")
	contextID := uuid.MustParse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")
	now := time.Now().UTC()

	rs1 := shared.ReconciliationSource{
		ID:        id,
		ContextID: contextID,
		Name:      "Source A",
		Type:      shared.SourceType("csv"),
		Config:    map[string]any{"key": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	rs2 := shared.ReconciliationSource{
		ID:        id,
		ContextID: contextID,
		Name:      "Source A",
		Type:      shared.SourceType("csv"),
		Config:    map[string]any{"key": "value"},
		CreatedAt: now,
		UpdatedAt: now,
	}

	assert.Equal(t, rs1.ID, rs2.ID)
	assert.Equal(t, rs1.ContextID, rs2.ContextID)
	assert.Equal(t, rs1.Name, rs2.Name)
	assert.Equal(t, rs1.Type, rs2.Type)
	assert.Equal(t, rs1.CreatedAt, rs2.CreatedAt)
	assert.Equal(t, rs1.UpdatedAt, rs2.UpdatedAt)
	assert.Equal(t, rs1.Config["key"], rs2.Config["key"])

	rs3 := shared.ReconciliationSource{
		ID:   uuid.New(),
		Name: "Source B",
		Type: shared.SourceType("api"),
	}
	assert.NotEqual(t, rs1.ID, rs3.ID)
	assert.NotEqual(t, rs1.Name, rs3.Name)
	assert.NotEqual(t, rs1.Type, rs3.Type)
}

func TestFieldMapJSONSerialization(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("12345678-1234-1234-1234-123456789012")
	contextID := uuid.MustParse("87654321-4321-4321-4321-210987654321")
	sourceID := uuid.MustParse("abcdefab-abcd-abcd-abcd-abcdefabcdef")
	createdAt := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 3, 16, 11, 0, 0, 0, time.UTC)

	original := shared.FieldMap{
		ID:        id,
		ContextID: contextID,
		SourceID:  sourceID,
		Mapping: map[string]any{
			"string":  "value",
			"number":  float64(42),
			"boolean": true,
		},
		Version:   3,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored shared.FieldMap

	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Equal(t, original.ID, restored.ID)
	assert.Equal(t, original.ContextID, restored.ContextID)
	assert.Equal(t, original.SourceID, restored.SourceID)
	assert.Equal(t, original.Version, restored.Version)
	assert.Equal(t, original.CreatedAt.UTC(), restored.CreatedAt.UTC())
	assert.Equal(t, original.UpdatedAt.UTC(), restored.UpdatedAt.UTC())
	assert.Equal(t, original.Mapping["string"], restored.Mapping["string"])
	assert.Equal(t, original.Mapping["number"], restored.Mapping["number"])
	assert.Equal(t, original.Mapping["boolean"], restored.Mapping["boolean"])
}

func TestReconciliationSourceJSONSerialization(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("fedcba98-7654-3210-fedc-ba9876543210")
	contextID := uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef")
	createdAt := time.Date(2024, 4, 20, 12, 30, 0, 0, time.UTC)
	updatedAt := time.Date(2024, 4, 21, 13, 45, 0, 0, time.UTC)

	original := shared.ReconciliationSource{
		ID:        id,
		ContextID: contextID,
		Name:      "API Source",
		Type:      shared.SourceType("rest"),
		Config: map[string]any{
			"url":     "https://api.example.com",
			"timeout": float64(5000),
			"headers": map[string]any{
				"Accept": "application/json",
			},
		},
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var restored shared.ReconciliationSource

	err = json.Unmarshal(data, &restored)
	require.NoError(t, err)

	assert.Equal(t, original.ID, restored.ID)
	assert.Equal(t, original.ContextID, restored.ContextID)
	assert.Equal(t, original.Name, restored.Name)
	assert.Equal(t, original.Type, restored.Type)
	assert.Equal(t, original.CreatedAt.UTC(), restored.CreatedAt.UTC())
	assert.Equal(t, original.UpdatedAt.UTC(), restored.UpdatedAt.UTC())
	assert.Equal(t, original.Config["url"], restored.Config["url"])
	assert.Equal(t, original.Config["timeout"], restored.Config["timeout"])

	headers, ok := restored.Config["headers"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "application/json", headers["Accept"])
}

func TestFieldMapWithEmptyUUIDs(t *testing.T) {
	t.Parallel()

	fm := shared.FieldMap{
		ID:        uuid.Nil,
		ContextID: uuid.Nil,
		SourceID:  uuid.Nil,
		Version:   1,
	}

	assert.Equal(t, "00000000-0000-0000-0000-000000000000", fm.ID.String())
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", fm.ContextID.String())
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", fm.SourceID.String())
	assert.Equal(t, 1, fm.Version)
}

func TestReconciliationSourceWithEmptyUUIDs(t *testing.T) {
	t.Parallel()

	rs := shared.ReconciliationSource{
		ID:        uuid.Nil,
		ContextID: uuid.Nil,
		Name:      "Test",
		Type:      shared.SourceType("csv"),
	}

	assert.Equal(t, "00000000-0000-0000-0000-000000000000", rs.ID.String())
	assert.Equal(t, "00000000-0000-0000-0000-000000000000", rs.ContextID.String())
	assert.Equal(t, "Test", rs.Name)
	assert.Equal(t, shared.SourceType("csv"), rs.Type)
}

func TestFieldMapWithZeroTime(t *testing.T) {
	t.Parallel()

	fm := shared.FieldMap{
		ID:        uuid.New(),
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
	}

	assert.True(t, fm.CreatedAt.IsZero())
	assert.True(t, fm.UpdatedAt.IsZero())
	assert.Equal(t, 1, fm.CreatedAt.Year())
	assert.NotEqual(t, uuid.Nil, fm.ID)
}

func TestReconciliationSourceWithZeroTime(t *testing.T) {
	t.Parallel()

	rs := shared.ReconciliationSource{
		ID:        uuid.New(),
		Name:      "Test",
		CreatedAt: time.Time{},
		UpdatedAt: time.Time{},
	}

	assert.True(t, rs.CreatedAt.IsZero())
	assert.True(t, rs.UpdatedAt.IsZero())
	assert.Equal(t, 1, rs.CreatedAt.Year())
	assert.NotEqual(t, uuid.Nil, rs.ID)
	assert.Equal(t, "Test", rs.Name)
}
