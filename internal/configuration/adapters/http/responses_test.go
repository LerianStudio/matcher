// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/configuration/adapters/http/dto"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
)

func TestToContextValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       []*entities.ReconciliationContext
		expectedLen int
		expectedIDs []string
	}{
		{
			name:        "nil input",
			input:       nil,
			expectedLen: 0,
			expectedIDs: []string{},
		},
		{
			name:        "empty slice",
			input:       []*entities.ReconciliationContext{},
			expectedLen: 0,
			expectedIDs: []string{},
		},
		{
			name: "with items",
			input: []*entities.ReconciliationContext{
				{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Name: "Context 1"},
				{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Name: "Context 2"},
			},
			expectedLen: 2,
			expectedIDs: []string{
				"11111111-1111-1111-1111-111111111111",
				"22222222-2222-2222-2222-222222222222",
			},
		},
		{
			name: "with nil items filtered out",
			input: []*entities.ReconciliationContext{
				{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Name: "Context 1"},
				nil,
				{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Name: "Context 2"},
			},
			expectedLen: 2,
			expectedIDs: []string{
				"11111111-1111-1111-1111-111111111111",
				"22222222-2222-2222-2222-222222222222",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := toContextValues(tt.input)

			assert.Len(t, result, tt.expectedLen)

			for i, ctx := range result {
				assert.Equal(t, tt.expectedIDs[i], ctx.ID)
			}
		})
	}
}

func TestToSourceValuesWithFieldMaps(t *testing.T) {
	t.Parallel()

	sourceID1 := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sourceID2 := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name             string
		input            []*entities.ReconciliationSource
		fieldMapsExist   map[uuid.UUID]bool
		expectedLen      int
		expectedIDs      []string
		expectedFieldMap []bool
	}{
		{
			name:             "nil input",
			input:            nil,
			fieldMapsExist:   map[uuid.UUID]bool{},
			expectedLen:      0,
			expectedIDs:      []string{},
			expectedFieldMap: []bool{},
		},
		{
			name:             "empty slice",
			input:            []*entities.ReconciliationSource{},
			fieldMapsExist:   map[uuid.UUID]bool{},
			expectedLen:      0,
			expectedIDs:      []string{},
			expectedFieldMap: []bool{},
		},
		{
			name: "with items and field maps",
			input: []*entities.ReconciliationSource{
				{ID: sourceID1, Name: "Source 1"},
				{ID: sourceID2, Name: "Source 2"},
			},
			fieldMapsExist: map[uuid.UUID]bool{sourceID1: true},
			expectedLen:    2,
			expectedIDs: []string{
				"11111111-1111-1111-1111-111111111111",
				"22222222-2222-2222-2222-222222222222",
			},
			expectedFieldMap: []bool{true, false},
		},
		{
			name: "with nil items filtered out",
			input: []*entities.ReconciliationSource{
				nil,
				{ID: sourceID1, Name: "Source 1"},
				nil,
			},
			fieldMapsExist:   map[uuid.UUID]bool{sourceID1: true},
			expectedLen:      1,
			expectedIDs:      []string{"11111111-1111-1111-1111-111111111111"},
			expectedFieldMap: []bool{true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := toSourceValuesWithFieldMaps(tt.input, tt.fieldMapsExist)

			assert.Len(t, result, tt.expectedLen)

			for i, src := range result {
				assert.Equal(t, tt.expectedIDs[i], src.ID)
				assert.Equal(t, tt.expectedFieldMap[i], src.HasFieldMaps)
			}
		})
	}
}

func TestToMatchRuleValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		input              []*entities.MatchRule
		expectedLen        int
		expectedIDs        []string
		expectedPriorities []int
	}{
		{
			name:               "nil input",
			input:              nil,
			expectedLen:        0,
			expectedIDs:        []string{},
			expectedPriorities: []int{},
		},
		{
			name:               "empty slice",
			input:              []*entities.MatchRule{},
			expectedLen:        0,
			expectedIDs:        []string{},
			expectedPriorities: []int{},
		},
		{
			name: "with items",
			input: []*entities.MatchRule{
				{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Priority: 1},
				{ID: uuid.MustParse("22222222-2222-2222-2222-222222222222"), Priority: 2},
			},
			expectedLen: 2,
			expectedIDs: []string{
				"11111111-1111-1111-1111-111111111111",
				"22222222-2222-2222-2222-222222222222",
			},
			expectedPriorities: []int{1, 2},
		},
		{
			name: "with nil items filtered out",
			input: []*entities.MatchRule{
				{ID: uuid.MustParse("11111111-1111-1111-1111-111111111111"), Priority: 1},
				nil,
			},
			expectedLen:        1,
			expectedIDs:        []string{"11111111-1111-1111-1111-111111111111"},
			expectedPriorities: []int{1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := toMatchRuleValues(tt.input)

			assert.Len(t, result, tt.expectedLen)

			for i, rule := range result {
				assert.Equal(t, tt.expectedIDs[i], rule.ID)
				assert.Equal(t, tt.expectedPriorities[i], rule.Priority)
			}
		})
	}
}

func TestListContextsResponse(t *testing.T) {
	t.Parallel()

	response := ListContextsResponse{
		Items: []dto.ReconciliationContextResponse{
			{ID: uuid.New().String(), Name: "Context 1"},
		},
		CursorResponse: sharedhttp.CursorResponse{
			Limit:      20,
			HasMore:    false,
			NextCursor: "cursor",
		},
	}

	assert.Len(t, response.Items, 1)
	assert.Equal(t, 20, response.Limit)
	assert.False(t, response.HasMore)
	assert.Equal(t, "cursor", response.NextCursor)
}

func TestListSourcesResponse(t *testing.T) {
	t.Parallel()

	response := ListSourcesResponse{
		Items: []dto.SourceWithFieldMapStatusResponse{
			{
				ReconciliationSourceResponse: dto.ReconciliationSourceResponse{
					ID:   uuid.New().String(),
					Name: "Source 1",
				},
				HasFieldMaps: true,
			},
		},
		CursorResponse: sharedhttp.CursorResponse{
			Limit:      50,
			HasMore:    true,
			NextCursor: "next-cursor",
		},
	}

	assert.Len(t, response.Items, 1)
	assert.Equal(t, 50, response.Limit)
	assert.True(t, response.HasMore)
	assert.Equal(t, "next-cursor", response.NextCursor)
	assert.True(t, response.Items[0].HasFieldMaps)
}

func TestListFieldMapsResponse(t *testing.T) {
	t.Parallel()

	response := ListFieldMapsResponse{
		Items: []dto.FieldMapResponse{
			{ID: uuid.New().String(), Mapping: map[string]any{"external_id": "id"}},
		},
	}

	assert.Len(t, response.Items, 1)
	assert.Equal(t, "id", response.Items[0].Mapping["external_id"])
}

func TestListMatchRulesResponse(t *testing.T) {
	t.Parallel()

	response := ListMatchRulesResponse{
		Items: []dto.MatchRuleResponse{
			{ID: uuid.New().String(), Priority: 1},
		},
		CursorResponse: sharedhttp.CursorResponse{
			Limit:   100,
			HasMore: false,
		},
	}

	assert.Len(t, response.Items, 1)
	assert.Equal(t, 100, response.Limit)
	assert.False(t, response.HasMore)
}
