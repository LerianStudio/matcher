//go:build unit

package http

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/LerianStudio/matcher/internal/matching/adapters/http/dto"
	sharedhttp "github.com/LerianStudio/matcher/internal/shared/adapters/http"
	"github.com/LerianStudio/matcher/internal/testutil"
)

func TestListMatchGroupsResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []dto.MatchGroupResponse
		cursor   sharedhttp.CursorResponse
		expected ListMatchGroupsResponse
	}{
		{
			name:  "empty items",
			items: nil,
			cursor: sharedhttp.CursorResponse{
				Limit:   50,
				HasMore: false,
			},
			expected: ListMatchGroupsResponse{
				Items: nil,
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   50,
					HasMore: false,
				},
			},
		},
		{
			name:  "empty slice",
			items: []dto.MatchGroupResponse{},
			cursor: sharedhttp.CursorResponse{
				Limit:   20,
				HasMore: false,
			},
			expected: ListMatchGroupsResponse{
				Items: []dto.MatchGroupResponse{},
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   20,
					HasMore: false,
				},
			},
		},
		{
			name: "with items",
			items: []dto.MatchGroupResponse{
				{
					ID:         "11111111-1111-1111-1111-111111111111",
					ContextID:  "22222222-2222-2222-2222-222222222222",
					RunID:      "33333333-3333-3333-3333-333333333333",
					RuleID:     testutil.StringPtr("44444444-4444-4444-4444-444444444444"),
					Confidence: 85,
					Status:     "PROPOSED",
					Items:      []dto.MatchItemResponse{},
					CreatedAt:  "2025-01-01T00:00:00Z",
					UpdatedAt:  "2025-01-01T00:00:00Z",
				},
			},
			cursor: sharedhttp.CursorResponse{
				Limit:   100,
				HasMore: false,
			},
			expected: ListMatchGroupsResponse{
				Items: []dto.MatchGroupResponse{
					{
						ID:         "11111111-1111-1111-1111-111111111111",
						ContextID:  "22222222-2222-2222-2222-222222222222",
						RunID:      "33333333-3333-3333-3333-333333333333",
						RuleID:     testutil.StringPtr("44444444-4444-4444-4444-444444444444"),
						Confidence: 85,
						Status:     "PROPOSED",
						Items:      []dto.MatchItemResponse{},
						CreatedAt:  "2025-01-01T00:00:00Z",
						UpdatedAt:  "2025-01-01T00:00:00Z",
					},
				},
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   100,
					HasMore: false,
				},
			},
		},
		{
			name: "with pagination",
			items: []dto.MatchGroupResponse{
				{ID: "11111111-1111-1111-1111-111111111111", Items: []dto.MatchItemResponse{}},
				{ID: "22222222-2222-2222-2222-222222222222", Items: []dto.MatchItemResponse{}},
			},
			cursor: sharedhttp.CursorResponse{
				NextCursor: "abc123",
				Limit:      10,
				HasMore:    true,
			},
			expected: ListMatchGroupsResponse{
				Items: []dto.MatchGroupResponse{
					{ID: "11111111-1111-1111-1111-111111111111", Items: []dto.MatchItemResponse{}},
					{ID: "22222222-2222-2222-2222-222222222222", Items: []dto.MatchItemResponse{}},
				},
				CursorResponse: sharedhttp.CursorResponse{
					NextCursor: "abc123",
					Limit:      10,
					HasMore:    true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			response := ListMatchGroupsResponse{
				Items:          tt.items,
				CursorResponse: tt.cursor,
			}

			assert.Equal(t, tt.expected.Limit, response.Limit)
			assert.Equal(t, tt.expected.HasMore, response.HasMore)
			assert.Equal(t, tt.expected.NextCursor, response.NextCursor)
			assert.Equal(t, tt.expected.Items, response.Items)
		})
	}
}

func TestListMatchGroupsResponseFieldsPresent(t *testing.T) {
	t.Parallel()

	groupDTO := dto.MatchGroupResponse{
		ID:         "11111111-1111-1111-1111-111111111111",
		ContextID:  "22222222-2222-2222-2222-222222222222",
		RunID:      "33333333-3333-3333-3333-333333333333",
		RuleID:     testutil.StringPtr("44444444-4444-4444-4444-444444444444"),
		Confidence: 75,
		Status:     "CONFIRMED",
		Items:      []dto.MatchItemResponse{},
		CreatedAt:  "2025-01-01T00:00:00Z",
		UpdatedAt:  "2025-01-01T00:00:00Z",
	}

	response := ListMatchGroupsResponse{
		Items: []dto.MatchGroupResponse{groupDTO},
		CursorResponse: sharedhttp.CursorResponse{
			Limit:   50,
			HasMore: false,
		},
	}

	assert.NotNil(t, response.Items)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, groupDTO.ID, response.Items[0].ID)
	assert.Equal(t, groupDTO.ContextID, response.Items[0].ContextID)
	assert.Equal(t, groupDTO.RunID, response.Items[0].RunID)
	assert.Equal(t, groupDTO.RuleID, response.Items[0].RuleID)
	assert.Equal(t, groupDTO.Confidence, response.Items[0].Confidence)
	assert.Equal(t, groupDTO.Status, response.Items[0].Status)
	assert.Equal(t, groupDTO.CreatedAt, response.Items[0].CreatedAt)
	assert.Equal(t, groupDTO.UpdatedAt, response.Items[0].UpdatedAt)
	assert.Empty(t, response.Items[0].Items)
	assert.Equal(t, 50, response.Limit)
	assert.False(t, response.HasMore)
}

func TestListMatchRunsResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		items    []dto.MatchRunResponse
		cursor   sharedhttp.CursorResponse
		expected ListMatchRunsResponse
	}{
		{
			name:  "nil items",
			items: nil,
			cursor: sharedhttp.CursorResponse{
				Limit:   50,
				HasMore: false,
			},
			expected: ListMatchRunsResponse{
				Items: nil,
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   50,
					HasMore: false,
				},
			},
		},
		{
			name:  "empty slice",
			items: []dto.MatchRunResponse{},
			cursor: sharedhttp.CursorResponse{
				Limit:   20,
				HasMore: false,
			},
			expected: ListMatchRunsResponse{
				Items: []dto.MatchRunResponse{},
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   20,
					HasMore: false,
				},
			},
		},
		{
			name: "with items",
			items: []dto.MatchRunResponse{
				{
					ID:          "11111111-1111-1111-1111-111111111111",
					ContextID:   "22222222-2222-2222-2222-222222222222",
					Mode:        "COMMIT",
					Status:      "COMPLETED",
					StartedAt:   "2025-01-15T10:30:00Z",
					CompletedAt: testutil.StringPtr("2025-01-15T10:35:00Z"),
					Stats:       map[string]int{"matched": 100, "unmatched": 5},
					CreatedAt:   "2025-01-15T10:30:00Z",
					UpdatedAt:   "2025-01-15T10:35:00Z",
				},
			},
			cursor: sharedhttp.CursorResponse{
				Limit:   100,
				HasMore: false,
			},
			expected: ListMatchRunsResponse{
				Items: []dto.MatchRunResponse{
					{
						ID:          "11111111-1111-1111-1111-111111111111",
						ContextID:   "22222222-2222-2222-2222-222222222222",
						Mode:        "COMMIT",
						Status:      "COMPLETED",
						StartedAt:   "2025-01-15T10:30:00Z",
						CompletedAt: testutil.StringPtr("2025-01-15T10:35:00Z"),
						Stats:       map[string]int{"matched": 100, "unmatched": 5},
						CreatedAt:   "2025-01-15T10:30:00Z",
						UpdatedAt:   "2025-01-15T10:35:00Z",
					},
				},
				CursorResponse: sharedhttp.CursorResponse{
					Limit:   100,
					HasMore: false,
				},
			},
		},
		{
			name: "with pagination",
			items: []dto.MatchRunResponse{
				{ID: "11111111-1111-1111-1111-111111111111", Stats: map[string]int{}},
				{ID: "22222222-2222-2222-2222-222222222222", Stats: map[string]int{}},
			},
			cursor: sharedhttp.CursorResponse{
				NextCursor: "abc123",
				Limit:      10,
				HasMore:    true,
			},
			expected: ListMatchRunsResponse{
				Items: []dto.MatchRunResponse{
					{ID: "11111111-1111-1111-1111-111111111111", Stats: map[string]int{}},
					{ID: "22222222-2222-2222-2222-222222222222", Stats: map[string]int{}},
				},
				CursorResponse: sharedhttp.CursorResponse{
					NextCursor: "abc123",
					Limit:      10,
					HasMore:    true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			response := ListMatchRunsResponse{
				Items:          tt.items,
				CursorResponse: tt.cursor,
			}

			assert.Equal(t, tt.expected.Limit, response.Limit)
			assert.Equal(t, tt.expected.HasMore, response.HasMore)
			assert.Equal(t, tt.expected.NextCursor, response.NextCursor)
			assert.Equal(t, tt.expected.Items, response.Items)
		})
	}
}

func TestListMatchRunsResponseFieldsPresent(t *testing.T) {
	t.Parallel()

	runDTO := dto.MatchRunResponse{
		ID:          "11111111-1111-1111-1111-111111111111",
		ContextID:   "22222222-2222-2222-2222-222222222222",
		Mode:        "DRY_RUN",
		Status:      "COMPLETED",
		StartedAt:   "2025-01-15T10:30:00Z",
		CompletedAt: testutil.StringPtr("2025-01-15T10:35:00Z"),
		Stats:       map[string]int{"matched": 50, "unmatched": 3},
		CreatedAt:   "2025-01-15T10:30:00Z",
		UpdatedAt:   "2025-01-15T10:35:00Z",
	}

	response := ListMatchRunsResponse{
		Items: []dto.MatchRunResponse{runDTO},
		CursorResponse: sharedhttp.CursorResponse{
			Limit:   50,
			HasMore: false,
		},
	}

	assert.NotNil(t, response.Items)
	assert.Len(t, response.Items, 1)
	assert.Equal(t, runDTO.ID, response.Items[0].ID)
	assert.Equal(t, runDTO.ContextID, response.Items[0].ContextID)
	assert.Equal(t, runDTO.Mode, response.Items[0].Mode)
	assert.Equal(t, runDTO.Status, response.Items[0].Status)
	assert.Equal(t, runDTO.StartedAt, response.Items[0].StartedAt)
	assert.Equal(t, runDTO.CompletedAt, response.Items[0].CompletedAt)
	assert.Equal(t, runDTO.Stats, response.Items[0].Stats)
	assert.Equal(t, runDTO.CreatedAt, response.Items[0].CreatedAt)
	assert.Equal(t, runDTO.UpdatedAt, response.Items[0].UpdatedAt)
	assert.Equal(t, 50, response.Limit)
	assert.False(t, response.HasMore)
}
