// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package http

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorResponse_JSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		response CursorResponse
		wantKeys []string
	}{
		{
			name: "full response with all fields",
			response: CursorResponse{
				NextCursor: "next123",
				PrevCursor: "prev123",
				Limit:      20,
				HasMore:    true,
			},
			wantKeys: []string{"nextCursor", "prevCursor", "limit", "hasMore"},
		},
		{
			name: "omits empty cursors",
			response: CursorResponse{
				Limit:   20,
				HasMore: false,
			},
			wantKeys: []string{"limit", "hasMore"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tc.response)
			require.NoError(t, err)

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(data, &decoded))

			for _, key := range tc.wantKeys {
				assert.Contains(t, decoded, key)
			}
		})
	}
}

func TestCursorResponse_Defaults(t *testing.T) {
	t.Parallel()

	var response CursorResponse

	assert.Empty(t, response.NextCursor)
	assert.Empty(t, response.PrevCursor)
	assert.Zero(t, response.Limit)
	assert.False(t, response.HasMore)
}
