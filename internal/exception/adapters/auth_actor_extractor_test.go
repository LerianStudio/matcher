// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package adapters

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

func TestNewAuthActorExtractor(t *testing.T) {
	t.Parallel()

	extractor := NewAuthActorExtractor()

	require.NotNil(t, extractor)
}

func TestAuthActorExtractor_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.ActorExtractor = (*AuthActorExtractor)(nil)

	extractor := NewAuthActorExtractor()
	assert.NotNil(t, extractor)
}

func TestAuthActorExtractor_GetActor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ctx      context.Context
		expected string
	}{
		{
			name:     "nil context returns empty string",
			ctx:      nil,
			expected: "",
		},
		{
			name:     "context without user ID returns empty string",
			ctx:      context.Background(),
			expected: "",
		},
		{
			name:     "context with user ID returns user ID",
			ctx:      context.WithValue(context.Background(), auth.UserIDKey, "user-123"),
			expected: "user-123",
		},
		{
			name:     "context with whitespace user ID gets trimmed",
			ctx:      context.WithValue(context.Background(), auth.UserIDKey, "  user-456  "),
			expected: "user-456",
		},
		{
			name:     "context with only whitespace returns empty string",
			ctx:      context.WithValue(context.Background(), auth.UserIDKey, "   "),
			expected: "",
		},
		{
			name:     "context with empty string returns empty string",
			ctx:      context.WithValue(context.Background(), auth.UserIDKey, ""),
			expected: "",
		},
		{
			name: "context with UUID user ID",
			ctx: context.WithValue(
				context.Background(),
				auth.UserIDKey,
				"550e8400-e29b-41d4-a716-446655440000",
			),
			expected: "550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			extractor := NewAuthActorExtractor()
			result := extractor.GetActor(tt.ctx)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAuthActorExtractor_GetActor_WrongValueType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value any
	}{
		{"integer value", 123},
		{"bool value", true},
		{"slice value", []string{"user"}},
		{"map value", map[string]string{"id": "user"}},
		{"nil value", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.WithValue(context.Background(), auth.UserIDKey, tt.value)
			extractor := NewAuthActorExtractor()
			result := extractor.GetActor(ctx)

			assert.Empty(t, result)
		})
	}
}
