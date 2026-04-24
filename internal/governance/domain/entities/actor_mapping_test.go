// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package entities

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewActorMapping_ValidInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	displayName := "John Doe"
	email := "john@example.com"

	mapping, err := NewActorMapping(ctx, "actor-123", &displayName, &email)
	require.NoError(t, err)
	require.NotNil(t, mapping)

	assert.Equal(t, "actor-123", mapping.ActorID)
	require.NotNil(t, mapping.DisplayName)
	assert.Equal(t, "John Doe", *mapping.DisplayName)
	require.NotNil(t, mapping.Email)
	assert.Equal(t, "john@example.com", *mapping.Email)
	assert.False(t, mapping.CreatedAt.IsZero())
	assert.False(t, mapping.UpdatedAt.IsZero())
	assert.Equal(t, mapping.CreatedAt, mapping.UpdatedAt)
}

func TestNewActorMapping_TrimsActorID(t *testing.T) {
	t.Parallel()

	mapping, err := NewActorMapping(context.Background(), "  actor-456  ", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, mapping)
	assert.Equal(t, "actor-456", mapping.ActorID)
}

func TestNewActorMapping_NilOptionalFields(t *testing.T) {
	t.Parallel()

	mapping, err := NewActorMapping(context.Background(), "actor-789", nil, nil)
	require.NoError(t, err)
	require.NotNil(t, mapping)

	assert.Equal(t, "actor-789", mapping.ActorID)
	assert.Nil(t, mapping.DisplayName)
	assert.Nil(t, mapping.Email)
}

func TestNewActorMapping_EmptyActorID(t *testing.T) {
	t.Parallel()

	_, err := NewActorMapping(context.Background(), "", nil, nil)
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestNewActorMapping_WhitespaceActorID(t *testing.T) {
	t.Parallel()

	_, err := NewActorMapping(context.Background(), "   ", nil, nil)
	require.ErrorIs(t, err, ErrActorIDRequired)
}

func TestNewActorMapping_ActorIDTooLong(t *testing.T) {
	t.Parallel()

	longActorID := strings.Repeat("a", MaxActorMappingActorIDLength+1)
	_, err := NewActorMapping(context.Background(), longActorID, nil, nil)
	require.ErrorIs(t, err, ErrActorIDExceedsMaxLen)
}

func TestNewActorMapping_ActorIDAtMaxLength(t *testing.T) {
	t.Parallel()

	exactActorID := strings.Repeat("a", MaxActorMappingActorIDLength)
	mapping, err := NewActorMapping(context.Background(), exactActorID, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, mapping)
	assert.Equal(t, exactActorID, mapping.ActorID)
}

func TestActorMapping_IsRedacted_BeforePseudonymization(t *testing.T) {
	t.Parallel()

	displayName := "Real Name"
	email := "real@example.com"

	mapping, err := NewActorMapping(context.Background(), "actor-003", &displayName, &email)
	require.NoError(t, err)
	assert.False(t, mapping.IsRedacted())
}

func TestActorMapping_IsRedacted_BothFieldsRedacted(t *testing.T) {
	t.Parallel()

	redacted := "[REDACTED]"

	mapping, err := NewActorMapping(context.Background(), "actor-001", &redacted, &redacted)
	require.NoError(t, err)
	assert.True(t, mapping.IsRedacted())
}

func TestActorMapping_IsRedacted_NilFields(t *testing.T) {
	t.Parallel()

	mapping, err := NewActorMapping(context.Background(), "actor-004", nil, nil)
	require.NoError(t, err)
	assert.False(t, mapping.IsRedacted(), "nil fields should not be considered redacted")
}

func TestActorMapping_IsRedacted_PartialRedaction(t *testing.T) {
	t.Parallel()

	redacted := "[REDACTED]"
	realEmail := "real@example.com"

	mapping, err := NewActorMapping(context.Background(), "actor-005", &redacted, &realEmail)
	require.NoError(t, err)
	assert.False(t, mapping.IsRedacted(), "partial redaction should return false")
}

func TestActorMapping_IsRedacted_OnlyEmailRedacted(t *testing.T) {
	t.Parallel()

	realName := "Real Name"
	redacted := "[REDACTED]"

	mapping, err := NewActorMapping(context.Background(), "actor-006", &realName, &redacted)
	require.NoError(t, err)
	assert.False(t, mapping.IsRedacted(), "partial redaction should return false")
}

func TestActorMappingSentinelErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{"ErrActorIDRequired", ErrActorIDRequired},
		{"ErrActorIDExceedsMaxLen", ErrActorIDExceedsMaxLen},
		{"ErrNilActorMappingRepository", ErrNilActorMappingRepository},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, tt.err)
			require.NotEmpty(t, tt.err.Error())
		})
	}
}

func TestSafeActorIDPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "empty string", input: "", expected: "***"},
		{name: "single char", input: "x", expected: "x***"},
		{name: "short ID", input: "ab", expected: "a***"},
		{name: "exact 4 chars", input: "abcd", expected: "a***"},
		{name: "5 chars", input: "abcde", expected: "abcd***"},
		{name: "long ID (email)", input: "user@example.com", expected: "user***"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, SafeActorIDPrefix(tt.input))
		})
	}
}
