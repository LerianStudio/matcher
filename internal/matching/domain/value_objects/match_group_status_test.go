// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchGroupStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchGroupStatus
		expected string
	}{
		{
			name:     "Proposed status has correct value",
			status:   value_objects.MatchGroupStatusProposed,
			expected: "PROPOSED",
		},
		{
			name:     "Confirmed status has correct value",
			status:   value_objects.MatchGroupStatusConfirmed,
			expected: "CONFIRMED",
		},
		{
			name:     "Rejected status has correct value",
			status:   value_objects.MatchGroupStatusRejected,
			expected: "REJECTED",
		},
		{
			name:     "Revoked status has correct value",
			status:   value_objects.MatchGroupStatusRevoked,
			expected: "REVOKED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestMatchGroupStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchGroupStatus
		expected bool
	}{
		{
			name:     "Proposed is valid",
			status:   value_objects.MatchGroupStatusProposed,
			expected: true,
		},
		{
			name:     "Confirmed is valid",
			status:   value_objects.MatchGroupStatusConfirmed,
			expected: true,
		},
		{
			name:     "Rejected is valid",
			status:   value_objects.MatchGroupStatusRejected,
			expected: true,
		},
		{
			name:     "Revoked is valid",
			status:   value_objects.MatchGroupStatusRevoked,
			expected: true,
		},
		{
			name:     "Empty string is invalid",
			status:   value_objects.MatchGroupStatus(""),
			expected: false,
		},
		{
			name:     "Random string is invalid",
			status:   value_objects.MatchGroupStatus("RANDOM"),
			expected: false,
		},
		{
			name:     "Lowercase proposed is invalid",
			status:   value_objects.MatchGroupStatus("proposed"),
			expected: false,
		},
		{
			name:     "Lowercase confirmed is invalid",
			status:   value_objects.MatchGroupStatus("confirmed"),
			expected: false,
		},
		{
			name:     "Lowercase rejected is invalid",
			status:   value_objects.MatchGroupStatus("rejected"),
			expected: false,
		},
		{
			name:     "Mixed case is invalid",
			status:   value_objects.MatchGroupStatus("Proposed"),
			expected: false,
		},
		{
			name:     "Similar but wrong value is invalid",
			status:   value_objects.MatchGroupStatus("PROPOSE"),
			expected: false,
		},
		{
			name:     "Whitespace is invalid",
			status:   value_objects.MatchGroupStatus(" PROPOSED"),
			expected: false,
		},
		{
			name:     "Unknown status is invalid",
			status:   value_objects.MatchGroupStatus("UNKNOWN"),
			expected: false,
		},
		{
			name:     "Pending is not a valid status",
			status:   value_objects.MatchGroupStatus("PENDING"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.IsValid())
		})
	}
}

func TestMatchGroupStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchGroupStatus
		expected string
	}{
		{
			name:     "Proposed returns PROPOSED",
			status:   value_objects.MatchGroupStatusProposed,
			expected: "PROPOSED",
		},
		{
			name:     "Confirmed returns CONFIRMED",
			status:   value_objects.MatchGroupStatusConfirmed,
			expected: "CONFIRMED",
		},
		{
			name:     "Rejected returns REJECTED",
			status:   value_objects.MatchGroupStatusRejected,
			expected: "REJECTED",
		},
		{
			name:     "Revoked returns REVOKED",
			status:   value_objects.MatchGroupStatusRevoked,
			expected: "REVOKED",
		},
		{
			name:     "Custom value returns itself",
			status:   value_objects.MatchGroupStatus("CUSTOM"),
			expected: "CUSTOM",
		},
		{
			name:     "Empty string returns empty",
			status:   value_objects.MatchGroupStatus(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestParseMatchGroupStatus_ValidStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected value_objects.MatchGroupStatus
	}{
		{
			name:     "Parse PROPOSED",
			input:    "PROPOSED",
			expected: value_objects.MatchGroupStatusProposed,
		},
		{
			name:     "Parse CONFIRMED",
			input:    "CONFIRMED",
			expected: value_objects.MatchGroupStatusConfirmed,
		},
		{
			name:     "Parse REJECTED",
			input:    "REJECTED",
			expected: value_objects.MatchGroupStatusRejected,
		},
		{
			name:     "Parse REVOKED",
			input:    "REVOKED",
			expected: value_objects.MatchGroupStatusRevoked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchGroupStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseMatchGroupStatus_InvalidStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Empty string",
			input: "",
		},
		{
			name:  "Lowercase proposed",
			input: "proposed",
		},
		{
			name:  "Lowercase confirmed",
			input: "confirmed",
		},
		{
			name:  "Lowercase rejected",
			input: "rejected",
		},
		{
			name:  "Mixed case Proposed",
			input: "Proposed",
		},
		{
			name:  "Random string",
			input: "RANDOM",
		},
		{
			name:  "Unknown status",
			input: "UNKNOWN",
		},
		{
			name:  "Similar but wrong PROPOSE",
			input: "PROPOSE",
		},
		{
			name:  "With leading whitespace",
			input: " PROPOSED",
		},
		{
			name:  "With trailing whitespace",
			input: "PROPOSED ",
		},
		{
			name:  "With surrounding whitespace",
			input: " PROPOSED ",
		},
		{
			name:  "Numeric string",
			input: "123",
		},
		{
			name:  "Special characters",
			input: "PROPOSED!",
		},
		{
			name:  "Pending is not valid",
			input: "PENDING",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchGroupStatus(tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidMatchGroupStatus)
			assert.Equal(t, value_objects.MatchGroupStatus(""), result)
		})
	}
}

func TestMatchGroupStatus_RoundTrip(t *testing.T) {
	t.Parallel()

	statuses := []value_objects.MatchGroupStatus{
		value_objects.MatchGroupStatusProposed,
		value_objects.MatchGroupStatusConfirmed,
		value_objects.MatchGroupStatusRejected,
		value_objects.MatchGroupStatusRevoked,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			stringVal := status.String()
			parsed, err := value_objects.ParseMatchGroupStatus(stringVal)
			require.NoError(t, err)
			assert.Equal(t, status, parsed)
		})
	}
}
