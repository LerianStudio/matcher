// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestExtractionStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExtractionStatus
		expected string
	}{
		{
			name:     "Pending status has correct value",
			status:   value_objects.ExtractionStatusPending,
			expected: "PENDING",
		},
		{
			name:     "Submitted status has correct value",
			status:   value_objects.ExtractionStatusSubmitted,
			expected: "SUBMITTED",
		},
		{
			name:     "Extracting status has correct value",
			status:   value_objects.ExtractionStatusExtracting,
			expected: "EXTRACTING",
		},
		{
			name:     "Complete status has correct value",
			status:   value_objects.ExtractionStatusComplete,
			expected: "COMPLETE",
		},
		{
			name:     "Failed status has correct value",
			status:   value_objects.ExtractionStatusFailed,
			expected: "FAILED",
		},
		{
			name:     "Cancelled status has correct value",
			status:   value_objects.ExtractionStatusCancelled,
			expected: "CANCELLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestExtractionStatus_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExtractionStatus
		expected bool
	}{
		{
			name:     "Pending is valid",
			status:   value_objects.ExtractionStatusPending,
			expected: true,
		},
		{
			name:     "Submitted is valid",
			status:   value_objects.ExtractionStatusSubmitted,
			expected: true,
		},
		{
			name:     "Extracting is valid",
			status:   value_objects.ExtractionStatusExtracting,
			expected: true,
		},
		{
			name:     "Complete is valid",
			status:   value_objects.ExtractionStatusComplete,
			expected: true,
		},
		{
			name:     "Failed is valid",
			status:   value_objects.ExtractionStatusFailed,
			expected: true,
		},
		{
			name:     "Cancelled is valid",
			status:   value_objects.ExtractionStatusCancelled,
			expected: true,
		},
		{
			name:     "Empty string is invalid",
			status:   value_objects.ExtractionStatus(""),
			expected: false,
		},
		{
			name:     "Random string is invalid",
			status:   value_objects.ExtractionStatus("RANDOM"),
			expected: false,
		},
		{
			name:     "Lowercase pending is invalid",
			status:   value_objects.ExtractionStatus("pending"),
			expected: false,
		},
		{
			name:     "Mixed case is invalid",
			status:   value_objects.ExtractionStatus("Pending"),
			expected: false,
		},
		{
			name:     "Similar but wrong COMPLET is invalid",
			status:   value_objects.ExtractionStatus("COMPLET"),
			expected: false,
		},
		{
			name:     "Whitespace prefix is invalid",
			status:   value_objects.ExtractionStatus(" PENDING"),
			expected: false,
		},
		{
			name:     "Running is not a valid status",
			status:   value_objects.ExtractionStatus("RUNNING"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.Valid())
		})
	}
}

func TestExtractionStatus_IsTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExtractionStatus
		expected bool
	}{
		{
			name:     "Pending is not terminal",
			status:   value_objects.ExtractionStatusPending,
			expected: false,
		},
		{
			name:     "Submitted is not terminal",
			status:   value_objects.ExtractionStatusSubmitted,
			expected: false,
		},
		{
			name:     "Extracting is not terminal",
			status:   value_objects.ExtractionStatusExtracting,
			expected: false,
		},
		{
			name:     "Complete is terminal",
			status:   value_objects.ExtractionStatusComplete,
			expected: true,
		},
		{
			name:     "Failed is terminal",
			status:   value_objects.ExtractionStatusFailed,
			expected: true,
		},
		{
			name:     "Cancelled is terminal",
			status:   value_objects.ExtractionStatusCancelled,
			expected: true,
		},
		{
			name:     "Invalid status is not terminal",
			status:   value_objects.ExtractionStatus("INVALID"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.IsTerminal())
		})
	}
}

func TestExtractionStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExtractionStatus
		expected string
	}{
		{
			name:     "Pending returns PENDING",
			status:   value_objects.ExtractionStatusPending,
			expected: "PENDING",
		},
		{
			name:     "Submitted returns SUBMITTED",
			status:   value_objects.ExtractionStatusSubmitted,
			expected: "SUBMITTED",
		},
		{
			name:     "Extracting returns EXTRACTING",
			status:   value_objects.ExtractionStatusExtracting,
			expected: "EXTRACTING",
		},
		{
			name:     "Complete returns COMPLETE",
			status:   value_objects.ExtractionStatusComplete,
			expected: "COMPLETE",
		},
		{
			name:     "Failed returns FAILED",
			status:   value_objects.ExtractionStatusFailed,
			expected: "FAILED",
		},
		{
			name:     "Cancelled returns CANCELLED",
			status:   value_objects.ExtractionStatusCancelled,
			expected: "CANCELLED",
		},
		{
			name:     "Custom value returns itself",
			status:   value_objects.ExtractionStatus("CUSTOM"),
			expected: "CUSTOM",
		},
		{
			name:     "Empty string returns empty",
			status:   value_objects.ExtractionStatus(""),
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

func TestParseExtractionStatus_ValidStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected value_objects.ExtractionStatus
	}{
		{
			name:     "Parse PENDING",
			input:    "PENDING",
			expected: value_objects.ExtractionStatusPending,
		},
		{
			name:     "Parse SUBMITTED",
			input:    "SUBMITTED",
			expected: value_objects.ExtractionStatusSubmitted,
		},
		{
			name:     "Parse EXTRACTING",
			input:    "EXTRACTING",
			expected: value_objects.ExtractionStatusExtracting,
		},
		{
			name:     "Parse COMPLETE",
			input:    "COMPLETE",
			expected: value_objects.ExtractionStatusComplete,
		},
		{
			name:     "Parse FAILED",
			input:    "FAILED",
			expected: value_objects.ExtractionStatusFailed,
		},
		{
			name:     "Parse CANCELLED",
			input:    "CANCELLED",
			expected: value_objects.ExtractionStatusCancelled,
		},
		{
			name:     "Parse lowercase pending",
			input:    "pending",
			expected: value_objects.ExtractionStatusPending,
		},
		{
			name:     "Parse lowercase cancelled",
			input:    "cancelled",
			expected: value_objects.ExtractionStatusCancelled,
		},
		{
			name:     "Parse mixed case Complete",
			input:    "Complete",
			expected: value_objects.ExtractionStatusComplete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseExtractionStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseExtractionStatus_InvalidStatuses(t *testing.T) {
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
			name:  "Random string",
			input: "RANDOM",
		},
		{
			name:  "Running is not valid",
			input: "RUNNING",
		},
		{
			name:  "Similar but wrong COMPLET",
			input: "COMPLET",
		},
		{
			name:  "With leading whitespace",
			input: " PENDING",
		},
		{
			name:  "With trailing whitespace",
			input: "PENDING ",
		},
		{
			name:  "Numeric string",
			input: "123",
		},
		{
			name:  "Special characters",
			input: "PENDING!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseExtractionStatus(tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidExtractionStatus)
			assert.Equal(t, value_objects.ExtractionStatus(""), result)
		})
	}
}

func TestExtractionStatus_RoundTrip(t *testing.T) {
	t.Parallel()

	statuses := []value_objects.ExtractionStatus{
		value_objects.ExtractionStatusPending,
		value_objects.ExtractionStatusSubmitted,
		value_objects.ExtractionStatusExtracting,
		value_objects.ExtractionStatusComplete,
		value_objects.ExtractionStatusFailed,
		value_objects.ExtractionStatusCancelled,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			stringVal := status.String()
			parsed, err := value_objects.ParseExtractionStatus(stringVal)
			require.NoError(t, err)
			assert.Equal(t, status, parsed)
		})
	}
}
