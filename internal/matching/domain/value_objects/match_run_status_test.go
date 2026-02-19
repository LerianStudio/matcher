//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestMatchRunStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchRunStatus
		expected string
	}{
		{
			name:     "Processing status has correct value",
			status:   value_objects.MatchRunStatusProcessing,
			expected: "PROCESSING",
		},
		{
			name:     "Completed status has correct value",
			status:   value_objects.MatchRunStatusCompleted,
			expected: "COMPLETED",
		},
		{
			name:     "Failed status has correct value",
			status:   value_objects.MatchRunStatusFailed,
			expected: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestMatchRunStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchRunStatus
		expected bool
	}{
		{
			name:     "Processing is valid",
			status:   value_objects.MatchRunStatusProcessing,
			expected: true,
		},
		{
			name:     "Completed is valid",
			status:   value_objects.MatchRunStatusCompleted,
			expected: true,
		},
		{
			name:     "Failed is valid",
			status:   value_objects.MatchRunStatusFailed,
			expected: true,
		},
		{
			name:     "Empty string is invalid",
			status:   value_objects.MatchRunStatus(""),
			expected: false,
		},
		{
			name:     "Random string is invalid",
			status:   value_objects.MatchRunStatus("RANDOM"),
			expected: false,
		},
		{
			name:     "Lowercase processing is invalid",
			status:   value_objects.MatchRunStatus("processing"),
			expected: false,
		},
		{
			name:     "Lowercase completed is invalid",
			status:   value_objects.MatchRunStatus("completed"),
			expected: false,
		},
		{
			name:     "Lowercase failed is invalid",
			status:   value_objects.MatchRunStatus("failed"),
			expected: false,
		},
		{
			name:     "Mixed case is invalid",
			status:   value_objects.MatchRunStatus("Processing"),
			expected: false,
		},
		{
			name:     "Similar but wrong value is invalid",
			status:   value_objects.MatchRunStatus("PROCESS"),
			expected: false,
		},
		{
			name:     "Whitespace is invalid",
			status:   value_objects.MatchRunStatus(" PROCESSING"),
			expected: false,
		},
		{
			name:     "Unknown status is invalid",
			status:   value_objects.MatchRunStatus("UNKNOWN"),
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

func TestMatchRunStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.MatchRunStatus
		expected string
	}{
		{
			name:     "Processing returns PROCESSING",
			status:   value_objects.MatchRunStatusProcessing,
			expected: "PROCESSING",
		},
		{
			name:     "Completed returns COMPLETED",
			status:   value_objects.MatchRunStatusCompleted,
			expected: "COMPLETED",
		},
		{
			name:     "Failed returns FAILED",
			status:   value_objects.MatchRunStatusFailed,
			expected: "FAILED",
		},
		{
			name:     "Custom value returns itself",
			status:   value_objects.MatchRunStatus("CUSTOM"),
			expected: "CUSTOM",
		},
		{
			name:     "Empty string returns empty",
			status:   value_objects.MatchRunStatus(""),
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

func TestParseMatchRunStatus_ValidStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected value_objects.MatchRunStatus
	}{
		{
			name:     "Parse PROCESSING",
			input:    "PROCESSING",
			expected: value_objects.MatchRunStatusProcessing,
		},
		{
			name:     "Parse COMPLETED",
			input:    "COMPLETED",
			expected: value_objects.MatchRunStatusCompleted,
		},
		{
			name:     "Parse FAILED",
			input:    "FAILED",
			expected: value_objects.MatchRunStatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchRunStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseMatchRunStatus_InvalidStatuses(t *testing.T) {
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
			name:  "Lowercase processing",
			input: "processing",
		},
		{
			name:  "Lowercase completed",
			input: "completed",
		},
		{
			name:  "Lowercase failed",
			input: "failed",
		},
		{
			name:  "Mixed case Processing",
			input: "Processing",
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
			name:  "Similar but wrong PROCESS",
			input: "PROCESS",
		},
		{
			name:  "With leading whitespace",
			input: " PROCESSING",
		},
		{
			name:  "With trailing whitespace",
			input: "PROCESSING ",
		},
		{
			name:  "With surrounding whitespace",
			input: " PROCESSING ",
		},
		{
			name:  "Numeric string",
			input: "123",
		},
		{
			name:  "Special characters",
			input: "PROCESSING!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseMatchRunStatus(tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidMatchRunStatus)
			assert.Equal(t, value_objects.MatchRunStatus(""), result)
		})
	}
}

func TestMatchRunStatus_RoundTrip(t *testing.T) {
	t.Parallel()

	statuses := []value_objects.MatchRunStatus{
		value_objects.MatchRunStatusProcessing,
		value_objects.MatchRunStatusCompleted,
		value_objects.MatchRunStatusFailed,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			stringVal := status.String()
			parsed, err := value_objects.ParseMatchRunStatus(stringVal)
			require.NoError(t, err)
			assert.Equal(t, status, parsed)
		})
	}
}
