//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/discovery/domain/value_objects"
)

func TestConnectionStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ConnectionStatus
		expected string
	}{
		{
			name:     "Available status has correct value",
			status:   value_objects.ConnectionStatusAvailable,
			expected: "AVAILABLE",
		},
		{
			name:     "Unreachable status has correct value",
			status:   value_objects.ConnectionStatusUnreachable,
			expected: "UNREACHABLE",
		},
		{
			name:     "Unknown status has correct value",
			status:   value_objects.ConnectionStatusUnknown,
			expected: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestConnectionStatus_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ConnectionStatus
		expected bool
	}{
		{
			name:     "Available is valid",
			status:   value_objects.ConnectionStatusAvailable,
			expected: true,
		},
		{
			name:     "Unreachable is valid",
			status:   value_objects.ConnectionStatusUnreachable,
			expected: true,
		},
		{
			name:     "Unknown is valid",
			status:   value_objects.ConnectionStatusUnknown,
			expected: true,
		},
		{
			name:     "Empty string is invalid",
			status:   value_objects.ConnectionStatus(""),
			expected: false,
		},
		{
			name:     "Random string is invalid",
			status:   value_objects.ConnectionStatus("RANDOM"),
			expected: false,
		},
		{
			name:     "Lowercase available is invalid",
			status:   value_objects.ConnectionStatus("available"),
			expected: false,
		},
		{
			name:     "Mixed case is invalid",
			status:   value_objects.ConnectionStatus("Available"),
			expected: false,
		},
		{
			name:     "Similar but wrong value is invalid",
			status:   value_objects.ConnectionStatus("AVAIL"),
			expected: false,
		},
		{
			name:     "Whitespace prefix is invalid",
			status:   value_objects.ConnectionStatus(" AVAILABLE"),
			expected: false,
		},
		{
			name:     "Connected is not a valid status",
			status:   value_objects.ConnectionStatus("CONNECTED"),
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

func TestConnectionStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ConnectionStatus
		expected string
	}{
		{
			name:     "Available returns AVAILABLE",
			status:   value_objects.ConnectionStatusAvailable,
			expected: "AVAILABLE",
		},
		{
			name:     "Unreachable returns UNREACHABLE",
			status:   value_objects.ConnectionStatusUnreachable,
			expected: "UNREACHABLE",
		},
		{
			name:     "Unknown returns UNKNOWN",
			status:   value_objects.ConnectionStatusUnknown,
			expected: "UNKNOWN",
		},
		{
			name:     "Custom value returns itself",
			status:   value_objects.ConnectionStatus("CUSTOM"),
			expected: "CUSTOM",
		},
		{
			name:     "Empty string returns empty",
			status:   value_objects.ConnectionStatus(""),
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

func TestParseConnectionStatus_ValidStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected value_objects.ConnectionStatus
	}{
		{
			name:     "Parse AVAILABLE",
			input:    "AVAILABLE",
			expected: value_objects.ConnectionStatusAvailable,
		},
		{
			name:     "Parse UNREACHABLE",
			input:    "UNREACHABLE",
			expected: value_objects.ConnectionStatusUnreachable,
		},
		{
			name:     "Parse UNKNOWN",
			input:    "UNKNOWN",
			expected: value_objects.ConnectionStatusUnknown,
		},
		{
			name:     "Parse lowercase available",
			input:    "available",
			expected: value_objects.ConnectionStatusAvailable,
		},
		{
			name:     "Parse mixed case Unknown",
			input:    "Unknown",
			expected: value_objects.ConnectionStatusUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseConnectionStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseConnectionStatus_InvalidStatuses(t *testing.T) {
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
			name:  "Connected is not valid",
			input: "CONNECTED",
		},
		{
			name:  "Similar but wrong AVAIL",
			input: "AVAIL",
		},
		{
			name:  "With leading whitespace",
			input: " AVAILABLE",
		},
		{
			name:  "With trailing whitespace",
			input: "AVAILABLE ",
		},
		{
			name:  "Numeric string",
			input: "123",
		},
		{
			name:  "Special characters",
			input: "AVAILABLE!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := value_objects.ParseConnectionStatus(tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidConnectionStatus)
			assert.Equal(t, value_objects.ConnectionStatus(""), result)
		})
	}
}

func TestConnectionStatus_RoundTrip(t *testing.T) {
	t.Parallel()

	statuses := []value_objects.ConnectionStatus{
		value_objects.ConnectionStatusAvailable,
		value_objects.ConnectionStatusUnreachable,
		value_objects.ConnectionStatusUnknown,
	}

	for _, status := range statuses {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()

			stringVal := status.String()
			parsed, err := value_objects.ParseConnectionStatus(stringVal)
			require.NoError(t, err)
			assert.Equal(t, status, parsed)
		})
	}
}
