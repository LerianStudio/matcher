//go:build unit

package value_objects_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestExceptionStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExceptionStatus
		expected string
	}{
		{name: "Open", status: value_objects.ExceptionStatusOpen, expected: "OPEN"},
		{name: "Assigned", status: value_objects.ExceptionStatusAssigned, expected: "ASSIGNED"},
		{name: "PendingResolution", status: value_objects.ExceptionStatusPendingResolution, expected: "PENDING_RESOLUTION"},
		{name: "Resolved", status: value_objects.ExceptionStatusResolved, expected: "RESOLVED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

func TestExceptionStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExceptionStatus
		expected bool
	}{
		{name: "Open valid", status: value_objects.ExceptionStatusOpen, expected: true},
		{name: "Assigned valid", status: value_objects.ExceptionStatusAssigned, expected: true},
		{name: "PendingResolution valid", status: value_objects.ExceptionStatusPendingResolution, expected: true},
		{name: "Resolved valid", status: value_objects.ExceptionStatusResolved, expected: true},
		{name: "Empty invalid", status: value_objects.ExceptionStatus(""), expected: false},
		{name: "Lowercase invalid", status: value_objects.ExceptionStatus("open"), expected: false},
		{
			name:     "Unknown invalid",
			status:   value_objects.ExceptionStatus("UNKNOWN"),
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

func TestExceptionStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   value_objects.ExceptionStatus
		expected string
	}{
		{name: "Open", status: value_objects.ExceptionStatusOpen, expected: "OPEN"},
		{name: "Assigned", status: value_objects.ExceptionStatusAssigned, expected: "ASSIGNED"},
		{name: "PendingResolution", status: value_objects.ExceptionStatusPendingResolution, expected: "PENDING_RESOLUTION"},
		{name: "Resolved", status: value_objects.ExceptionStatusResolved, expected: "RESOLVED"},
		{name: "Custom", status: value_objects.ExceptionStatus("CUSTOM"), expected: "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestParseExceptionStatus(t *testing.T) {
	t.Parallel()

	valid := []string{"OPEN", "ASSIGNED", "PENDING_RESOLUTION", "RESOLVED"}
	for _, status := range valid {
		t.Run("Valid "+status, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionStatus(status)
			require.NoError(t, err)
			assert.Equal(t, value_objects.ExceptionStatus(status), parsed)
		})
	}

	// Case-insensitive parsing: lowercase and mixed-case inputs should be accepted.
	caseInsensitive := []struct {
		input    string
		expected value_objects.ExceptionStatus
	}{
		{"open", value_objects.ExceptionStatusOpen},
		{"assigned", value_objects.ExceptionStatusAssigned},
		{"pending_resolution", value_objects.ExceptionStatusPendingResolution},
		{"resolved", value_objects.ExceptionStatusResolved},
		{"Open", value_objects.ExceptionStatusOpen},
		{"Assigned", value_objects.ExceptionStatusAssigned},
		{"Pending_Resolution", value_objects.ExceptionStatusPendingResolution},
		{"Resolved", value_objects.ExceptionStatusResolved},
	}
	for _, tc := range caseInsensitive {
		t.Run("CaseInsensitive_"+tc.input, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionStatus(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, parsed)
		})
	}

	// Whitespace trimming: leading/trailing whitespace should be stripped.
	trimmed := []struct {
		input    string
		expected value_objects.ExceptionStatus
	}{
		{" OPEN ", value_objects.ExceptionStatusOpen},
		{"  resolved  ", value_objects.ExceptionStatusResolved},
		{"\tASSIGNED\t", value_objects.ExceptionStatusAssigned},
	}
	for _, tc := range trimmed {
		t.Run("Trimmed_"+tc.input, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionStatus(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, parsed)
		})
	}

	invalid := []string{"", "UNKNOWN", "INVALID"}
	for _, status := range invalid {
		t.Run("Invalid_"+status, func(t *testing.T) {
			t.Parallel()

			parsed, err := value_objects.ParseExceptionStatus(status)
			require.Error(t, err)
			require.ErrorIs(t, err, value_objects.ErrInvalidExceptionStatus)
			assert.Equal(t, value_objects.ExceptionStatus(""), parsed)
		})
	}
}
