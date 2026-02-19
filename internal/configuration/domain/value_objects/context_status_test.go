//go:build unit

package value_objects

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextStatus_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status ContextStatus
		valid  bool
	}{
		{"draft", ContextStatusDraft, true},
		{"active", ContextStatusActive, true},
		{"paused", ContextStatusPaused, true},
		{"archived", ContextStatusArchived, true},
		{"invalid", ContextStatus("STOPPED"), false},
		{"empty", ContextStatus(""), false},
		{"lowercase_active", ContextStatus("active"), false},
		{"lowercase_paused", ContextStatus("paused"), false},
		{"mixed_case", ContextStatus("Active"), false},
		{"with_spaces", ContextStatus(" ACTIVE "), false},
		{"numeric", ContextStatus("123"), false},
		{"special_chars", ContextStatus("ACTIVE!"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.status.Valid())
		})
	}
}

func TestContextStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status ContextStatus
		valid  bool
	}{
		{"draft_is_valid", ContextStatusDraft, true},
		{"active_is_valid", ContextStatusActive, true},
		{"paused_is_valid", ContextStatusPaused, true},
		{"archived_is_valid", ContextStatusArchived, true},
		{"invalid_is_not_valid", ContextStatus("UNKNOWN"), false},
		{"empty_is_not_valid", ContextStatus(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.status.IsValid())
			assert.Equal(t, tt.status.Valid(), tt.status.IsValid())
		})
	}
}

func TestContextStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		status   ContextStatus
		expected string
	}{
		{"active_string", ContextStatusActive, "ACTIVE"},
		{"paused_string", ContextStatusPaused, "PAUSED"},
		{"empty_string", ContextStatus(""), ""},
		{"custom_value", ContextStatus("CUSTOM"), "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.status.String())
		})
	}
}

func TestParseContextStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ContextStatus
		wantErr bool
	}{
		{"valid_draft", "DRAFT", ContextStatusDraft, false},
		{"valid_active", "ACTIVE", ContextStatusActive, false},
		{"valid_paused", "PAUSED", ContextStatusPaused, false},
		{"valid_archived", "ARCHIVED", ContextStatusArchived, false},
		{"lowercase_draft", "draft", ContextStatusDraft, false},
		{"lowercase_active", "active", ContextStatusActive, false},
		{"lowercase_paused", "paused", ContextStatusPaused, false},
		{"lowercase_archived", "archived", ContextStatusArchived, false},
		{"mixed_case_active", "Active", ContextStatusActive, false},
		{"mixed_case_paused", "Paused", ContextStatusPaused, false},
		{"mixed_case_draft", "Draft", ContextStatusDraft, false},
		{"mixed_case_archived", "Archived", ContextStatusArchived, false},
		{"invalid_status", "UNKNOWN", "", true},
		{"empty_string", "", "", true},
		{"numeric", "123", "", true},
		{"special_chars", "ACTIVE!", "", true},
		{"with_spaces", " ACTIVE ", "", true},
		{"stopped", "STOPPED", "", true},
		{"pending", "PENDING", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseContextStatus(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, errors.Is(err, ErrInvalidContextStatus))
				assert.Contains(t, err.Error(), tt.input)
				assert.Empty(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.True(t, got.Valid())
		})
	}
}

func TestContextStatus_Constants(t *testing.T) {
	t.Parallel()

	t.Run("draft_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextStatus("DRAFT"), ContextStatusDraft)
	})

	t.Run("active_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextStatus("ACTIVE"), ContextStatusActive)
	})

	t.Run("paused_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextStatus("PAUSED"), ContextStatusPaused)
	})

	t.Run("archived_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextStatus("ARCHIVED"), ContextStatusArchived)
	})
}

func TestErrInvalidContextStatus(t *testing.T) {
	t.Parallel()

	t.Run("error_is_not_nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrInvalidContextStatus)
	})

	t.Run("error_message", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "invalid context status", ErrInvalidContextStatus.Error())
	})

	t.Run("wrapped_error_can_be_unwrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ParseContextStatus("INVALID")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidContextStatus))
	})
}
