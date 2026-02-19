//go:build unit

package value_objects

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextType_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		ct    ContextType
		valid bool
	}{
		{"1:1 valid", ContextTypeOneToOne, true},
		{"1:N valid", ContextTypeOneToMany, true},
		{"N:M valid", ContextTypeManyToMany, true},
		{"invalid", ContextType("INVALID"), false},
		{"empty", ContextType(""), false},
		{"wrong_format_one_to_one", ContextType("ONE_TO_ONE"), false},
		{"wrong_format_one_to_many", ContextType("ONE_TO_MANY"), false},
		{"wrong_format_many_to_many", ContextType("MANY_TO_MANY"), false},
		{"numeric_only", ContextType("11"), false},
		{"with_spaces", ContextType(" 1:1 "), false},
		{"lowercase_n", ContextType("n:m"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.ct.Valid())
		})
	}
}

func TestContextType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		ct    ContextType
		valid bool
	}{
		{"one_to_one_is_valid", ContextTypeOneToOne, true},
		{"one_to_many_is_valid", ContextTypeOneToMany, true},
		{"many_to_many_is_valid", ContextTypeManyToMany, true},
		{"invalid_is_not_valid", ContextType("UNKNOWN"), false},
		{"empty_is_not_valid", ContextType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.ct.IsValid())
			assert.Equal(t, tt.ct.Valid(), tt.ct.IsValid())
		})
	}
}

func TestContextType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ct       ContextType
		expected string
	}{
		{"one_to_one_string", ContextTypeOneToOne, "1:1"},
		{"one_to_many_string", ContextTypeOneToMany, "1:N"},
		{"many_to_many_string", ContextTypeManyToMany, "N:M"},
		{"empty_string", ContextType(""), ""},
		{"custom_value", ContextType("CUSTOM"), "CUSTOM"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.ct.String())
		})
	}
}

func TestParseContextType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    ContextType
		wantErr bool
	}{
		{"valid_one_to_one", "1:1", ContextTypeOneToOne, false},
		{"valid_one_to_many", "1:N", ContextTypeOneToMany, false},
		{"valid_many_to_many", "N:M", ContextTypeManyToMany, false},
		{"with_leading_spaces", " 1:1", ContextTypeOneToOne, false},
		{"with_trailing_spaces", "1:1 ", ContextTypeOneToOne, false},
		{"with_surrounding_spaces", " 1:1 ", ContextTypeOneToOne, false},
		{"invalid_type", "invalid", "", true},
		{"empty_string", "", "", true},
		{"wrong_format", "ONE_TO_ONE", "", true},
		{"lowercase_n", "n:m", "", true},
		{"numeric_only", "11", "", true},
		{"partial_match", "1:", "", true},
		{"reversed", "M:N", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ct, err := ParseContextType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid context type")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, ct)
			assert.True(t, ct.Valid())
		})
	}
}

func TestContextType_Constants(t *testing.T) {
	t.Parallel()

	t.Run("one_to_one_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextType("1:1"), ContextTypeOneToOne)
	})

	t.Run("one_to_many_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextType("1:N"), ContextTypeOneToMany)
	})

	t.Run("many_to_many_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, ContextType("N:M"), ContextTypeManyToMany)
	})
}

func TestErrInvalidContextType(t *testing.T) {
	t.Parallel()

	t.Run("error_is_not_nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrInvalidContextType)
	})

	t.Run("error_message", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "invalid context type", ErrInvalidContextType.Error())
	})

	t.Run("wrapped_error_can_be_unwrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ParseContextType("INVALID")
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrInvalidContextType))
	})
}
