//go:build unit

package value_objects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSourceType_Valid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source SourceType
		valid  bool
	}{
		{"ledger", SourceTypeLedger, true},
		{"bank", SourceTypeBank, true},
		{"gateway", SourceTypeGateway, true},
		{"custom", SourceTypeCustom, true},
		{"invalid", SourceType("OTHER"), false},
		{"empty", SourceType(""), false},
		{"lowercase_ledger", SourceType("ledger"), false},
		{"lowercase_bank", SourceType("bank"), false},
		{"mixed_case", SourceType("Ledger"), false},
		{"with_spaces", SourceType(" LEDGER "), false},
		{"numeric", SourceType("123"), false},
		{"special_chars", SourceType("LEDGER!"), false},
		{"partial_match", SourceType("LED"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.source.Valid())
		})
	}
}

func TestSourceType_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		source SourceType
		valid  bool
	}{
		{"ledger_is_valid", SourceTypeLedger, true},
		{"bank_is_valid", SourceTypeBank, true},
		{"gateway_is_valid", SourceTypeGateway, true},
		{"custom_is_valid", SourceTypeCustom, true},
		{"invalid_is_not_valid", SourceType("UNKNOWN"), false},
		{"empty_is_not_valid", SourceType(""), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.valid, tt.source.IsValid())
			assert.Equal(t, tt.source.Valid(), tt.source.IsValid())
		})
	}
}

func TestSourceType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		source   SourceType
		expected string
	}{
		{"ledger_string", SourceTypeLedger, "LEDGER"},
		{"bank_string", SourceTypeBank, "BANK"},
		{"gateway_string", SourceTypeGateway, "GATEWAY"},
		{"custom_string", SourceTypeCustom, "CUSTOM"},
		{"empty_string", SourceType(""), ""},
		{"arbitrary_value", SourceType("ARBITRARY"), "ARBITRARY"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.expected, tt.source.String())
		})
	}
}

func TestParseSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    SourceType
		wantErr bool
	}{
		{"valid_ledger", "LEDGER", SourceTypeLedger, false},
		{"valid_bank", "BANK", SourceTypeBank, false},
		{"valid_gateway", "GATEWAY", SourceTypeGateway, false},
		{"valid_custom", "CUSTOM", SourceTypeCustom, false},
		{"lowercase_ledger", "ledger", SourceTypeLedger, false},
		{"lowercase_bank", "bank", SourceTypeBank, false},
		{"mixed_case", "Ledger", SourceTypeLedger, false},
		{"lowercase_gateway", "gateway", SourceTypeGateway, false},
		{"lowercase_custom", "custom", SourceTypeCustom, false},
		{"mixed_case_gateway", "GaTeWaY", SourceTypeGateway, false},
		{"invalid_type", "UNKNOWN", "", true},
		{"empty_string", "", "", true},
		{"numeric", "123", "", true},
		{"special_chars", "LEDGER!", "", true},
		{"with_spaces", " LEDGER ", "", true},
		{"partial_match", "LED", "", true},
		{"other", "OTHER", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ParseSourceType(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, ErrInvalidSourceType)
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

func TestSourceType_Constants(t *testing.T) {
	t.Parallel()

	t.Run("ledger_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, SourceType("LEDGER"), SourceTypeLedger)
	})

	t.Run("bank_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, SourceType("BANK"), SourceTypeBank)
	})

	t.Run("gateway_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, SourceType("GATEWAY"), SourceTypeGateway)
	})

	t.Run("custom_constant_value", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, SourceType("CUSTOM"), SourceTypeCustom)
	})
}

func TestErrInvalidSourceType(t *testing.T) {
	t.Parallel()

	t.Run("error_is_not_nil", func(t *testing.T) {
		t.Parallel()

		assert.NotNil(t, ErrInvalidSourceType)
	})

	t.Run("error_message", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, "invalid source type", ErrInvalidSourceType.Error())
	})

	t.Run("wrapped_error_can_be_unwrapped", func(t *testing.T) {
		t.Parallel()

		_, err := ParseSourceType("INVALID")
		require.Error(t, err)
		require.ErrorIs(t, err, ErrInvalidSourceType)
	})
}
