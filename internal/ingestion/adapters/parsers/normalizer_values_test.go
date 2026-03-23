//go:build unit

package parsers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStringValue(t *testing.T) {
	t.Parallel()

	t.Run("string type trimmed", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"name": "  Alice  "}
		result, ok := getStringValue(row, "name")

		require.True(t, ok)
		assert.Equal(t, "Alice", result)
	})

	t.Run("json.Number converted", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"amount": json.Number("42.50")}
		result, ok := getStringValue(row, "amount")

		require.True(t, ok)
		assert.Equal(t, "42.50", result)
	})

	t.Run("missing field returns false", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"name": "Alice"}
		_, ok := getStringValue(row, "missing")

		require.False(t, ok)
	})

	t.Run("nil value returns false", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"name": nil}
		_, ok := getStringValue(row, "name")

		require.False(t, ok)
	})
}

func TestGetStringValue_SanitizesFormulas(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		row      map[string]any
		field    string
		expected string
		ok       bool
	}{
		{
			name:     "equals formula sanitized",
			row:      map[string]any{"desc": "=HYPERLINK(\"http://evil.com\")"},
			field:    "desc",
			expected: "'=HYPERLINK(\"http://evil.com\")",
			ok:       true,
		},
		{
			name:     "at formula sanitized",
			row:      map[string]any{"note": "@SUM(A1)"},
			field:    "note",
			expected: "'@SUM(A1)",
			ok:       true,
		},
		{
			name:     "plus formula sanitized",
			row:      map[string]any{"val": "+cmd|calc"},
			field:    "val",
			expected: "'+cmd|calc",
			ok:       true,
		},
		{
			name:     "negative number preserved",
			row:      map[string]any{"amount": "-100.50"},
			field:    "amount",
			expected: "-100.50",
			ok:       true,
		},
		{
			name:     "positive number preserved",
			row:      map[string]any{"amount": "+200.00"},
			field:    "amount",
			expected: "+200.00",
			ok:       true,
		},
		{
			name:     "normal text preserved",
			row:      map[string]any{"name": "Alice"},
			field:    "name",
			expected: "Alice",
			ok:       true,
		},
		{
			name:     "missing field returns false",
			row:      map[string]any{"name": "Alice"},
			field:    "missing",
			expected: "",
			ok:       false,
		},
		{
			name:     "nil value returns false",
			row:      map[string]any{"name": nil},
			field:    "name",
			expected: "",
			ok:       false,
		},
		{
			name:     "tab prefix stripped by TrimSpace",
			row:      map[string]any{"val": "\tmalicious"},
			field:    "val",
			expected: "malicious",
			ok:       true,
		},
		{
			name:     "carriage return prefix stripped by TrimSpace",
			row:      map[string]any{"val": "\revil"},
			field:    "val",
			expected: "evil",
			ok:       true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, ok := getStringValue(testCase.row, testCase.field)
			assert.Equal(t, testCase.ok, ok)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestSanitizeFormulaInjection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"equals prefix sanitized", "=HYPERLINK(\"http://evil.com\",\"Click\")", "'=HYPERLINK(\"http://evil.com\",\"Click\")"},
		{"plus formula sanitized", "+cmd|'/c calc'!A0", "'+cmd|'/c calc'!A0"},
		{"minus formula sanitized", "-cmd|'/c calc'!A0", "'-cmd|'/c calc'!A0"},
		{"at symbol sanitized", "@SUM(1+1)", "'@SUM(1+1)"},
		{"tab prefix sanitized", "\tdata", "'\tdata"},
		{"carriage return sanitized", "\rdata", "'\rdata"},
		{"positive numeric preserved", "+100.50", "+100.50"},
		{"negative numeric preserved", "-200.00", "-200.00"},
		{"positive integer preserved", "+42", "+42"},
		{"negative integer preserved", "-42", "-42"},
		{"scientific notation preserved", "+1.5e3", "+1.5e3"},
		{"normal text unchanged", "Normal text", "Normal text"},
		{"empty string unchanged", "", ""},
		{"plain number unchanged", "12345", "12345"},
		{"embedded equals unchanged", "a=b", "a=b"},
		{"embedded plus unchanged", "a+b", "a+b"},
		{"single plus sanitized", "+", "'+"},
		{"single minus sanitized", "-", "'-"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := sanitizeFormulaInjection(testCase.input)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestBuildMappedFieldSet(t *testing.T) {
	t.Parallel()

	t.Run("includes all mapped raw field names", func(t *testing.T) {
		t.Parallel()

		mapping := map[string]string{
			"external_id": "txn_id",
			"amount":      "txn_amount",
			"currency":    "ccy",
			"date":        "txn_date",
			"description": "memo",
		}

		result := buildMappedFieldSet(mapping)

		require.True(t, result["txn_id"])
		require.True(t, result["txn_amount"])
		require.True(t, result["ccy"])
		require.True(t, result["txn_date"])
		require.True(t, result["memo"])
		require.Len(t, result, 5)
	})

	t.Run("skips empty raw field names", func(t *testing.T) {
		t.Parallel()

		mapping := map[string]string{
			"external_id": "txn_id",
			"amount":      "",
		}

		result := buildMappedFieldSet(mapping)

		require.True(t, result["txn_id"])
		require.False(t, result[""])
		require.Len(t, result, 1)
	})

	t.Run("empty mapping returns empty set", func(t *testing.T) {
		t.Parallel()

		result := buildMappedFieldSet(map[string]string{})
		require.Empty(t, result)
	})
}

func TestBuildMetadata(t *testing.T) {
	t.Parallel()

	t.Run("excludes mapped fields from metadata", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":           "TXN-001",
			"amount":       "100.50",
			"currency":     "USD",
			"date":         "2024-01-15",
			"description":  "Wire transfer",
			"extra_field":  "should be kept",
			"another_note": "also kept",
		}
		mappedFields := map[string]bool{
			"id":          true,
			"amount":      true,
			"currency":    true,
			"date":        true,
			"description": true,
		}

		metadata := buildMetadata(row, mappedFields)

		require.Len(t, metadata, 2)
		require.Equal(t, "should be kept", metadata["extra_field"])
		require.Equal(t, "also kept", metadata["another_note"])
		require.NotContains(t, metadata, "id")
		require.NotContains(t, metadata, "amount")
		require.NotContains(t, metadata, "currency")
		require.NotContains(t, metadata, "date")
		require.NotContains(t, metadata, "description")
	})

	t.Run("nil row returns empty map", func(t *testing.T) {
		t.Parallel()

		metadata := buildMetadata(nil, map[string]bool{"id": true})
		require.Empty(t, metadata)
		require.NotNil(t, metadata)
	})

	t.Run("nil mapped fields keeps all row data", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":     "TXN-001",
			"amount": "50.00",
			"extra":  "value",
		}

		metadata := buildMetadata(row, nil)

		require.Len(t, metadata, 3)
		require.Equal(t, "TXN-001", metadata["id"])
		require.Equal(t, "50.00", metadata["amount"])
		require.Equal(t, "value", metadata["extra"])
	})

	t.Run("empty mapped fields keeps all row data", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":     "TXN-001",
			"amount": "50.00",
		}

		metadata := buildMetadata(row, map[string]bool{})

		require.Len(t, metadata, 2)
		require.Equal(t, "TXN-001", metadata["id"])
		require.Equal(t, "50.00", metadata["amount"])
	})

	t.Run("all fields mapped results in empty metadata", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"id":       "TXN-001",
			"amount":   "100.00",
			"currency": "EUR",
		}
		mappedFields := map[string]bool{
			"id":       true,
			"amount":   true,
			"currency": true,
		}

		metadata := buildMetadata(row, mappedFields)

		require.Empty(t, metadata)
		require.NotNil(t, metadata)
	})
}

func TestBuildMetadata_SanitizesStringValues(t *testing.T) {
	t.Parallel()

	t.Run("formula strings in metadata are sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"safe_field":    "normal text",
			"formula_field": "=HYPERLINK(\"http://evil.com\")",
			"at_field":      "@SUM(1+1)",
			"plus_field":    "+cmd|calc",
			"minus_field":   "-cmd|calc",
			"tab_field":     "\tdata",
			"cr_field":      "\rdata",
			"numeric_field": "-100.50",
			"int_field":     42,
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		assert.Equal(t, "normal text", metadata["safe_field"])
		assert.Equal(t, "'=HYPERLINK(\"http://evil.com\")", metadata["formula_field"])
		assert.Equal(t, "'@SUM(1+1)", metadata["at_field"])
		assert.Equal(t, "'+cmd|calc", metadata["plus_field"])
		assert.Equal(t, "'-cmd|calc", metadata["minus_field"])
		// Tab and CR are stripped by TrimSpace, leaving just "data"
		assert.Equal(t, "data", metadata["tab_field"])
		assert.Equal(t, "data", metadata["cr_field"])
		// Numeric strings that are valid numbers should NOT be sanitized
		assert.Equal(t, "-100.50", metadata["numeric_field"])
		// Non-string values pass through unchanged
		assert.Equal(t, 42, metadata["int_field"])
	})

	t.Run("non-string values not affected by sanitization", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"bool_val":  true,
			"int_val":   123,
			"float_val": 3.14,
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		assert.Equal(t, true, metadata["bool_val"])
		assert.Equal(t, 123, metadata["int_val"])
		assert.Equal(t, 3.14, metadata["float_val"])
	})

	t.Run("leading whitespace before formula chars is trimmed then sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"formula_with_space": " =HYPERLINK(\"http://evil.com\")",
			"at_with_space":      " @evil",
			"plus_with_space":    " +cmd",
			"minus_with_space":   " -cmd",
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		// Leading space is trimmed, then the formula char triggers sanitization
		assert.Equal(t, "'=HYPERLINK(\"http://evil.com\")", metadata["formula_with_space"])
		assert.Equal(t, "'@evil", metadata["at_with_space"])
		assert.Equal(t, "'+cmd", metadata["plus_with_space"])
		assert.Equal(t, "'-cmd", metadata["minus_with_space"])
	})

	t.Run("json.Number values are sanitized", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{
			"json_num_safe":    json.Number("12345"),
			"json_num_formula": json.Number("=MALICIOUS"),
		}
		mappedFields := map[string]bool{}

		metadata := buildMetadata(row, mappedFields)

		// Safe numeric json.Number passes through unchanged
		assert.Equal(t, "12345", metadata["json_num_safe"])
		// Formula-prefixed json.Number gets sanitized
		assert.Equal(t, "'=MALICIOUS", metadata["json_num_formula"])
	})
}

func TestRequiredString(t *testing.T) {
	t.Parallel()

	t.Run("missing mapping returns error", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"id": "123"}
		mapping := map[string]string{}

		_, parseErr := requiredString(row, mapping, "external_id", 1)

		require.NotNil(t, parseErr)
		assert.Equal(t, "external_id", parseErr.Field)
		assert.Equal(t, "missing field mapping", parseErr.Message)
	})

	t.Run("empty value returns error", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"id": "  "}
		mapping := map[string]string{"external_id": "id"}

		_, parseErr := requiredString(row, mapping, "external_id", 1)

		require.NotNil(t, parseErr)
		assert.Equal(t, "missing required field", parseErr.Message)
	})
}

func TestParseCurrency(t *testing.T) {
	t.Parallel()

	t.Run("valid code uppercased", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"ccy": "usd"}
		mapping := map[string]string{"currency": "ccy"}

		currency, parseErr := parseCurrency(row, mapping, 1)

		require.Nil(t, parseErr)
		assert.Equal(t, "USD", currency)
	})

	t.Run("invalid code rejected", func(t *testing.T) {
		t.Parallel()

		row := map[string]any{"ccy": "ZZZZ"}
		mapping := map[string]string{"currency": "ccy"}

		_, parseErr := parseCurrency(row, mapping, 1)

		require.NotNil(t, parseErr)
		assert.Equal(t, "currency", parseErr.Field)
	})
}
