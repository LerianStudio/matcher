//go:build unit

package sanitize

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		// Cases previously passing through weak isNumericAfterSign in reporting.
		{"minus 1 abc is sanitized", "-1abc", "'-1abc"},
		{"plus 1 abc is sanitized", "+1abc", "'+1abc"},
		{"negative number with trailing text sanitized", "-100.50abc", "'-100.50abc"},
		// Numeric values from reporting context.
		{"negative integer from strconv", "-1", "-1"},
		{"negative decimal amount", "-100", "-100"},
		{"positive with decimal", "+123.45", "+123.45"},
		{"negative with decimal", "-123.45", "-123.45"},
		{"plus CMD is sanitized", "+CMD", "'+CMD"},
		{"minus CMD is sanitized", "-CMD", "'-CMD"},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := SanitizeFormulaInjection(testCase.input)
			assert.Equal(t, testCase.expected, result)
		})
	}
}

func TestIsNumericString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected bool
	}{
		{"", false},
		{"123", true},
		{"-123", true},
		{"+123", true},
		{"12.34", true},
		{"-12.34", true},
		{"+12.34", true},
		{"0", true},
		{"0.0", true},
		{"-", false},
		{"+", false},
		{".", false},
		{"12.", true},
		{".12", true},
		{"12.34.56", false},
		{"abc", false},
		{"-abc", false},
		{"+abc", false},
		{"12abc", false},
		{"-12abc", false},
		{"1+2", false},
		{"1-2", false},
		// Scientific notation.
		{"1e10", true},
		{"1E10", true},
		{"-1e10", true},
		{"+1e10", true},
		{"1.5e3", true},
		{"-1.5E3", true},
		{"+1.5e3", true},
		{"1e+10", true},
		{"1e-10", true},
		{"1.5E+3", true},
		{"1.5E-3", true},
		{".5e2", true},
		{"1.e2", true},
		{"1e0", true},
		// Malformed scientific notation.
		{"e10", false},
		{"E10", false},
		{".e10", false},
		{"1e", false},
		{"1E", false},
		{"1e+", false},
		{"1e-", false},
		{"-e5", false},
		{"+e5", false},
		{"1e2e3", false},
		{"1e2.3", false},
		{"1e2+3", false},
		// Cases that would pass isNumericAfterSign but fail IsNumericString.
		{"-1abc", false},
		{"+1abc", false},
		{"-100.50abc", false},
	}

	for _, testCase := range tests {
		t.Run(testCase.input, func(t *testing.T) {
			t.Parallel()

			result := IsNumericString(testCase.input)
			assert.Equal(t, testCase.expected, result, "for input: %q", testCase.input)
		})
	}
}
