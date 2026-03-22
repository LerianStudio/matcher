// Package sanitize provides shared sanitization functions for preventing
// CSV/spreadsheet formula injection across ingestion and reporting contexts.
package sanitize

// SanitizeFormulaInjection prevents CSV/spreadsheet formula injection by
// prefixing a single quote to values that start with characters interpreted
// as formulas by spreadsheet applications (=, @, tab, carriage return).
// Values starting with + or - are only sanitized if they are NOT valid numeric
// strings, preserving legitimate signed numbers like "+100.50" or "-200.00".
func SanitizeFormulaInjection(value string) string {
	if len(value) == 0 {
		return value
	}

	switch value[0] {
	case '=', '@', '\t', '\r':
		return "'" + value
	case '+', '-':
		if IsNumericString(value) {
			return value
		}

		return "'" + value
	default:
		return value
	}
}

// IsNumericString checks if a string represents a valid numeric value.
// Accepts formats like: 123, -123, +123, 12.34, .12, 12., 1e10, 1.5E-3, etc.
func IsNumericString(value string) bool {
	if len(value) == 0 {
		return false
	}

	start := skipSign(value)
	if start >= len(value) {
		return false
	}

	return scanMantissa(value, start)
}

// skipSign returns the index after an optional leading '+' or '-'.
func skipSign(value string) int {
	if len(value) == 0 {
		return 0
	}

	if value[0] == '+' || value[0] == '-' {
		return 1
	}

	return 0
}

// scanMantissa scans digit and dot characters starting at pos,
// delegating to consumeExponent when 'e'/'E' is encountered.
func scanMantissa(value string, pos int) bool {
	hasDigit := false
	hasDot := false

	for i := pos; i < len(value); i++ {
		char := value[i]

		switch {
		case char >= '0' && char <= '9':
			hasDigit = true
		case char == '.' && !hasDot:
			hasDot = true
		case (char == 'e' || char == 'E') && hasDigit:
			return consumeExponent(value, i+1)
		default:
			return false
		}
	}

	return hasDigit
}

// consumeExponent validates the exponent part of a numeric string starting at
// position pos (immediately after the 'e'/'E'). It expects an optional sign
// followed by one or more digits.
func consumeExponent(value string, pos int) bool {
	if pos < len(value) && (value[pos] == '+' || value[pos] == '-') {
		pos++
	}

	hasDigit := false

	for pos < len(value) {
		if value[pos] < '0' || value[pos] > '9' {
			return false
		}

		hasDigit = true
		pos++
	}

	return hasDigit
}
