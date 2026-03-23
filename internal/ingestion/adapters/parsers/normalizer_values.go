package parsers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	"github.com/LerianStudio/matcher/internal/shared/sanitize"
)

// sanitizeFormulaInjection delegates to the shared sanitize package to prevent
// CSV/spreadsheet formula injection. See sanitize.SanitizeFormulaInjection for
// full documentation.
func sanitizeFormulaInjection(value string) string {
	return sanitize.SanitizeFormulaInjection(value)
}

// getStringValue extracts a string value from a row map, trims whitespace,
// and sanitizes against formula injection. Note: sanitization runs before
// downstream parsing (e.g., decimal.NewFromString for amount fields), so
// formula-prefixed values like "=100.50" become "'=100.50" and will fail
// type-specific parsing with a generic error. This is intentional — formula
// characters in numeric fields indicate malformed or malicious input.
func getStringValue(row map[string]any, fieldName string) (string, bool) {
	value, ok := row[fieldName]
	if !ok || value == nil {
		return "", false
	}

	var result string

	switch typed := value.(type) {
	case string:
		result = strings.TrimSpace(typed)
	case json.Number:
		result = strings.TrimSpace(typed.String())
	default:
		result = strings.TrimSpace(fmt.Sprintf("%v", typed))
	}

	return sanitizeFormulaInjection(result), true
}

// buildMappedFieldSet creates a set of raw field names that are already stored
// in dedicated columns (external_id, amount, currency, date, description).
// These fields should be excluded from the metadata JSONB to avoid data
// duplication and inadvertent storage of sensitive values that are already
// persisted elsewhere.
func buildMappedFieldSet(mapping map[string]string) map[string]bool {
	mapped := make(map[string]bool, len(mapping))
	for _, rawFieldName := range mapping {
		if rawFieldName != "" {
			mapped[rawFieldName] = true
		}
	}

	return mapped
}

// buildMetadata creates a metadata map from the raw row data, excluding fields
// that are already persisted in dedicated transaction columns.
// String values are sanitized against formula injection before storage.
//
// Sensitive field values are preserved at ingestion time for business logic
// (fee-rule evaluation, matching, audit). Redaction is applied at output
// boundaries (API responses, exports, logs) instead — see
// redactSensitivePreviewColumns in preview_queries.go for an example.
func buildMetadata(row map[string]any, mappedFields map[string]bool) map[string]any {
	if row == nil {
		return map[string]any{}
	}

	metadata := make(map[string]any, len(row))
	for key, value := range row {
		if mappedFields[key] {
			continue
		}

		switch v := value.(type) {
		case string:
			metadata[key] = sanitizeFormulaInjection(strings.TrimSpace(v))
		case json.Number:
			metadata[key] = sanitizeFormulaInjection(strings.TrimSpace(v.String()))
		default:
			metadata[key] = value
		}
	}

	return metadata
}

// requiredRawString extracts a required field value from the row WITHOUT
// formula sanitization. This is used for fields like external_id where the
// raw value must be preserved verbatim for dedup hash integrity and search.
func requiredRawString(
	row map[string]any,
	mapping map[string]string,
	canonicalField string,
	rowNumber int,
) (string, *ports.ParseError) {
	fieldName, ok := mapping[canonicalField]
	if !ok {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   canonicalField,
			Message: "missing field mapping",
		}
	}

	value, exists := row[fieldName]
	if !exists || value == nil {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   canonicalField,
			Message: "missing required field",
		}
	}

	var result string

	switch v := value.(type) {
	case string:
		result = strings.TrimSpace(v)
	case json.Number:
		result = strings.TrimSpace(v.String())
	default:
		result = strings.TrimSpace(fmt.Sprintf("%v", value))
	}

	if result == "" {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   canonicalField,
			Message: "missing required field",
		}
	}

	return result, nil
}

func requiredString(
	row map[string]any,
	mapping map[string]string,
	canonicalField string,
	rowNumber int,
) (string, *ports.ParseError) {
	fieldName, ok := mapping[canonicalField]
	if !ok {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   canonicalField,
			Message: "missing field mapping",
		}
	}

	value, ok := getStringValue(row, fieldName)
	if !ok || strings.TrimSpace(value) == "" {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   canonicalField,
			Message: "missing required field",
		}
	}

	return value, nil
}

func parseCurrency(
	row map[string]any,
	mapping map[string]string,
	rowNumber int,
) (string, *ports.ParseError) {
	currency, parseErr := requiredString(row, mapping, "currency", rowNumber)
	if parseErr != nil {
		return "", parseErr
	}

	currency = strings.ToUpper(currency)
	if !isValidCurrencyCode(currency) {
		return "", &ports.ParseError{
			Row:     rowNumber,
			Field:   "currency",
			Message: errInvalidCurrencyCode.Error(),
		}
	}

	return currency, nil
}
