package parsers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var (
	errMissingFieldMap        = errors.New("field map is required")
	errMissingFieldMapping    = errors.New("field map mapping is required")
	errMissingIngestionJob    = errors.New("ingestion job is required")
	errInvalidMappingFormat   = errors.New("field map mapping must contain string values")
	errReaderRequired         = errors.New("reader is required")
	errCallbackRequired       = errors.New("chunk callback is required")
	errMissingMappingKey      = errors.New("field map missing required mapping key")
	errEmptyMappingValue      = errors.New("field map mapping value must be non-empty")
	errDateEmpty              = errors.New("date value is empty")
	errUnsupportedDateFormat  = errors.New("unsupported date format")
	errRegistryNotInitialized = errors.New("parser registry not initialized")
	errUnsupportedFormat      = errors.New("unsupported format")
	errJSONPayloadInvalid     = errors.New("json payload must be an object or array of objects")
	errJSONArrayNotObjects    = errors.New("json array must contain objects")
	errJSONUnexpectedKeyType  = errors.New("expected string key in json object")
	errInvalidCurrencyCode    = errors.New("invalid ISO 4217 currency code")
)

const maxCurrencyLength = 3

const (
	unixTimestampSecondsLen = 10
	unixTimestampMillisLen  = 13
	base10                  = 10
)

var requiredMappingKeys = []string{"external_id", "amount", "currency", "date"}

func mappingFromFieldMap(fieldMap *shared.FieldMap) (map[string]string, error) {
	if fieldMap == nil {
		return nil, errMissingFieldMap
	}

	if len(fieldMap.Mapping) == 0 {
		return nil, errMissingFieldMapping
	}

	mapping := make(map[string]string)

	for _, key := range requiredMappingKeys {
		value, ok := fieldMap.Mapping[key]
		if !ok {
			return nil, fmt.Errorf("%w: %s", errMissingMappingKey, key)
		}

		name, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field map mapping for %s: %w", key, errInvalidMappingFormat)
		}

		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("%w: %s", errEmptyMappingValue, key)
		}

		mapping[key] = name
	}

	if value, ok := fieldMap.Mapping["description"]; ok {
		if name, isString := value.(string); isString {
			if trimmed := strings.TrimSpace(name); trimmed != "" {
				mapping["description"] = trimmed
			}
		}
	}

	return mapping, nil
}

func normalizeTransaction(
	job *entities.IngestionJob,
	mapping map[string]string,
	row map[string]any,
	rowNumber int,
) (*shared.Transaction, *ports.ParseError) {
	if job == nil {
		return nil, &ports.ParseError{
			Row:     rowNumber,
			Field:   "job",
			Message: errMissingIngestionJob.Error(),
		}
	}

	externalID, parseErr := requiredString(row, mapping, "external_id", rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	amountValue, parseErr := requiredString(row, mapping, "amount", rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	amount, err := decimal.NewFromString(amountValue)
	if err != nil {
		return nil, &ports.ParseError{
			Row:     rowNumber,
			Field:   "amount",
			Message: "invalid decimal amount",
		}
	}

	currency, parseErr := parseCurrency(row, mapping, rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	dateValue, parseErr := requiredString(row, mapping, "date", rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	date, err := parseTime(dateValue)
	if err != nil {
		return nil, &ports.ParseError{Row: rowNumber, Field: "date", Message: "invalid date format"}
	}

	description := ""

	if fieldName, ok := mapping["description"]; ok {
		if value, ok := getStringValue(row, fieldName); ok {
			description = value
		}
	}

	mappedFields := buildMappedFieldSet(mapping)
	metadata := buildMetadata(row, mappedFields)

	transaction, err := shared.NewTransaction(
		job.ID,
		job.SourceID,
		externalID,
		amount,
		currency,
		date,
		description,
		metadata,
	)
	if err != nil {
		return nil, &ports.ParseError{
			Row:     rowNumber,
			Field:   "transaction",
			Message: err.Error(),
		}
	}

	if err := transaction.MarkExtractionComplete(); err != nil {
		return nil, &ports.ParseError{
			Row:     rowNumber,
			Field:   "extraction_status",
			Message: err.Error(),
		}
	}

	return transaction, nil
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

func getStringValue(row map[string]any, fieldName string) (string, bool) {
	value, ok := row[fieldName]
	if !ok || value == nil {
		return "", false
	}

	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed), true
	case json.Number:
		return strings.TrimSpace(typed.String()), true
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed)), true
	}
}

// dateLayouts contains supported date formats in order of parsing priority.
// Formats are ordered from most specific to least specific to avoid ambiguity.
// Note: Ambiguous formats like MM/DD/YYYY and DD/MM/YYYY are intentionally excluded
// as they cannot be reliably distinguished without explicit configuration.
var dateLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02 15:04:05",
	"2006/01/02",
	"20060102150405",
	"20060102",
	"02-Jan-2006 15:04:05",
	"02-Jan-2006",
	"Jan 2, 2006 15:04:05",
	"Jan 2, 2006",
	"January 2, 2006 15:04:05",
	"January 2, 2006",
	"2 Jan 2006 15:04:05",
	"2 Jan 2006",
}

func parseTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, errDateEmpty
	}

	for _, layout := range dateLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}

	if parsed, ok := parseUnixTimestamp(value); ok {
		return parsed, nil
	}

	return time.Time{}, fmt.Errorf("%w: %s", errUnsupportedDateFormat, value)
}

// parseUnixTimestamp attempts to parse a string as a Unix timestamp.
// Supports exactly 10 digits (seconds) or 13 digits (milliseconds).
func parseUnixTimestamp(value string) (time.Time, bool) {
	if len(value) != unixTimestampSecondsLen && len(value) != unixTimestampMillisLen {
		return time.Time{}, false
	}

	for _, c := range value {
		if c < '0' || c > '9' {
			return time.Time{}, false
		}
	}

	var timestamp int64
	for _, c := range value {
		timestamp = timestamp*base10 + int64(c-'0')
	}

	if len(value) == unixTimestampSecondsLen {
		return time.Unix(timestamp, 0).UTC(), true
	}

	return time.UnixMilli(timestamp).UTC(), true
}

func updateDateRange(dateRange *ports.DateRange, date time.Time) *ports.DateRange {
	if dateRange == nil {
		return &ports.DateRange{Start: date, End: date}
	}

	if date.Before(dateRange.Start) {
		dateRange.Start = date
	}

	if date.After(dateRange.End) {
		dateRange.End = date
	}

	return dateRange
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
func buildMetadata(row map[string]any, mappedFields map[string]bool) map[string]any {
	if row == nil {
		return map[string]any{}
	}

	metadata := make(map[string]any, len(row))
	for key, value := range row {
		if mappedFields[key] {
			continue
		}

		metadata[key] = value
	}

	return metadata
}
