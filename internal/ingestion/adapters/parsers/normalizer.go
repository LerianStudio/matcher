package parsers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	"github.com/LerianStudio/matcher/internal/ingestion/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var requiredMappingKeys = []string{"external_id", "amount", "currency", "date"}

// tenantIDFromContext extracts the tenant ID from context as a uuid.UUID.
// Falls back to the default tenant when the context does not carry explicit
// tenant claims (single-tenant / auth-disabled mode).
// Returns an error if the tenant ID string cannot be parsed as a UUID,
// enabling callers to fail fast on systemic context problems instead of
// producing N misleading per-row parse errors.
func tenantIDFromContext(ctx context.Context) (uuid.UUID, error) {
	tid := auth.GetTenantID(ctx)

	parsed, err := uuid.Parse(tid)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid tenant id in context %q: %w", tid, err)
	}

	return parsed, nil
}

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

// normalizeTransaction converts a raw parsed row into a domain Transaction.
// It orchestrates field extraction, validation, and entity construction.
func normalizeTransaction(
	ctx context.Context,
	tenantID uuid.UUID,
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

	// external_id is extracted WITHOUT formula sanitization to preserve the raw
	// value for dedup hash integrity and search. Formula injection is a display
	// concern handled at output boundaries (preview, export, API responses).
	externalID, parseErr := requiredRawString(row, mapping, "external_id", rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	amount, parseErr := parseAmount(mapping, row, rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	currency, parseErr := parseCurrency(row, mapping, rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	date, parseErr := extractDate(mapping, row, rowNumber)
	if parseErr != nil {
		return nil, parseErr
	}

	// Extract optional description field.
	var description string

	if descField, ok := mapping["description"]; ok {
		if v, found := getStringValue(row, descField); found {
			description = v
		}
	}

	// Gather all unmapped fields into metadata.
	metadata := buildMetadata(row, buildMappedFieldSet(mapping))

	return buildTransaction(ctx, tenantID, job, externalID, amount, currency, date, description, metadata, rowNumber)
}

// parseAmount extracts the amount string and converts it to a decimal value.
func parseAmount(mapping map[string]string, row map[string]any, rowNumber int) (decimal.Decimal, *ports.ParseError) {
	amountValue, parseErr := requiredString(row, mapping, "amount", rowNumber)
	if parseErr != nil {
		return decimal.Zero, parseErr
	}

	amount, err := decimal.NewFromString(amountValue)
	if err != nil {
		return decimal.Zero, &ports.ParseError{
			Row:     rowNumber,
			Field:   "amount",
			Message: "invalid decimal amount",
		}
	}

	return amount, nil
}

// extractDate retrieves the date string and parses it into a time.Time.
func extractDate(mapping map[string]string, row map[string]any, rowNumber int) (time.Time, *ports.ParseError) {
	dateValue, parseErr := requiredString(row, mapping, "date", rowNumber)
	if parseErr != nil {
		return time.Time{}, parseErr
	}

	date, err := parseTime(dateValue)
	if err != nil {
		return time.Time{}, &ports.ParseError{Row: rowNumber, Field: "date", Message: "invalid date format"}
	}

	return date, nil
}

// buildTransaction constructs the domain Transaction entity and marks it extraction-complete.
func buildTransaction(
	ctx context.Context,
	tenantID uuid.UUID,
	job *entities.IngestionJob,
	externalID string,
	amount decimal.Decimal,
	currency string,
	date time.Time,
	description string,
	metadata map[string]any,
	rowNumber int,
) (*shared.Transaction, *ports.ParseError) {
	transaction, err := shared.NewTransaction(
		ctx,
		tenantID,
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
