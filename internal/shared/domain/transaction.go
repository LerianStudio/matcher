// Package shared provides shared domain types used across bounded contexts.
package shared

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/lib-commons/v4/commons/assert"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// Static errors for extraction status validation.
var (
	// ErrInvalidExtractionStatus indicates an invalid extraction status value.
	ErrInvalidExtractionStatus = errors.New("invalid extraction status")
	// ErrInvalidTransactionStatus indicates an invalid transaction status value.
	ErrInvalidTransactionStatus = errors.New("invalid transaction status")
)

// Static errors for transaction operations.
var (
	// ErrTransactionNil indicates a nil transaction receiver.
	ErrTransactionNil = errors.New("transaction is nil")
	// ErrExtractionNotPending indicates extraction status must be pending to complete.
	ErrExtractionNotPending = errors.New("extraction status must be pending to complete")
	// ErrExtractionAlreadyComplete indicates extraction is already complete.
	ErrExtractionAlreadyComplete = errors.New("extraction already complete")
	// ErrExtractionAlreadyFailed indicates extraction has already failed.
	ErrExtractionAlreadyFailed = errors.New("extraction already failed")
	// ErrBaseCurrencyRequired indicates base currency is required for FX conversion.
	ErrBaseCurrencyRequired = errors.New("base currency is required")
	// ErrBaseAmountNegative indicates base amount must be non-negative.
	ErrBaseAmountNegative = errors.New("base amount must be non-negative")
	// ErrFXRateNotPositive indicates FX rate must be positive.
	ErrFXRateNotPositive = errors.New("fx rate must be positive")
	// ErrIngestionJobIDRequired indicates ingestion job ID is required.
	ErrIngestionJobIDRequired = errors.New("ingestion job id is required")
	// ErrSourceIDRequired indicates source ID is required.
	ErrSourceIDRequired = errors.New("source id is required")
	// ErrExternalIDRequired indicates external ID is required.
	ErrExternalIDRequired = errors.New("external id is required")
	// ErrCurrencyRequired indicates currency is required.
	ErrCurrencyRequired = errors.New("currency is required")
	// ErrTransactionTenantIDRequired indicates a valid tenant ID is required.
	ErrTransactionTenantIDRequired = errors.New("tenant id is required")
)

// ExtractionStatus matches PostgreSQL enum `transaction_extraction_status` from 000001_init_schema.up.sql.
type ExtractionStatus string

// Extraction status constants.
const (
	ExtractionStatusPending  ExtractionStatus = "PENDING"
	ExtractionStatusComplete ExtractionStatus = "COMPLETE"
	ExtractionStatusFailed   ExtractionStatus = "FAILED"
)

// IsValid returns true if the extraction status is a known valid value.
func (s ExtractionStatus) IsValid() bool {
	switch s {
	case ExtractionStatusPending, ExtractionStatusComplete, ExtractionStatusFailed:
		return true
	}

	return false
}

func (s ExtractionStatus) String() string {
	return string(s)
}

// ParseExtractionStatus parses a string into an ExtractionStatus, returning an error for invalid values.
func ParseExtractionStatus(s string) (ExtractionStatus, error) {
	status := ExtractionStatus(s)
	if !status.IsValid() {
		return "", fmt.Errorf("%w: %q", ErrInvalidExtractionStatus, s)
	}

	return status, nil
}

// TransactionStatus matches PostgreSQL enum `transaction_status` from 000001_init_schema.up.sql.
type TransactionStatus string

// Transaction status constants.
const (
	TransactionStatusUnmatched     TransactionStatus = "UNMATCHED"
	TransactionStatusMatched       TransactionStatus = "MATCHED"
	TransactionStatusIgnored       TransactionStatus = "IGNORED"
	TransactionStatusPendingReview TransactionStatus = "PENDING_REVIEW"
)

// IsValid returns true if the transaction status is a known valid value.
func (s TransactionStatus) IsValid() bool {
	switch s {
	case TransactionStatusUnmatched,
		TransactionStatusMatched,
		TransactionStatusIgnored,
		TransactionStatusPendingReview:
		return true
	}

	return false
}

func (s TransactionStatus) String() string {
	return string(s)
}

// ParseTransactionStatus parses a string into a TransactionStatus, returning an error for invalid values.
func ParseTransactionStatus(s string) (TransactionStatus, error) {
	status := TransactionStatus(s)
	if !status.IsValid() {
		return "", ErrInvalidTransactionStatus
	}

	return status, nil
}

// Transaction represents a canonical record of a financial movement with controlled state transitions
// Matches schema: migrations/000001_init_schema.up.sql:84-104.
type Transaction struct {
	ID             uuid.UUID
	IngestionJobID uuid.UUID
	SourceID       uuid.UUID
	ExternalID     string

	Amount   decimal.Decimal
	Currency string

	AmountBase    *decimal.Decimal
	BaseCurrency  *string
	FXRate        *decimal.Decimal
	FXRateSource  *string
	FXRateEffDate *time.Time

	ExtractionStatus ExtractionStatus
	Status           TransactionStatus

	Date        time.Time
	Description string
	Metadata    map[string]any

	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewTransaction creates a new Transaction with the provided values and default statuses.
// Returns an error if required fields are missing or invalid.
func NewTransaction(
	ctx context.Context,
	tenantID uuid.UUID,
	jobID, sourceID uuid.UUID,
	externalID string,
	amount decimal.Decimal,
	currency string,
	date time.Time,
	description string,
	metadata map[string]any,
) (*Transaction, error) {
	// tenantID is validated to ensure the caller operates within a valid tenant
	// context, but is NOT stored on the Transaction struct. Tenant isolation is
	// enforced at the PostgreSQL schema level (search_path per tenant), not at
	// the entity level. This validation prevents transaction creation outside
	// a valid tenant context.
	asserter := assert.New(ctx, nil, constants.ApplicationName, "shared.transaction.new")

	if err := asserter.That(ctx, tenantID != uuid.Nil, "tenant id is required"); err != nil {
		return nil, fmt.Errorf("transaction tenant id: %w", ErrTransactionTenantIDRequired)
	}

	if err := asserter.That(ctx, jobID != uuid.Nil, "ingestion job id is required"); err != nil {
		return nil, fmt.Errorf("transaction ingestion job id: %w", ErrIngestionJobIDRequired)
	}

	if err := asserter.That(ctx, sourceID != uuid.Nil, "source id is required"); err != nil {
		return nil, fmt.Errorf("transaction source id: %w", ErrSourceIDRequired)
	}

	if err := asserter.NotEmpty(ctx, strings.TrimSpace(externalID), "external id is required"); err != nil {
		return nil, fmt.Errorf("transaction external id: %w", ErrExternalIDRequired)
	}

	normalizedCurrency := strings.ToUpper(strings.TrimSpace(currency))

	if err := asserter.NotEmpty(ctx, normalizedCurrency, "currency is required"); err != nil {
		return nil, fmt.Errorf("transaction currency: %w", ErrCurrencyRequired)
	}

	var metaCopy map[string]any
	if metadata != nil {
		metaCopy = make(map[string]any, len(metadata))
		for k, v := range metadata {
			metaCopy[k] = cloneMetadataValue(v)
		}
	}

	now := time.Now().UTC()

	return &Transaction{
		ID:               uuid.New(),
		IngestionJobID:   jobID,
		SourceID:         sourceID,
		ExternalID:       externalID,
		Amount:           amount,
		Currency:         normalizedCurrency,
		ExtractionStatus: ExtractionStatusPending,
		Status:           TransactionStatusUnmatched,
		Date:             date,
		Description:      description,
		Metadata:         metaCopy,
		CreatedAt:        now,
		UpdatedAt:        now,
	}, nil
}

// MarkExtractionComplete transitions the extraction status from PENDING to COMPLETE.
func (txn *Transaction) MarkExtractionComplete() error {
	if txn == nil {
		return ErrTransactionNil
	}

	if txn.ExtractionStatus != ExtractionStatusPending {
		return ErrExtractionNotPending
	}

	txn.ExtractionStatus = ExtractionStatusComplete
	txn.UpdatedAt = time.Now().UTC()

	return nil
}

// MarkExtractionFailed transitions the extraction status to FAILED if not already terminal.
func (txn *Transaction) MarkExtractionFailed() error {
	if txn == nil {
		return ErrTransactionNil
	}

	if txn.ExtractionStatus == ExtractionStatusComplete {
		return ErrExtractionAlreadyComplete
	}

	if txn.ExtractionStatus == ExtractionStatusFailed {
		return ErrExtractionAlreadyFailed
	}

	txn.ExtractionStatus = ExtractionStatusFailed
	txn.UpdatedAt = time.Now().UTC()

	return nil
}

// SetFXConversion sets the FX conversion fields on the transaction.
//
// Parameters:
//   - originalAmount: The pre-conversion amount in the original currency
//   - baseCurrency: The target/base currency code (e.g., "USD")
//   - fxRate: The exchange rate to apply (original currency → base currency)
//   - source: The FX rate provider (e.g., "ECB", "provider")
//   - effectiveDate: The date the FX rate was effective
//
// The method computes AmountBase = originalAmount * fxRate and stores the result.
// Callers must provide the pre-conversion amount; the final converted value is
// calculated and stored internally.
func (txn *Transaction) SetFXConversion(
	originalAmount decimal.Decimal,
	baseCurrency string,
	fxRate decimal.Decimal,
	source string,
	effectiveDate time.Time,
) error {
	if txn == nil {
		return ErrTransactionNil
	}

	if strings.TrimSpace(baseCurrency) == "" {
		return ErrBaseCurrencyRequired
	}

	if originalAmount.IsNegative() {
		return ErrBaseAmountNegative
	}

	if fxRate.LessThanOrEqual(decimal.Zero) {
		return ErrFXRateNotPositive
	}

	trimmedCurrency := strings.TrimSpace(baseCurrency)
	convertedAmount := originalAmount.Mul(fxRate)
	txn.AmountBase = &convertedAmount
	txn.BaseCurrency = &trimmedCurrency
	txn.FXRate = &fxRate
	txn.FXRateSource = &source
	txn.FXRateEffDate = &effectiveDate
	txn.UpdatedAt = time.Now().UTC()

	return nil
}

// cloneMetadataValue recursively deep-copies a metadata value so that nested
// maps and slices are fully independent of the caller's original references.
// Scalar types (string, int, float64, bool, nil, etc.) are returned as-is
// since they are already value types or immutable.
func cloneMetadataValue(value any) any {
	switch val := value.(type) {
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, inner := range val {
			cp[k] = cloneMetadataValue(inner)
		}

		return cp
	case []any:
		cp := make([]any, len(val))
		for i, inner := range val {
			cp[i] = cloneMetadataValue(inner)
		}

		return cp
	default:
		return value
	}
}
