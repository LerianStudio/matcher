//go:build unit

package shared_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestTransactionLifecycle(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	sourceID := uuid.New()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		jobID,
		sourceID,
		"ext-123",
		decimal.NewFromFloat(42.5),
		"usd",
		time.Now().UTC(),
		"payment",
		map[string]any{"raw": "value"},
	)
	require.NoError(t, err)

	require.Equal(t, shared.TransactionStatusUnmatched, tx.Status)
	require.Equal(t, shared.ExtractionStatusPending, tx.ExtractionStatus)
	require.Equal(t, "USD", tx.Currency)

	require.NoError(t, tx.MarkExtractionComplete())
	require.Equal(t, shared.ExtractionStatusComplete, tx.ExtractionStatus)
	require.Error(t, tx.MarkExtractionComplete())
	require.Error(t, tx.MarkExtractionFailed())

	effectiveDate := time.Now().UTC()
	require.NoError(
		t,
		tx.SetFXConversion(
			decimal.NewFromFloat(40.0),
			"USD",
			decimal.NewFromFloat(1.2),
			"provider",
			effectiveDate,
		),
	)
	require.NotNil(t, tx.AmountBase)
	require.True(t, tx.AmountBase.Equal(decimal.NewFromFloat(48.0)))
	require.Equal(t, "USD", *tx.BaseCurrency)
}

func TestTransactionExtractionFailure(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	sourceID := uuid.New()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		jobID,
		sourceID,
		"ext-124",
		decimal.NewFromFloat(10.0),
		"usd",
		time.Now().UTC(),
		"refund",
		map[string]any{},
	)
	require.NoError(t, err)

	require.NoError(t, tx.MarkExtractionFailed())
	require.Equal(t, shared.ExtractionStatusFailed, tx.ExtractionStatus)
	require.Error(t, tx.MarkExtractionFailed())
	require.Error(t, tx.MarkExtractionComplete())
}

func TestTransactionFXConversionValidation(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	sourceID := uuid.New()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		jobID,
		sourceID,
		"ext-125",
		decimal.NewFromFloat(0),
		"usd",
		time.Now().UTC(),
		"zero",
		map[string]any{},
	)
	require.NoError(t, err)

	require.Error(
		t,
		tx.SetFXConversion(
			decimal.NewFromFloat(10.0),
			"",
			decimal.NewFromFloat(1.2),
			"provider",
			time.Now().UTC(),
		),
	)
	require.Error(
		t,
		tx.SetFXConversion(
			decimal.NewFromFloat(-10.0),
			"USD",
			decimal.NewFromFloat(1.2),
			"provider",
			time.Now().UTC(),
		),
	)
	require.Error(
		t,
		tx.SetFXConversion(
			decimal.NewFromFloat(10.0),
			"USD",
			decimal.NewFromFloat(-1.2),
			"provider",
			time.Now().UTC(),
		),
	)
	require.Error(
		t,
		tx.SetFXConversion(
			decimal.NewFromFloat(10.0),
			"USD",
			decimal.NewFromFloat(0.0),
			"provider",
			time.Now().UTC(),
		),
	)
}

func TestTransactionImmutability(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	sourceID := uuid.New()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		jobID,
		sourceID,
		"ext",
		decimal.NewFromInt(10),
		"USD",
		time.Now().UTC(),
		"desc",
		map[string]any{},
	)
	require.NoError(t, err)
	initialAmount := tx.Amount
	initialDate := tx.Date
	initialCurrency := tx.Currency
	initialExternalID := tx.ExternalID

	require.NoError(t, tx.MarkExtractionComplete())
	require.Error(t, tx.MarkExtractionFailed())
	require.NoError(
		t,
		tx.SetFXConversion(
			decimal.NewFromInt(10),
			"USD",
			decimal.NewFromInt(1),
			"provider",
			time.Now().UTC(),
		),
	)

	require.Equal(t, initialAmount, tx.Amount)
	require.Equal(t, initialDate, tx.Date)
	require.Equal(t, initialCurrency, tx.Currency)
	require.Equal(t, initialExternalID, tx.ExternalID)
	require.Equal(t, shared.ExtractionStatusComplete, tx.ExtractionStatus)
}

func TestExtractionStatusParsing(t *testing.T) {
	t.Parallel()

	valid := []shared.ExtractionStatus{
		shared.ExtractionStatusPending,
		shared.ExtractionStatusComplete,
		shared.ExtractionStatusFailed,
	}
	for _, status := range valid {
		parsed, err := shared.ParseExtractionStatus(status.String())
		require.NoError(t, err)
		require.Equal(t, status, parsed)
	}

	_, err := shared.ParseExtractionStatus("INVALID")
	require.Error(t, err)

	invalidCases := []string{"", "pending", "Pending", " PENDING "}
	for _, input := range invalidCases {
		_, err := shared.ParseExtractionStatus(input)
		require.Error(t, err, "expected error for input: %q", input)
	}
}

func TestTransactionStatusParsing(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		input      string
		wantErr    bool
		wantStatus shared.TransactionStatus
	}{
		{
			name:       "unmatched",
			input:      shared.TransactionStatusUnmatched.String(),
			wantStatus: shared.TransactionStatusUnmatched,
		},
		{
			name:       "matched",
			input:      shared.TransactionStatusMatched.String(),
			wantStatus: shared.TransactionStatusMatched,
		},
		{
			name:       "ignored",
			input:      shared.TransactionStatusIgnored.String(),
			wantStatus: shared.TransactionStatusIgnored,
		},
		{
			name:       "pending review",
			input:      shared.TransactionStatusPendingReview.String(),
			wantStatus: shared.TransactionStatusPendingReview,
		},
		{
			name:    "invalid",
			input:   "INVALID",
			wantErr: true,
		},
		{
			name:    "empty",
			input:   "",
			wantErr: true,
		},
		{
			name:    "space",
			input:   " ",
			wantErr: true,
		},
		{
			name:    "lowercase",
			input:   "unmatched",
			wantErr: true,
		},
		{
			name:    "padded",
			input:   " Unmatched ",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := shared.ParseTransactionStatus(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, parsed)
		})
	}
}

func TestNewTransaction_NilTenantID_ReturnsError(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.Nil,
		uuid.New(),
		uuid.New(),
		"ext-tenant-nil",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.ErrorIs(t, err, shared.ErrTransactionTenantIDRequired)
	require.Nil(t, tx)
}

func TestNewTransaction_NilContext_DoesNotPanic(t *testing.T) {
	t.Parallel()

	// A nil ctx should not cause a panic; the constructor should still
	// succeed when all other arguments are valid.
	//nolint:staticcheck // intentionally passing nil context for test
	tx, err := shared.NewTransaction(
		nil,
		uuid.New(),
		uuid.New(),
		uuid.New(),
		"ext-nil-ctx",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, tx)
}

func TestNewTransaction_MetadataDefensiveCopy(t *testing.T) {
	t.Parallel()

	nestedMap := map[string]any{"inner": "original_inner"}
	nestedSlice := []any{"a", "b"}

	original := map[string]any{
		"key1":   "value1",
		"key2":   "value2",
		"nested": nestedMap,
		"list":   nestedSlice,
	}

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		"ext-meta-copy",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		original,
	)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// Verify initial values are correct.
	require.Equal(t, "value1", tx.Metadata["key1"])
	require.Equal(t, "value2", tx.Metadata["key2"])

	txNested, ok := tx.Metadata["nested"].(map[string]any)
	require.True(t, ok, "nested metadata value must be map[string]any")
	require.Equal(t, "original_inner", txNested["inner"])

	txList, ok := tx.Metadata["list"].([]any)
	require.True(t, ok, "list metadata value must be []any")
	require.Equal(t, []any{"a", "b"}, txList)

	// Mutate the original top-level map after construction.
	original["key1"] = "mutated"
	original["key3"] = "new_key"

	// The entity's metadata must remain unchanged.
	require.Equal(t, "value1", tx.Metadata["key1"], "entity metadata must not be affected by external mutation")
	_, hasKey3 := tx.Metadata["key3"]
	require.False(t, hasKey3, "entity metadata must not gain keys added to original map after construction")

	// Mutate the nested map — the transaction must be fully isolated.
	nestedMap["inner"] = "mutated_inner"
	nestedMap["injected"] = "attack"

	require.Equal(t, "original_inner", txNested["inner"],
		"nested map mutation must not propagate into transaction metadata")
	_, hasInjected := txNested["injected"]
	require.False(t, hasInjected,
		"keys added to original nested map must not appear in transaction metadata")

	// Mutate the nested slice — the transaction must be fully isolated.
	nestedSlice[0] = "MUTATED"

	require.Equal(t, "a", txList[0],
		"nested slice mutation must not propagate into transaction metadata")
}

func TestNewTransactionWithDonatedMetadata_StoresMapDirectly(t *testing.T) {
	t.Parallel()

	metadata := map[string]any{"key": "value"}

	tx, err := shared.NewTransactionWithDonatedMetadata(
		context.Background(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		"ext-donated",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		metadata,
	)
	require.NoError(t, err)
	require.NotNil(t, tx)

	// Donated path stores the caller's map directly. Mutating the original
	// after construction should be visible on the transaction — this
	// asserts the ownership contract the caller is opting into.
	metadata["key"] = "mutated"
	metadata["added"] = "later"

	require.Equal(t, "mutated", tx.Metadata["key"],
		"donated metadata must reflect caller mutations (ownership was transferred)")
	require.Equal(t, "later", tx.Metadata["added"])
}

func TestNewTransactionWithDonatedMetadata_ValidatesRequiredFields(t *testing.T) {
	t.Parallel()

	_, err := shared.NewTransactionWithDonatedMetadata(
		context.Background(),
		uuid.Nil, // invalid tenant
		uuid.New(),
		uuid.New(),
		"ext",
		decimal.NewFromInt(1),
		"USD",
		time.Now().UTC(),
		"",
		nil,
	)
	require.ErrorIs(t, err, shared.ErrTransactionTenantIDRequired)
}

func TestNewTransaction_NilMetadata_RemainsNil(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		context.Background(),
		uuid.New(),
		uuid.New(),
		uuid.New(),
		"ext-nil-meta",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, tx)
	require.Nil(t, tx.Metadata, "nil metadata should remain nil, not become an empty map")
}
