//go:build unit

package shared_test

import (
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
