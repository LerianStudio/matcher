//go:build unit

package shared_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestExtractionStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status shared.ExtractionStatus
		want   string
	}{
		{"pending", shared.ExtractionStatusPending, "PENDING"},
		{"complete", shared.ExtractionStatusComplete, "COMPLETE"},
		{"failed", shared.ExtractionStatusFailed, "FAILED"},
		{"empty", shared.ExtractionStatus(""), ""},
		{"invalid", shared.ExtractionStatus("INVALID"), "INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.status.String())
		})
	}
}

func TestExtractionStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status shared.ExtractionStatus
		want   bool
	}{
		{"pending is valid", shared.ExtractionStatusPending, true},
		{"complete is valid", shared.ExtractionStatusComplete, true},
		{"failed is valid", shared.ExtractionStatusFailed, true},
		{"empty is invalid", shared.ExtractionStatus(""), false},
		{"lowercase is invalid", shared.ExtractionStatus("pending"), false},
		{"unknown is invalid", shared.ExtractionStatus("UNKNOWN"), false},
		{"partial match is invalid", shared.ExtractionStatus("PEND"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.status.IsValid())
		})
	}
}

func TestParseExtractionStatus_AllValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  shared.ExtractionStatus
	}{
		{"PENDING", shared.ExtractionStatusPending},
		{"COMPLETE", shared.ExtractionStatusComplete},
		{"FAILED", shared.ExtractionStatusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := shared.ParseExtractionStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseExtractionStatus_AllInvalid(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		" ",
		"pending",
		"PEND",
		"COMPLETED",
		"123",
		"PENDING ",
		" PENDING",
	}

	for _, input := range tests {
		name := input
		if name == "" {
			name = "empty"
		} else if name == " " {
			name = "whitespace"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := shared.ParseExtractionStatus(input)
			require.ErrorIs(t, err, shared.ErrInvalidExtractionStatus)
		})
	}
}

func TestTransactionStatus_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status shared.TransactionStatus
		want   string
	}{
		{"unmatched", shared.TransactionStatusUnmatched, "UNMATCHED"},
		{"matched", shared.TransactionStatusMatched, "MATCHED"},
		{"ignored", shared.TransactionStatusIgnored, "IGNORED"},
		{"pending_review", shared.TransactionStatusPendingReview, "PENDING_REVIEW"},
		{"empty", shared.TransactionStatus(""), ""},
		{"invalid", shared.TransactionStatus("INVALID"), "INVALID"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.status.String())
		})
	}
}

func TestTransactionStatus_IsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status shared.TransactionStatus
		want   bool
	}{
		{"unmatched is valid", shared.TransactionStatusUnmatched, true},
		{"matched is valid", shared.TransactionStatusMatched, true},
		{"ignored is valid", shared.TransactionStatusIgnored, true},
		{"pending_review is valid", shared.TransactionStatusPendingReview, true},
		{"empty is invalid", shared.TransactionStatus(""), false},
		{"lowercase is invalid", shared.TransactionStatus("unmatched"), false},
		{"unknown is invalid", shared.TransactionStatus("UNKNOWN"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.status.IsValid())
		})
	}
}

func TestParseTransactionStatus_AllValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  shared.TransactionStatus
	}{
		{"UNMATCHED", shared.TransactionStatusUnmatched},
		{"MATCHED", shared.TransactionStatusMatched},
		{"IGNORED", shared.TransactionStatusIgnored},
		{"PENDING_REVIEW", shared.TransactionStatusPendingReview},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got, err := shared.ParseTransactionStatus(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTransactionStatus_AllInvalid(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		" ",
		"unmatched",
		"UNMATCH",
		"MATCHED ",
		" MATCHED",
		"PENDING",
		"REVIEW",
	}

	for _, input := range tests {
		name := input
		if name == "" {
			name = "empty"
		} else if name == " " {
			name = "whitespace"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := shared.ParseTransactionStatus(input)
			require.ErrorIs(t, err, shared.ErrInvalidTransactionStatus)
		})
	}
}

func TestNewTransaction_DefaultValues(t *testing.T) {
	t.Parallel()

	jobID := uuid.New()
	sourceID := uuid.New()
	date := time.Now().UTC()
	metadata := map[string]any{"key": "value"}

	tx, err := shared.NewTransaction(
		jobID,
		sourceID,
		"ext-001",
		decimal.NewFromFloat(100.50),
		"USD",
		date,
		"Test transaction",
		metadata,
	)
	require.NoError(t, err)

	require.NotNil(t, tx)
	assert.NotEqual(t, uuid.Nil, tx.ID)
	assert.Equal(t, jobID, tx.IngestionJobID)
	assert.Equal(t, sourceID, tx.SourceID)
	assert.Equal(t, "ext-001", tx.ExternalID)
	assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(100.50)))
	assert.Equal(t, "USD", tx.Currency)
	assert.Equal(t, shared.ExtractionStatusPending, tx.ExtractionStatus)
	assert.Equal(t, shared.TransactionStatusUnmatched, tx.Status)
	assert.Equal(t, date, tx.Date)
	assert.Equal(t, "Test transaction", tx.Description)
	assert.Equal(t, "value", tx.Metadata["key"])
	assert.False(t, tx.CreatedAt.IsZero())
	assert.False(t, tx.UpdatedAt.IsZero())
	assert.Nil(t, tx.AmountBase)
	assert.Nil(t, tx.BaseCurrency)
	assert.Nil(t, tx.FXRate)
	assert.Nil(t, tx.FXRateSource)
	assert.Nil(t, tx.FXRateEffDate)
}

func TestNewTransaction_NilMetadata(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-002",
		decimal.NewFromInt(50),
		"EUR",
		time.Now().UTC(),
		"No metadata",
		nil,
	)
	require.NoError(t, err)

	require.NotNil(t, tx)
	assert.Nil(t, tx.Metadata)
}

func TestNewTransaction_ZeroAmount(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-003",
		decimal.Zero,
		"GBP",
		time.Now().UTC(),
		"Zero amount",
		nil,
	)
	require.NoError(t, err)

	require.NotNil(t, tx)
	assert.True(t, tx.Amount.IsZero())
}

func TestTransaction_MarkExtractionComplete_NilReceiver(t *testing.T) {
	t.Parallel()

	var tx *shared.Transaction
	err := tx.MarkExtractionComplete()
	require.ErrorIs(t, err, shared.ErrTransactionNil)
}

func TestTransaction_MarkExtractionFailed_NilReceiver(t *testing.T) {
	t.Parallel()

	var tx *shared.Transaction
	err := tx.MarkExtractionFailed()
	require.ErrorIs(t, err, shared.ErrTransactionNil)
}

func TestTransaction_SetFXConversion_NilReceiver(t *testing.T) {
	t.Parallel()

	var tx *shared.Transaction
	err := tx.SetFXConversion(
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromFloat(1.2),
		"provider",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrTransactionNil)
}

func TestTransaction_MarkExtractionComplete_FromPending(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-004",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	originalUpdatedAt := tx.UpdatedAt
	time.Sleep(time.Millisecond)

	err = tx.MarkExtractionComplete()
	require.NoError(t, err)
	assert.Equal(t, shared.ExtractionStatusComplete, tx.ExtractionStatus)
	assert.True(t, tx.UpdatedAt.After(originalUpdatedAt))
}

func TestTransaction_MarkExtractionComplete_AlreadyComplete(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-005",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, tx.MarkExtractionComplete())
	err = tx.MarkExtractionComplete()
	require.ErrorIs(t, err, shared.ErrExtractionNotPending)
}

func TestTransaction_MarkExtractionComplete_AlreadyFailed(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-006",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, tx.MarkExtractionFailed())
	err = tx.MarkExtractionComplete()
	require.ErrorIs(t, err, shared.ErrExtractionNotPending)
}

func TestTransaction_MarkExtractionFailed_FromPending(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-007",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	originalUpdatedAt := tx.UpdatedAt
	time.Sleep(time.Millisecond)

	err = tx.MarkExtractionFailed()
	require.NoError(t, err)
	assert.Equal(t, shared.ExtractionStatusFailed, tx.ExtractionStatus)
	assert.True(t, tx.UpdatedAt.After(originalUpdatedAt))
}

func TestTransaction_MarkExtractionFailed_AlreadyFailed(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-008",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, tx.MarkExtractionFailed())
	err = tx.MarkExtractionFailed()
	require.ErrorIs(t, err, shared.ErrExtractionAlreadyFailed)
}

func TestTransaction_MarkExtractionFailed_AlreadyComplete(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-009",
		decimal.NewFromInt(100),
		"USD",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, tx.MarkExtractionComplete())
	err = tx.MarkExtractionFailed()
	require.ErrorIs(t, err, shared.ErrExtractionAlreadyComplete)
}

func TestTransaction_SetFXConversion_Success(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-010",
		decimal.NewFromFloat(100.0),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	originalUpdatedAt := tx.UpdatedAt
	time.Sleep(time.Millisecond)

	effectiveDate := time.Now().UTC()
	err = tx.SetFXConversion(
		decimal.NewFromFloat(100.0),
		"USD",
		decimal.NewFromFloat(1.1),
		"ECB",
		effectiveDate,
	)
	require.NoError(t, err)

	require.NotNil(t, tx.AmountBase)
	assert.True(t, tx.AmountBase.Equal(decimal.NewFromFloat(110.0)))
	require.NotNil(t, tx.BaseCurrency)
	assert.Equal(t, "USD", *tx.BaseCurrency)
	require.NotNil(t, tx.FXRate)
	assert.True(t, tx.FXRate.Equal(decimal.NewFromFloat(1.1)))
	require.NotNil(t, tx.FXRateSource)
	assert.Equal(t, "ECB", *tx.FXRateSource)
	require.NotNil(t, tx.FXRateEffDate)
	assert.Equal(t, effectiveDate, *tx.FXRateEffDate)
	assert.True(t, tx.UpdatedAt.After(originalUpdatedAt))
}

func TestTransaction_SetFXConversion_EmptyBaseCurrency(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-011",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromInt(100),
		"",
		decimal.NewFromFloat(1.1),
		"ECB",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrBaseCurrencyRequired)
}

func TestTransaction_SetFXConversion_WhitespaceBaseCurrency(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-012",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromInt(100),
		"   ",
		decimal.NewFromFloat(1.1),
		"ECB",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrBaseCurrencyRequired)
}

func TestTransaction_SetFXConversion_NegativeAmount(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-013",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromFloat(-100.0),
		"USD",
		decimal.NewFromFloat(1.1),
		"ECB",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrBaseAmountNegative)
}

func TestTransaction_SetFXConversion_ZeroFXRate(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-014",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromInt(100),
		"USD",
		decimal.Zero,
		"ECB",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrFXRateNotPositive)
}

func TestTransaction_SetFXConversion_NegativeFXRate(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-015",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromInt(100),
		"USD",
		decimal.NewFromFloat(-1.1),
		"ECB",
		time.Now().UTC(),
	)
	require.ErrorIs(t, err, shared.ErrFXRateNotPositive)
}

func TestTransaction_SetFXConversion_ZeroAmount(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-016",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.Zero,
		"USD",
		decimal.NewFromFloat(1.1),
		"ECB",
		time.Now().UTC(),
	)
	require.NoError(t, err)

	require.NotNil(t, tx.AmountBase)
	assert.True(t, tx.AmountBase.IsZero())
}

func TestTransaction_SetFXConversion_TrimsCurrency(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-017",
		decimal.NewFromInt(100),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromInt(100),
		"  USD  ",
		decimal.NewFromFloat(1.0),
		"ECB",
		time.Now().UTC(),
	)
	require.NoError(t, err)
	assert.Equal(t, "USD", *tx.BaseCurrency)
}

func TestTransaction_SetFXConversion_Precision(t *testing.T) {
	t.Parallel()

	tx, err := shared.NewTransaction(
		uuid.New(),
		uuid.New(),
		"ext-018",
		decimal.NewFromFloat(123.45),
		"EUR",
		time.Now().UTC(),
		"test",
		nil,
	)
	require.NoError(t, err)

	err = tx.SetFXConversion(
		decimal.NewFromFloat(123.45),
		"USD",
		decimal.NewFromFloat(1.23456789),
		"ECB",
		time.Now().UTC(),
	)
	require.NoError(t, err)

	expected := decimal.NewFromFloat(123.45).Mul(decimal.NewFromFloat(1.23456789))
	require.NotNil(t, tx.AmountBase)
	assert.True(t, tx.AmountBase.Equal(expected))
}

func TestTransaction_FieldValues(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	jobID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	sourceID := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	date := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	now := time.Now().UTC()

	amountBase := decimal.NewFromFloat(110.0)
	baseCurrency := "USD"
	fxRate := decimal.NewFromFloat(1.1)
	fxSource := "ECB"

	tx := &shared.Transaction{
		ID:               id,
		IngestionJobID:   jobID,
		SourceID:         sourceID,
		ExternalID:       "ext-unique",
		Amount:           decimal.NewFromFloat(100.0),
		Currency:         "EUR",
		AmountBase:       &amountBase,
		BaseCurrency:     &baseCurrency,
		FXRate:           &fxRate,
		FXRateSource:     &fxSource,
		FXRateEffDate:    &date,
		ExtractionStatus: shared.ExtractionStatusComplete,
		Status:           shared.TransactionStatusMatched,
		Date:             date,
		Description:      "Full transaction",
		Metadata:         map[string]any{"type": "payment"},
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	assert.Equal(t, id, tx.ID)
	assert.Equal(t, jobID, tx.IngestionJobID)
	assert.Equal(t, sourceID, tx.SourceID)
	assert.Equal(t, "ext-unique", tx.ExternalID)
	assert.True(t, tx.Amount.Equal(decimal.NewFromFloat(100.0)))
	assert.Equal(t, "EUR", tx.Currency)
	assert.True(t, tx.AmountBase.Equal(decimal.NewFromFloat(110.0)))
	assert.Equal(t, "USD", *tx.BaseCurrency)
	assert.True(t, tx.FXRate.Equal(decimal.NewFromFloat(1.1)))
	assert.Equal(t, "ECB", *tx.FXRateSource)
	assert.Equal(t, date, *tx.FXRateEffDate)
	assert.Equal(t, shared.ExtractionStatusComplete, tx.ExtractionStatus)
	assert.Equal(t, shared.TransactionStatusMatched, tx.Status)
	assert.Equal(t, date, tx.Date)
	assert.Equal(t, "Full transaction", tx.Description)
	assert.Equal(t, "payment", tx.Metadata["type"])
}

func TestSentinelErrors(t *testing.T) {
	t.Parallel()

	t.Run("extraction status errors", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, shared.ErrInvalidExtractionStatus.Error())
	})

	t.Run("transaction status errors", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, shared.ErrInvalidTransactionStatus.Error())
	})

	t.Run("transaction operation errors", func(t *testing.T) {
		t.Parallel()
		assert.NotEmpty(t, shared.ErrTransactionNil.Error())
		assert.NotEmpty(t, shared.ErrExtractionNotPending.Error())
		assert.NotEmpty(t, shared.ErrExtractionAlreadyComplete.Error())
		assert.NotEmpty(t, shared.ErrExtractionAlreadyFailed.Error())
		assert.NotEmpty(t, shared.ErrBaseCurrencyRequired.Error())
		assert.NotEmpty(t, shared.ErrBaseAmountNegative.Error())
		assert.NotEmpty(t, shared.ErrFXRateNotPositive.Error())
	})
}
