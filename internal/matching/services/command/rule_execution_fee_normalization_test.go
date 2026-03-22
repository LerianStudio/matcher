//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestMapTransactions_Empty(t *testing.T) {
	t.Parallel()

	result := mapTransactions(nil)
	assert.Empty(t, result)
}

func TestMapTransactions_SkipsNil(t *testing.T) {
	t.Parallel()

	txs := []*shared.Transaction{nil, nil}
	result := mapTransactions(txs)
	assert.Empty(t, result)
}

func TestMapTransactions_BasicMapping(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000290001")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000290002")
	baseCur := "EUR"
	baseAmt := decimal.NewFromInt(90)

	txs := []*shared.Transaction{
		{
			ID:           id,
			SourceID:     sourceID,
			Amount:       decimal.NewFromInt(100),
			Currency:     "USD",
			AmountBase:   &baseAmt,
			BaseCurrency: &baseCur,
			Date:         time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExternalID:   "REF-001",
		},
	}

	result := mapTransactions(txs)
	require.Len(t, result, 1)

	c := result[0]
	assert.Equal(t, id, c.ID)
	assert.Equal(t, sourceID, c.SourceID)
	assert.True(t, c.Amount.Equal(decimal.NewFromInt(100)))
	assert.Equal(t, "USD", c.Currency)
	assert.NotNil(t, c.AmountBase)
	assert.Equal(t, "EUR", c.CurrencyBase)
	assert.Equal(t, "REF-001", c.Reference)
	assert.True(t, c.OriginalAmount.Equal(decimal.NewFromInt(100)))
}

func TestMapTransactions_NilBaseCurrency(t *testing.T) {
	t.Parallel()

	txs := []*shared.Transaction{
		{
			ID:           uuid.New(),
			Amount:       decimal.NewFromInt(100),
			Currency:     "USD",
			BaseCurrency: nil,
		},
	}

	result := mapTransactions(txs)
	require.Len(t, result, 1)
	assert.Empty(t, result[0].CurrencyBase)
}

func TestMapTransactionsWithFeeRules_Empty(t *testing.T) {
	t.Parallel()

	result := mapTransactionsWithFeeRules(
		context.Background(),
		nil,
		nil,
		nil,
		fee.NormalizationModeNone,
		&libLog.NopLogger{},
	)
	assert.Empty(t, result)
}

func TestMapTransactionsWithFeeRules_SkipsNil(t *testing.T) {
	t.Parallel()

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{nil, nil},
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)
	assert.Empty(t, result)
}

func TestMapTransactionsWithFeeRules_NoMatchingSchedule(t *testing.T) {
	t.Parallel()

	txs := []*shared.Transaction{
		{
			ID:       uuid.New(),
			Amount:   decimal.NewFromInt(100),
			Currency: "USD",
		},
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		txs,
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)
	require.Len(t, result, 1)
	assert.True(t, result[0].Amount.Equal(decimal.NewFromInt(100)))
}

func TestValidateFeeCurrencies_NilTransaction(t *testing.T) {
	t.Parallel()

	result := validateFeeCurrencies(context.Background(), nil, &fee.FeeSchedule{Currency: "USD"}, &libLog.NopLogger{})
	assert.False(t, result)
}

func TestValidateFeeCurrencies_NilSchedule(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{Currency: "USD"}
	result := validateFeeCurrencies(context.Background(), txn, nil, &libLog.NopLogger{})
	assert.False(t, result)
}

func TestValidateFeeCurrencies_CurrencyMatch(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{ID: uuid.New(), Currency: "USD"}
	schedule := &fee.FeeSchedule{Currency: "USD"}
	result := validateFeeCurrencies(context.Background(), txn, schedule, &libLog.NopLogger{})
	assert.True(t, result)
}

func TestValidateFeeCurrencies_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{ID: uuid.New(), Currency: "EUR"}
	schedule := &fee.FeeSchedule{Currency: "USD"}
	result := validateFeeCurrencies(context.Background(), txn, schedule, &libLog.NopLogger{})
	assert.False(t, result)
}

func TestApplyFeeNormalization_NilSchedule_NoOp(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		ID:       uuid.New(),
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}

	c := mapTransactions([]*shared.Transaction{txn})[0]
	original := c.Amount

	applyFeeNormalization(context.Background(), &c, txn, nil, fee.NormalizationModeNet, &libLog.NopLogger{})
	assert.True(t, c.Amount.Equal(original))
}

func TestApplyFeeNormalization_NoneMode_NoOp(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		ID:       uuid.New(),
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}

	c := mapTransactions([]*shared.Transaction{txn})[0]
	original := c.Amount

	schedule := &fee.FeeSchedule{Currency: "USD"}
	applyFeeNormalization(context.Background(), &c, txn, schedule, fee.NormalizationModeNone, &libLog.NopLogger{})
	assert.True(t, c.Amount.Equal(original))
}

func TestValidateFeeCurrencies_InvalidCurrency(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{ID: uuid.New(), Currency: ""}
	schedule := &fee.FeeSchedule{Currency: "USD"}
	result := validateFeeCurrencies(context.Background(), txn, schedule, &libLog.NopLogger{})
	assert.False(t, result)
}
