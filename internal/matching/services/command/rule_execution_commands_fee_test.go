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

func newTestFeeSchedule(currency string, rate decimal.Decimal) *fee.FeeSchedule {
	return &fee.FeeSchedule{
		ID:               uuid.New(),
		TenantID:         uuid.New(),
		Name:             "Test Schedule",
		Currency:         currency,
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:       uuid.New(),
				Name:     "Processing Fee",
				Priority: 1,
				Structure: fee.PercentageFee{
					Rate: rate,
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			},
		},
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
}

func newTestRule(scheduleID uuid.UUID, priority int, predicates []fee.FieldPredicate) *fee.FeeRule {
	return &fee.FeeRule{
		ID:            uuid.New(),
		ContextID:     uuid.New(),
		Side:          fee.MatchingSideAny,
		FeeScheduleID: scheduleID,
		Name:          "Test Rule",
		Priority:      priority,
		Predicates:    predicates,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
}

func newTestTransaction(sourceID uuid.UUID, amount string, currency string, metadata map[string]any) *shared.Transaction {
	return &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString(amount),
		Currency:   currency,
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-" + uuid.New().String()[:8],
		Metadata:   metadata,
	}
}

func TestMapTransactionsWithFeeRules_NoRules(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "1000.00", "USD", map[string]any{"type": "wire"})

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		nil, // no rules
		nil, // no schedules
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, txn.Amount.Equal(result[0].Amount),
		"amount should pass through unchanged with no rules")
	assert.True(t, txn.Amount.Equal(result[0].OriginalAmount),
		"original amount should equal amount with no rules")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown when no rules")

	// Also test with empty slices
	result = mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{},
		map[uuid.UUID]*fee.FeeSchedule{},
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, txn.Amount.Equal(result[0].Amount),
		"amount should pass through unchanged with empty rules")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown when empty rules")
}

func TestMapTransactionsWithFeeRules_RuleMatchesNetMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "1000.00", "USD", map[string]any{"type": "wire"})

	// 1.5% fee schedule
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)

	// Original amount preserved
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should be preserved")

	// Fee is 1.5% of 1000 = 15.00, Net = 1000 - 15 = 985.00
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].Amount),
		"amount should be net (gross - fee): got %s", result[0].Amount)

	require.NotNil(t, result[0].FeeBreakdown, "fee breakdown should be populated")
	assert.True(t, decimal.RequireFromString("15.00").Equal(result[0].FeeBreakdown.TotalFee.Amount),
		"total fee should be 15.00")
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].FeeBreakdown.NetAmount.Amount),
		"net amount in breakdown should be 985.00")
}

func TestMapTransactionsWithFeeRules_RuleMatchesGrossMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "1000.00", "USD", map[string]any{"type": "wire"})

	// 1.5% fee schedule
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeGross,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)

	// Original amount preserved
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should be preserved")

	// GROSS mode: gross = net / (1 - rate) = 1000 / 0.985 = 1015.228...
	// With rounding scale 2, HALF_UP: gross = 1015.23, fee = 15.23
	expectedGross := decimal.RequireFromString("1015.23")
	assert.True(t, result[0].Amount.Equal(expectedGross),
		"expected gross %s, got %s", expectedGross, result[0].Amount)

	require.NotNil(t, result[0].FeeBreakdown, "fee breakdown should be populated")
	assert.True(t, result[0].FeeBreakdown.TotalFee.Amount.Equal(decimal.RequireFromString("15.23")),
		"expected fee 15.23, got %s", result[0].FeeBreakdown.TotalFee.Amount)
}

func TestMapTransactionsWithFeeRules_NoMatchingRule(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	// Transaction has type=ach, but rule only matches type=wire
	txn := newTestTransaction(sourceID, "500.00", "USD", map[string]any{"type": "ach"})

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.02"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, decimal.RequireFromString("500.00").Equal(result[0].Amount),
		"amount should pass through unchanged when no rule matches")
	assert.True(t, decimal.RequireFromString("500.00").Equal(result[0].OriginalAmount),
		"original amount should equal amount")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown when no rule matches")
}

func TestMapTransactionsWithFeeRules_NilTransactionSkipped(t *testing.T) {
	t.Parallel()

	txns := []*shared.Transaction{nil, nil}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		txns,
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	assert.Empty(t, result, "nil transactions should be skipped")
}

func TestMapTransactionsWithFeeRules_NilMetadataNoMatch(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	// Transaction has nil metadata -- rule predicates can't match
	txn := &shared.Transaction{
		ID:         uuid.New(),
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("750.00"),
		Currency:   "USD",
		Date:       time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID: "REF-NIL-META",
		Metadata:   nil,
	}

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.01"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "channel",
			Operator: fee.PredicateOperatorEquals,
			Value:    "online",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, decimal.RequireFromString("750.00").Equal(result[0].Amount),
		"amount should pass through unchanged when metadata is nil")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown when metadata is nil")
}

func TestMapTransactionsWithFeeRules_MultipleTransactionsDifferentRules(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()

	// Schedule A: 1.5% fee for wire transfers
	scheduleA := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))
	ruleA := newTestRule(scheduleA.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	// Schedule B: 3% fee for ach transfers
	scheduleB := newTestFeeSchedule("USD", decimal.RequireFromString("0.03"))
	ruleB := newTestRule(scheduleB.ID, 2, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "ach",
		},
	})

	txnWire := newTestTransaction(sourceID, "1000.00", "USD", map[string]any{"type": "wire"})
	txnACH := newTestTransaction(sourceID, "2000.00", "USD", map[string]any{"type": "ach"})
	txnCheck := newTestTransaction(sourceID, "500.00", "USD", map[string]any{"type": "check"})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		scheduleA.ID: scheduleA,
		scheduleB.ID: scheduleB,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txnWire, txnACH, txnCheck},
		[]*fee.FeeRule{ruleA, ruleB},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 3)

	// Wire: 1000 - (1000 * 0.015) = 985.00
	assert.True(t, decimal.RequireFromString("985.00").Equal(result[0].Amount),
		"wire net amount should be 985.00, got %s", result[0].Amount)
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"wire original should be preserved")
	require.NotNil(t, result[0].FeeBreakdown, "wire should have fee breakdown")

	// ACH: 2000 - (2000 * 0.03) = 1940.00
	assert.True(t, decimal.RequireFromString("1940.00").Equal(result[1].Amount),
		"ach net amount should be 1940.00, got %s", result[1].Amount)
	assert.True(t, decimal.RequireFromString("2000.00").Equal(result[1].OriginalAmount),
		"ach original should be preserved")
	require.NotNil(t, result[1].FeeBreakdown, "ach should have fee breakdown")

	// Check: no rule matches, passes through unchanged
	assert.True(t, decimal.RequireFromString("500.00").Equal(result[2].Amount),
		"check amount should be unchanged, got %s", result[2].Amount)
	assert.True(t, decimal.RequireFromString("500.00").Equal(result[2].OriginalAmount),
		"check original should be preserved")
	assert.Nil(t, result[2].FeeBreakdown, "check should have no fee breakdown")
}

func TestMapTransactionsWithFeeRules_CurrencyFromBaseCurrency(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	baseCurrency := "EUR"
	baseAmount := decimal.RequireFromString("920.00")

	txn := &shared.Transaction{
		ID:           uuid.New(),
		SourceID:     sourceID,
		Amount:       decimal.RequireFromString("1000.00"),
		Currency:     "USD",
		AmountBase:   &baseAmount,
		BaseCurrency: &baseCurrency,
		Date:         time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC),
		ExternalID:   "REF-BASE",
		Metadata:     map[string]any{"type": "wire"},
	}

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.01"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.Equal(t, "EUR", result[0].CurrencyBase,
		"CurrencyBase should be populated from transaction.BaseCurrency")
	require.NotNil(t, result[0].AmountBase,
		"AmountBase should be populated from transaction.AmountBase")
	assert.True(t, baseAmount.Equal(*result[0].AmountBase),
		"AmountBase should match transaction.AmountBase")
}

func TestMapTransactionsWithFeeRules_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	// Transaction in EUR but schedule in USD -- currency mismatch should skip normalization
	txn := newTestTransaction(sourceID, "1000.00", "EUR", map[string]any{"type": "wire"})

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].Amount),
		"amount should pass through unchanged on currency mismatch")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown when currency mismatch")
}

func TestMapTransactionsWithFeeRules_NoneMode(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "1000.00", "USD", map[string]any{"type": "wire"})

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.015"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "type",
			Operator: fee.PredicateOperatorEquals,
			Value:    "wire",
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	// NormalizationModeNone should pass through unchanged even when rules match
	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNone,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].Amount),
		"amount should pass through unchanged in None mode")
	assert.True(t, decimal.RequireFromString("1000.00").Equal(result[0].OriginalAmount),
		"original amount should equal amount")
	assert.Nil(t, result[0].FeeBreakdown,
		"no breakdown in None mode")
}

func TestMapTransactionsWithFeeRules_CatchAllRule(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "500.00", "USD", map[string]any{"anything": "goes"})

	// A catch-all rule has an empty predicate slice -- always matches
	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.02"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)

	// 2% of 500 = 10, net = 490
	assert.True(t, decimal.RequireFromString("490.00").Equal(result[0].Amount),
		"catch-all rule should apply: expected 490.00, got %s", result[0].Amount)
	require.NotNil(t, result[0].FeeBreakdown, "catch-all should produce fee breakdown")
}

func TestMapTransactionsWithFeeRules_InOperatorPredicate(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txn := newTestTransaction(sourceID, "800.00", "USD", map[string]any{"channel": "mobile"})

	schedule := newTestFeeSchedule("USD", decimal.RequireFromString("0.01"))

	rule := newTestRule(schedule.ID, 1, []fee.FieldPredicate{
		{
			Field:    "channel",
			Operator: fee.PredicateOperatorIn,
			Values:   []string{"web", "mobile", "api"},
		},
	})

	schedules := map[uuid.UUID]*fee.FeeSchedule{
		schedule.ID: schedule,
	}

	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		[]*fee.FeeRule{rule},
		schedules,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)

	// 1% of 800 = 8, net = 792
	assert.True(t, decimal.RequireFromString("792.00").Equal(result[0].Amount),
		"IN operator should match: expected 792.00, got %s", result[0].Amount)
	require.NotNil(t, result[0].FeeBreakdown, "IN-matched rule should produce fee breakdown")
}

func TestMapTransactionsWithFeeRules_EmptyInput(t *testing.T) {
	t.Parallel()

	result := mapTransactionsWithFeeRules(
		context.Background(),
		nil,
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	assert.Empty(t, result, "nil input should return empty slice")

	result = mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{},
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	assert.Empty(t, result, "empty input should return empty slice")
}

func TestMapTransactionsWithFeeRules_TransactionFieldsPreserved(t *testing.T) {
	t.Parallel()

	sourceID := uuid.New()
	txnID := uuid.New()
	txnDate := time.Date(2026, 3, 15, 10, 30, 0, 0, time.UTC)

	txn := &shared.Transaction{
		ID:         txnID,
		SourceID:   sourceID,
		Amount:     decimal.RequireFromString("123.45"),
		Currency:   "GBP",
		Date:       txnDate,
		ExternalID: "EXT-XYZ",
		Metadata:   map[string]any{"type": "card"},
	}

	// No rules -- verify all candidate fields are correctly mapped from the transaction
	result := mapTransactionsWithFeeRules(
		context.Background(),
		[]*shared.Transaction{txn},
		nil,
		nil,
		fee.NormalizationModeNet,
		&libLog.NopLogger{},
	)

	require.Len(t, result, 1)

	c := result[0]
	assert.Equal(t, txnID, c.ID, "ID should match")
	assert.Equal(t, sourceID, c.SourceID, "SourceID should match")
	assert.True(t, decimal.RequireFromString("123.45").Equal(c.Amount), "Amount should match")
	assert.Equal(t, "GBP", c.Currency, "Currency should match")
	assert.Equal(t, txnDate, c.Date, "Date should match")
	assert.Equal(t, "EXT-XYZ", c.Reference, "Reference should map from ExternalID")
	assert.True(t, decimal.RequireFromString("123.45").Equal(c.OriginalAmount), "OriginalAmount should match Amount")
	assert.Equal(t, "", c.CurrencyBase, "CurrencyBase should be empty when BaseCurrency is nil")
	assert.Nil(t, c.AmountBase, "AmountBase should be nil when not set")
}
