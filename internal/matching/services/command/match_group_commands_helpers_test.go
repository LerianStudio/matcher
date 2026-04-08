//go:build unit

package command

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
	outboxmocks "github.com/LerianStudio/matcher/internal/shared/ports/mocks"
)

// --- parseAmount tests ---

func TestParseAmount_StringSuccess(t *testing.T) {
	t.Parallel()

	amount, feeErr := parseAmount("123.45")
	require.Nil(t, feeErr)
	assert.True(t, amount.Equal(decimal.RequireFromString("123.45")))
}

func TestParseAmount_StringInvalid(t *testing.T) {
	t.Parallel()

	_, feeErr := parseAmount("not-a-number")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

func TestParseAmount_Float64(t *testing.T) {
	t.Parallel()

	amount, feeErr := parseAmount(float64(42.5))
	require.Nil(t, feeErr)
	assert.True(t, amount.Equal(decimal.NewFromFloat(42.5)))
}

func TestParseAmount_Int(t *testing.T) {
	t.Parallel()

	amount, feeErr := parseAmount(int(100))
	require.Nil(t, feeErr)
	assert.True(t, amount.Equal(decimal.NewFromInt(100)))
}

func TestParseAmount_Int64(t *testing.T) {
	t.Parallel()

	amount, feeErr := parseAmount(int64(9999))
	require.Nil(t, feeErr)
	assert.True(t, amount.Equal(decimal.NewFromInt(9999)))
}

func TestParseAmount_UnsupportedType(t *testing.T) {
	t.Parallel()

	_, feeErr := parseAmount(true)
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

// --- extractActualFee tests ---

func TestExtractActualFee_NilMetadata(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{Metadata: nil}
	_, feeErr := extractActualFee(txn, "USD")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

func TestExtractActualFee_MissingFeeKey(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{"other": "data"},
	}
	_, feeErr := extractActualFee(txn, "USD")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

func TestExtractActualFee_FeeNotMap(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{"fee": "not-a-map"},
	}
	_, feeErr := extractActualFee(txn, "USD")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

func TestExtractActualFee_MissingAmountInFeeMap(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{
			"fee": map[string]any{"currency": "USD"},
		},
	}
	_, feeErr := extractActualFee(txn, "USD")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeDataMissing, feeErr.reason)
}

func TestExtractActualFee_SuccessWithMatchingCurrency(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "10.50",
				"currency": "USD",
			},
		},
	}
	money, feeErr := extractActualFee(txn, "USD")
	require.Nil(t, feeErr)
	assert.True(t, money.Amount.Equal(decimal.RequireFromString("10.50")))
	assert.Equal(t, "USD", money.Currency)
}

func TestExtractActualFee_CurrencyMismatch(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "10.50",
				"currency": "EUR",
			},
		},
	}
	_, feeErr := extractActualFee(txn, "USD")
	require.NotNil(t, feeErr)
	assert.Equal(t, enums.ReasonFeeCurrencyMismatch, feeErr.reason)
}

func TestExtractActualFee_NoCurrencyUsesExpected(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount": float64(5.25),
			},
		},
	}
	money, feeErr := extractActualFee(txn, "BRL")
	require.Nil(t, feeErr)
	assert.True(t, money.Amount.Equal(decimal.NewFromFloat(5.25)))
	assert.Equal(t, "BRL", money.Currency)
}

func TestExtractActualFee_EmptyCurrencyUsesExpected(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   float64(5.25),
				"currency": "  ",
			},
		},
	}
	money, feeErr := extractActualFee(txn, "BRL")
	require.Nil(t, feeErr)
	assert.Equal(t, "BRL", money.Currency)
}

// --- buildExceptionInputs tests ---

func TestBuildExceptionInputs_EmptyTxIDs(t *testing.T) {
	t.Parallel()

	result := buildExceptionInputs(nil, nil, nil, nil)
	assert.Nil(t, result)
}

func TestBuildExceptionInputs_DeduplicatesTxIDs(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100001")
	result := buildExceptionInputs(
		[]uuid.UUID{txID, txID, txID},
		nil,
		nil,
		nil,
	)
	assert.Len(t, result, 1)
	assert.Equal(t, txID, result[0].TransactionID)
}

func TestBuildExceptionInputs_WithReasons(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100002")
	reasons := map[uuid.UUID]string{txID: "some_reason"}
	result := buildExceptionInputs([]uuid.UUID{txID}, nil, nil, reasons)
	require.Len(t, result, 1)
	assert.Equal(t, "some_reason", result[0].Reason)
}

func TestBuildExceptionInputs_WithTxByID(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100003")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100004")
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Date:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	txByID := map[uuid.UUID]*shared.Transaction{txID: txn}
	sourceTypeByID := map[uuid.UUID]string{sourceID: "ledger"}

	result := buildExceptionInputs([]uuid.UUID{txID}, txByID, sourceTypeByID, nil)
	require.Len(t, result, 1)
	assert.Equal(t, txID, result[0].TransactionID)
	assert.Equal(t, "ledger", result[0].SourceType)
}

func TestBuildExceptionInputs_NilTxInMap(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100005")
	txByID := map[uuid.UUID]*shared.Transaction{txID: nil}

	result := buildExceptionInputs([]uuid.UUID{txID}, txByID, nil, nil)
	require.Len(t, result, 1)
	assert.Equal(t, txID, result[0].TransactionID)
}

// --- buildExceptionInputFromTx tests ---

func TestBuildExceptionInputFromTx_NilTxn(t *testing.T) {
	t.Parallel()

	result := buildExceptionInputFromTx(nil, nil, "some-reason")
	assert.Nil(t, result)
}

func TestBuildExceptionInputFromTx_WithAmountBase(t *testing.T) {
	t.Parallel()

	base := decimal.NewFromInt(50)
	txn := &shared.Transaction{
		ID:         uuid.MustParse("00000000-0000-0000-0000-000000100010"),
		SourceID:   uuid.MustParse("00000000-0000-0000-0000-000000100011"),
		Amount:     decimal.NewFromInt(100),
		AmountBase: &base,
		Currency:   "USD",
	}

	result := buildExceptionInputFromTx(txn, nil, "test-reason")
	require.NotNil(t, result)
	assert.True(t, result.AmountAbsBase.Equal(decimal.NewFromInt(50)))
	assert.Equal(t, "test-reason", result.Reason)
}

func TestBuildExceptionInputFromTx_WithoutAmountBase(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000100012"),
		SourceID: uuid.MustParse("00000000-0000-0000-0000-000000100013"),
		Amount:   decimal.NewFromInt(200),
		Currency: "USD",
	}

	result := buildExceptionInputFromTx(txn, nil, "")
	require.NotNil(t, result)
	assert.True(t, result.AmountAbsBase.Equal(decimal.NewFromInt(200)))
}

func TestBuildExceptionInputFromTx_FXMissingFlag(t *testing.T) {
	t.Parallel()

	baseCur := "EUR"
	txn := &shared.Transaction{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000100014"),
		SourceID:     uuid.MustParse("00000000-0000-0000-0000-000000100015"),
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   nil,
		BaseCurrency: &baseCur,
	}

	result := buildExceptionInputFromTx(txn, nil, "")
	require.NotNil(t, result)
	assert.True(t, result.FXMissing)
}

// --- buildSourceTypeMap tests ---

func TestBuildSourceTypeMap_NilSources(t *testing.T) {
	t.Parallel()

	result := buildSourceTypeMap(nil)
	assert.Nil(t, result)
}

func TestBuildSourceTypeMap_EmptySources(t *testing.T) {
	t.Parallel()

	result := buildSourceTypeMap([]*ports.SourceInfo{})
	assert.Nil(t, result)
}

func TestBuildSourceTypeMap_SkipsNilSources(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000100020")
	sources := []*ports.SourceInfo{
		nil,
		{ID: id, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		nil,
	}
	result := buildSourceTypeMap(sources)
	require.Len(t, result, 1)
	assert.Equal(t, string(ports.SourceTypeLedger), result[id])
}

// --- collectUnmatched tests ---

func TestCollectUnmatched_AllMatched(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000100050")
	txs := []*shared.Transaction{{ID: id}}
	matched := map[uuid.UUID]struct{}{id: {}}

	result := collectUnmatched(txs, matched)
	assert.Empty(t, result)
}

func TestCollectUnmatched_NoneMatched(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000100051")
	txs := []*shared.Transaction{{ID: id}}

	result := collectUnmatched(txs, map[uuid.UUID]struct{}{})
	require.Len(t, result, 1)
	assert.Equal(t, id, result[0])
}

func TestCollectUnmatched_SkipsNilTx(t *testing.T) {
	t.Parallel()

	txs := []*shared.Transaction{nil, nil}
	result := collectUnmatched(txs, map[uuid.UUID]struct{}{})
	assert.Empty(t, result)
}

// --- indexTransactions tests ---

func TestIndexTransactions_SkipsNil(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000100060")
	txs := []*shared.Transaction{nil, {ID: id}, nil}

	result := indexTransactions(txs)
	require.Len(t, result, 1)
	assert.NotNil(t, result[id])
}

// --- mergeTransactionMaps tests ---

func TestMergeTransactionMaps_Empty(t *testing.T) {
	t.Parallel()

	result := mergeTransactionMaps()
	assert.Empty(t, result)
}

func TestMergeTransactionMaps_MultipleMaps(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000100070")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000100071")
	tx1 := &shared.Transaction{ID: id1}
	tx2 := &shared.Transaction{ID: id2}

	m1 := map[uuid.UUID]*shared.Transaction{id1: tx1}
	m2 := map[uuid.UUID]*shared.Transaction{id2: tx2}

	result := mergeTransactionMaps(m1, m2)
	assert.Len(t, result, 2)
	assert.Equal(t, tx1, result[id1])
	assert.Equal(t, tx2, result[id2])
}

// --- mergeMatched tests ---

func TestMergeMatched_NilDest(t *testing.T) {
	t.Parallel()

	src := map[uuid.UUID]struct{}{uuid.New(): {}}
	// Should not panic
	mergeMatched(nil, src)
}

func TestMergeMatched_NilSrc(t *testing.T) {
	t.Parallel()

	dest := map[uuid.UUID]struct{}{}
	// Should not panic
	mergeMatched(dest, nil)
}

func TestMergeMatched_Merges(t *testing.T) {
	t.Parallel()

	id := uuid.MustParse("00000000-0000-0000-0000-000000100080")
	dest := map[uuid.UUID]struct{}{}
	src := map[uuid.UUID]struct{}{id: {}}

	mergeMatched(dest, src)
	_, ok := dest[id]
	assert.True(t, ok)
}

// --- allocationFields tests ---

func TestAllocationFields_NoAllocation(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000100090"),
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}

	allocated, currency, expected, errInfo := allocationFields(
		txn,
		map[uuid.UUID]decimal.Decimal{},
		map[uuid.UUID]string{},
		map[uuid.UUID]bool{},
	)

	assert.Nil(t, errInfo)
	assert.True(t, allocated.Equal(decimal.NewFromInt(100)))
	assert.Equal(t, "USD", currency)
	assert.True(t, expected.Equal(decimal.NewFromInt(100)))
}

func TestAllocationFields_WithAllocation_NoBaseAmount(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100091")
	txn := &shared.Transaction{
		ID:       txID,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}

	allocations := map[uuid.UUID]decimal.Decimal{txID: decimal.NewFromInt(50)}
	currencies := map[uuid.UUID]string{txID: "EUR"}
	useBase := map[uuid.UUID]bool{txID: false}

	allocated, currency, expected, errInfo := allocationFields(txn, allocations, currencies, useBase)
	assert.Nil(t, errInfo)
	assert.True(t, allocated.Equal(decimal.NewFromInt(50)))
	assert.Equal(t, "EUR", currency)
	assert.True(t, expected.Equal(decimal.NewFromInt(100)))
}

func TestAllocationFields_UseBase_MissingBaseAmount(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100092")
	txn := &shared.Transaction{
		ID:         txID,
		Amount:     decimal.NewFromInt(100),
		Currency:   "USD",
		AmountBase: nil,
	}

	allocations := map[uuid.UUID]decimal.Decimal{txID: decimal.NewFromInt(50)}
	useBase := map[uuid.UUID]bool{txID: true}

	_, _, _, errInfo := allocationFields(txn, allocations, map[uuid.UUID]string{}, useBase)
	require.NotNil(t, errInfo)
	assert.Equal(t, enums.ReasonMissingBaseAmount, errInfo.reason)
}

func TestAllocationFields_UseBase_MissingBaseCurrency(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100093")
	base := decimal.NewFromInt(90)
	txn := &shared.Transaction{
		ID:           txID,
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   &base,
		BaseCurrency: nil,
	}

	allocations := map[uuid.UUID]decimal.Decimal{txID: decimal.NewFromInt(50)}
	useBase := map[uuid.UUID]bool{txID: true}

	_, _, _, errInfo := allocationFields(txn, allocations, map[uuid.UUID]string{}, useBase)
	require.NotNil(t, errInfo)
	assert.Equal(t, enums.ReasonMissingBaseCurrency, errInfo.reason)
}

func TestAllocationFields_UseBase_EmptyBaseCurrency(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100094")
	base := decimal.NewFromInt(90)
	emptyStr := "   "
	txn := &shared.Transaction{
		ID:           txID,
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   &base,
		BaseCurrency: &emptyStr,
	}

	allocations := map[uuid.UUID]decimal.Decimal{txID: decimal.NewFromInt(50)}
	useBase := map[uuid.UUID]bool{txID: true}

	_, _, _, errInfo := allocationFields(txn, allocations, map[uuid.UUID]string{}, useBase)
	require.NotNil(t, errInfo)
	assert.Equal(t, enums.ReasonMissingBaseCurrency, errInfo.reason)
}

func TestAllocationFields_UseBase_Success(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100095")
	base := decimal.NewFromInt(90)
	baseCur := "EUR"
	txn := &shared.Transaction{
		ID:           txID,
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   &base,
		BaseCurrency: &baseCur,
	}

	allocations := map[uuid.UUID]decimal.Decimal{txID: decimal.NewFromInt(50)}
	useBase := map[uuid.UUID]bool{txID: true}

	allocated, currency, expected, errInfo := allocationFields(txn, allocations, map[uuid.UUID]string{}, useBase)
	assert.Nil(t, errInfo)
	assert.True(t, allocated.Equal(decimal.NewFromInt(50)))
	assert.Equal(t, "EUR", currency)
	assert.True(t, expected.Equal(decimal.NewFromInt(90)))
}

// --- performFeeVerification tests ---

func TestPerformFeeVerification_NilFeeInput(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, nil)
	require.NoError(t, err)
}

func TestPerformFeeVerification_NilCtxInfo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, &feeVerificationInput{
		ctxInfo: nil,
	})
	require.NoError(t, err)
}

func TestPerformFeeVerification_EmptyFeeRules(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, &feeVerificationInput{
		ctxInfo:    &ports.ReconciliationContextInfo{},
		leftRules:  nil,
		rightRules: nil,
	})
	require.NoError(t, err)
}

func TestPerformFeeVerification_FatalFindingDoesNotPersistPartialResults(t *testing.T) {
	t.Parallel()

	leftTxID := uuid.MustParse("00000000-0000-0000-0000-000000100130")
	rightTxID := uuid.MustParse("00000000-0000-0000-0000-000000100131")
	leftSourceID := uuid.MustParse("00000000-0000-0000-0000-000000100132")
	rightSourceID := uuid.MustParse("00000000-0000-0000-0000-000000100133")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100134")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000100135")
	validScheduleID := uuid.MustParse("00000000-0000-0000-0000-000000100136")
	invalidScheduleID := uuid.MustParse("00000000-0000-0000-0000-000000100137")

	validRule, err := fee.NewFeeRule(
		context.Background(), contextID, validScheduleID,
		fee.MatchingSideLeft, "valid-left-rule", 1, nil,
	)
	require.NoError(t, err)

	invalidRule, err := fee.NewFeeRule(
		context.Background(), contextID, invalidScheduleID,
		fee.MatchingSideRight, "invalid-right-rule", 1, nil,
	)
	require.NoError(t, err)

	validSchedule := &fee.FeeSchedule{
		ID:               validScheduleID,
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		Items: []fee.FeeScheduleItem{{
			ID:        uuid.New(),
			Name:      "flat-valid",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)},
		}},
	}

	invalidSchedule := &fee.FeeSchedule{
		ID:               invalidScheduleID,
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrder("INVALID"),
		Items: []fee.FeeScheduleItem{{
			ID:        uuid.New(),
			Name:      "flat-invalid",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)},
		}},
	}

	groups := []*matchingEntities.MatchGroup{
		{
			ID:     uuid.New(),
			Status: matchingVO.MatchGroupStatusConfirmed,
			Items: []*matchingEntities.MatchItem{
				{TransactionID: leftTxID},
			},
		},
		{
			ID:     uuid.New(),
			Status: matchingVO.MatchGroupStatusConfirmed,
			Items: []*matchingEntities.MatchItem{
				{TransactionID: rightTxID},
			},
		},
	}

	feeInput := &feeVerificationInput{
		ctxInfo: &ports.ReconciliationContextInfo{ID: contextID},
		txByID: map[uuid.UUID]*shared.Transaction{
			leftTxID: {
				ID:       leftTxID,
				SourceID: leftSourceID,
				Amount:   decimal.NewFromInt(1000),
				Currency: "USD",
				Metadata: map[string]any{"fee": map[string]any{"amount": "50.00", "currency": "USD"}},
			},
			rightTxID: {
				ID:       rightTxID,
				SourceID: rightSourceID,
				Amount:   decimal.NewFromInt(1000),
				Currency: "USD",
				Metadata: map[string]any{"fee": map[string]any{"amount": "15.00", "currency": "USD"}},
			},
		},
		sourceTypeByID: map[uuid.UUID]string{
			leftSourceID:  "LEDGER",
			rightSourceID: "BANK",
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{leftSourceID: {}},
		rightSourceIDs: map[uuid.UUID]struct{}{rightSourceID: {}},
		leftRules:      []*fee.FeeRule{validRule},
		rightRules:     []*fee.FeeRule{invalidRule},
		allSchedules: map[uuid.UUID]*fee.FeeSchedule{
			validScheduleID:   validSchedule,
			invalidScheduleID: invalidSchedule,
		},
	}

	feeVarianceRepo := &stubFeeVarianceRepo{}
	exceptionCreator := &stubExceptionCreator{}
	uc := &UseCase{
		feeVarianceRepo:  feeVarianceRepo,
		exceptionCreator: exceptionCreator,
	}

	err = uc.performFeeVerification(
		context.Background(),
		nil,
		&matchingEntities.MatchRun{ID: runID, ContextID: contextID},
		groups,
		feeInput,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "collect fee findings")
	assert.False(t, feeVarianceRepo.called, "fatal findings must prevent fee variance persistence")
	assert.False(t, exceptionCreator.called, "fatal findings must prevent fee exception persistence")
}

// --- processFeeForItem tests ---

func TestProcessFeeForItem_NilItem(t *testing.T) {
	t.Parallel()

	result := processFeeForItem(
		context.Background(), nil, nil, nil, nil,
		&feeVerificationInput{}, &fee.FeeSchedule{}, fee.Tolerance{},
	)
	assert.Nil(t, result)
}

func TestProcessFeeForItem_MissingTransaction(t *testing.T) {
	t.Parallel()

	item := &matchingEntities.MatchItem{
		TransactionID: uuid.MustParse("00000000-0000-0000-0000-000000100099"),
	}
	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{},
	}
	result := processFeeForItem(
		context.Background(), nil, item, nil, nil,
		feeIn, &fee.FeeSchedule{}, fee.Tolerance{},
	)
	assert.Nil(t, result)
}

func TestProcessFeeForItem_NilTransactionInMap(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100100")
	item := &matchingEntities.MatchItem{TransactionID: txID}
	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{txID: nil},
	}
	result := processFeeForItem(
		context.Background(), nil, item, nil, nil,
		feeIn, &fee.FeeSchedule{}, fee.Tolerance{},
	)
	assert.Nil(t, result)
}

func TestProcessFeeForItem_FeeExtractionError(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100101")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100102")
	item := &matchingEntities.MatchItem{TransactionID: txID}
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Metadata: nil, // Will trigger fee extraction error
	}
	feeIn := &feeVerificationInput{
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}
	result := processFeeForItem(
		context.Background(), nil, item, nil, nil,
		feeIn, &fee.FeeSchedule{Currency: "USD"}, fee.Tolerance{},
	)
	require.NotNil(t, result)
	require.NotNil(t, result.exceptionInput)
	assert.Nil(t, result.variance)
}

// TestProcessFeeForItem_NewFeeVarianceFailure exercises the branch where fee
// extraction and verification succeed (producing a non-match variance) but
// NewFeeVariance fails due to an invalid input (uuid.Nil schedule ID).
// The function must return a fatalErr so the caller can abort the run.
func TestProcessFeeForItem_NewFeeVarianceFailure(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100110")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100111")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100112")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000100113")
	groupID := uuid.MustParse("00000000-0000-0000-0000-000000100114")

	item := &matchingEntities.MatchItem{TransactionID: txID}

	// Transaction with valid fee metadata so extractActualFee succeeds.
	// Actual fee = 50 USD, but expected fee from the schedule = 10 USD,
	// which produces a variance well outside zero tolerance.
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(1000),
		Currency: "USD",
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "50.00",
				"currency": "USD",
			},
		},
	}

	group := &matchingEntities.MatchGroup{ID: groupID}
	createdRun := &matchingEntities.MatchRun{
		ID:        runID,
		ContextID: contextID,
	}

	feeIn := &feeVerificationInput{
		ctxInfo: &ports.ReconciliationContextInfo{
			ID: contextID,
		},
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}

	// Schedule with uuid.Nil ID ⟹ NewFeeVariance will fail on "fee schedule id is required".
	schedule := &fee.FeeSchedule{
		ID:               uuid.Nil,
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		Items: []fee.FeeScheduleItem{
			{
				Name:      "flat",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)}, // expected = 10 USD, actual = 50 USD → OVERCHARGE
			},
		},
	}
	tolerance := fee.Tolerance{} // zero tolerance → variance guaranteed

	result := processFeeForItem(
		context.Background(), nil, item, group, createdRun,
		feeIn, schedule, tolerance,
	)

	// NewFeeVariance fails ⟹ fatalErr must be set so the run aborts.
	require.NotNil(t, result, "expected non-nil result with fatalErr")
	require.Error(t, result.fatalErr, "expected fatalErr when NewFeeVariance fails")
	assert.Contains(t, result.fatalErr.Error(), "create fee variance")
	assert.Nil(t, result.variance, "variance must be nil on construction failure")
}

func TestProcessFeeForItem_CalculateExpectedFeeFailure_ReturnsFatalErr(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100115")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100116")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100117")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000100118")
	groupID := uuid.MustParse("00000000-0000-0000-0000-000000100119")

	item := &matchingEntities.MatchItem{TransactionID: txID}
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(1000),
		Currency: "USD",
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "15.00",
				"currency": "USD",
			},
		},
	}

	group := &matchingEntities.MatchGroup{ID: groupID}
	createdRun := &matchingEntities.MatchRun{ID: runID, ContextID: contextID}
	feeIn := &feeVerificationInput{
		ctxInfo:        &ports.ReconciliationContextInfo{ID: contextID},
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}

	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrder("INVALID"),
		Items: []fee.FeeScheduleItem{{
			ID:        uuid.New(),
			Name:      "flat",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)},
		}},
	}

	result := processFeeForItem(context.Background(), nil, item, group, createdRun, feeIn, schedule, fee.Tolerance{})
	require.NotNil(t, result)
	require.Error(t, result.fatalErr)
	assert.Contains(t, result.fatalErr.Error(), "calculate expected fee")
	assert.Nil(t, result.exceptionInput)
	assert.Nil(t, result.variance)
}

func TestProcessFeeForItem_VerifyFeeFailure_ReturnsFatalErr(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100120")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100121")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100122")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000100123")
	groupID := uuid.MustParse("00000000-0000-0000-0000-000000100124")

	item := &matchingEntities.MatchItem{TransactionID: txID}
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(1000),
		Currency: "USD",
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "10.00",
				"currency": "USD",
			},
		},
	}

	group := &matchingEntities.MatchGroup{ID: groupID}
	createdRun := &matchingEntities.MatchRun{ID: runID, ContextID: contextID}
	feeIn := &feeVerificationInput{
		ctxInfo:        &ports.ReconciliationContextInfo{ID: contextID},
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}

	schedule := &fee.FeeSchedule{
		ID:               uuid.New(),
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		Items: []fee.FeeScheduleItem{{
			ID:        uuid.New(),
			Name:      "flat",
			Priority:  1,
			Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)},
		}},
	}

	tolerance := fee.Tolerance{Percent: decimal.RequireFromString("-0.01")}
	result := processFeeForItem(context.Background(), nil, item, group, createdRun, feeIn, schedule, tolerance)
	require.NotNil(t, result)
	require.Error(t, result.fatalErr)
	assert.Contains(t, result.fatalErr.Error(), "verify fee variance")
	assert.Nil(t, result.exceptionInput)
	assert.Nil(t, result.variance)
}

// --- persistFeeFindings tests ---

func TestPersistFeeFindings_EmptyFindings(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		exceptionCreator: &stubExceptionCreator{},
	}
	err := uc.persistFeeFindings(context.Background(), nil, nil, nil, &feeFindings{})
	require.NoError(t, err)
}

// --- collectFeeFindings tests ---

func TestCollectFeeFindings_SkipsNilGroups(t *testing.T) {
	t.Parallel()

	groups := []*matchingEntities.MatchGroup{nil}
	findings, err := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{}, fee.Tolerance{},
	)
	require.NoError(t, err)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
}

func TestCollectFeeFindings_SkipsNonConfirmedGroups(t *testing.T) {
	t.Parallel()

	confidence, _ := matchingVO.ParseConfidenceScore(50)
	groups := []*matchingEntities.MatchGroup{
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusProposed,
			Confidence: confidence,
		},
	}
	findings, err := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{}, fee.Tolerance{},
	)
	require.NoError(t, err)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
}

// --- loadFeeRulesAndSchedules tests ---

// --- recordGroupResults tests ---

func TestRecordGroupResults_AutoConfirm(t *testing.T) {
	t.Parallel()

	confidence, _ := matchingVO.ParseConfidenceScore(100)
	now := time.Now().UTC()
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000100200")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000100201")
	items := []*matchingEntities.MatchItem{
		{TransactionID: txID1},
		{TransactionID: txID2},
	}
	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		Items:       items,
	}

	leftByID := map[uuid.UUID]*shared.Transaction{txID1: {ID: txID1}}
	rightByID := map[uuid.UUID]*shared.Transaction{txID2: {ID: txID2}}

	result := &proposalProcessingResult{
		groups:           make([]*matchingEntities.MatchGroup, 0),
		items:            make([]*matchingEntities.MatchItem, 0),
		autoMatchedIDs:   make([]uuid.UUID, 0),
		pendingReviewIDs: make([]uuid.UUID, 0),
		leftMatched:      make(map[uuid.UUID]struct{}),
		rightMatched:     make(map[uuid.UUID]struct{}),
		leftConfirmed:    make(map[uuid.UUID]struct{}),
		rightConfirmed:   make(map[uuid.UUID]struct{}),
		leftPending:      make(map[uuid.UUID]struct{}),
		rightPending:     make(map[uuid.UUID]struct{}),
		unmatchedReasons: make(map[uuid.UUID]string),
	}

	recordGroupResults(result, group, leftByID, rightByID)

	assert.Len(t, result.groups, 1)
	assert.Len(t, result.items, 2)
	assert.Len(t, result.autoMatchedIDs, 2)
	assert.Empty(t, result.pendingReviewIDs)
	_, leftOk := result.leftConfirmed[txID1]
	assert.True(t, leftOk)
	_, rightOk := result.rightConfirmed[txID2]
	assert.True(t, rightOk)
}

func TestRecordGroupResults_PendingReview(t *testing.T) {
	t.Parallel()

	confidence, _ := matchingVO.ParseConfidenceScore(50)
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000100210")
	items := []*matchingEntities.MatchItem{
		{TransactionID: txID1},
	}
	group := &matchingEntities.MatchGroup{
		ID:         uuid.New(),
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
		Items:      items,
	}

	leftByID := map[uuid.UUID]*shared.Transaction{txID1: {ID: txID1}}

	result := &proposalProcessingResult{
		groups:           make([]*matchingEntities.MatchGroup, 0),
		items:            make([]*matchingEntities.MatchItem, 0),
		autoMatchedIDs:   make([]uuid.UUID, 0),
		pendingReviewIDs: make([]uuid.UUID, 0),
		leftMatched:      make(map[uuid.UUID]struct{}),
		rightMatched:     make(map[uuid.UUID]struct{}),
		leftConfirmed:    make(map[uuid.UUID]struct{}),
		rightConfirmed:   make(map[uuid.UUID]struct{}),
		leftPending:      make(map[uuid.UUID]struct{}),
		rightPending:     make(map[uuid.UUID]struct{}),
		unmatchedReasons: make(map[uuid.UUID]string),
	}

	recordGroupResults(result, group, leftByID, map[uuid.UUID]*shared.Transaction{})

	assert.Len(t, result.pendingReviewIDs, 1)
	assert.Empty(t, result.autoMatchedIDs)
	_, leftPendingOk := result.leftPending[txID1]
	assert.True(t, leftPendingOk)
}

// --- classifySources tests ---

func TestClassifySources_OneToOne_UsesConfiguredSides(t *testing.T) {
	t.Parallel()

	leftID := uuid.MustParse("00000000-0000-0000-0000-000000100300")
	rightID := uuid.MustParse("00000000-0000-0000-0000-000000100301")

	sources := []*ports.SourceInfo{
		{ID: leftID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: rightID, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	left, right, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)

	_, leftOk := left[leftID]
	assert.True(t, leftOk, "configured left source should be in left set")

	_, rightOk := right[rightID]
	assert.True(t, rightOk, "configured right source should be in right set")
}

func TestClassifySources_OneToOne_RejectsMissingSourceSide(t *testing.T) {
	t.Parallel()

	firstID := uuid.MustParse("00000000-0000-0000-0000-000000100302")
	secondID := uuid.MustParse("00000000-0000-0000-0000-000000100303")

	sources := []*ports.SourceInfo{
		{ID: firstID, Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
		{ID: secondID, Type: ports.SourceTypeFile},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrSourceSideRequiredForMatching)
}

func TestClassifySources_OneToOne_RejectsInvalidTopology(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000100305"), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000100306"), Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrOneToOneRequiresExactlyOneLeftSource)
}

func TestClassifySources_OnlyOneSource(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
	}

	_, _, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.ErrorIs(t, err, ErrAtLeastTwoSourcesRequired)
}

func TestClassifySources_NilSourcesSkipped(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000100310")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000100311")

	sources := []*ports.SourceInfo{
		nil,
		{ID: id1, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		nil,
		{ID: id2, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
	}

	left, right, err := classifySources(shared.ContextTypeOneToOne, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)
}

func TestClassifySources_OneToMany_RequiresOneLeftAndManyRight(t *testing.T) {
	t.Parallel()

	primaryID := uuid.MustParse("00000000-0000-0000-0000-000000100312")
	otherID1 := uuid.MustParse("00000000-0000-0000-0000-000000100313")
	otherID2 := uuid.MustParse("00000000-0000-0000-0000-000000100314")

	sources := []*ports.SourceInfo{
		{ID: otherID1, Type: ports.SourceTypeFile, Side: fee.MatchingSideRight},
		{ID: primaryID, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: otherID2, Type: ports.SourceTypeAPI, Side: fee.MatchingSideRight},
	}

	left, right, err := classifySources(shared.ContextTypeOneToMany, sources)
	require.NoError(t, err)
	assert.Len(t, left, 1, "only one configured LEFT source")
	assert.Len(t, right, 2, "configured RIGHT sources remain on the right")

	_, leftOk := left[primaryID]
	assert.True(t, leftOk)
	_, right1Ok := right[otherID1]
	assert.True(t, right1Ok)
	_, right2Ok := right[otherID2]
	assert.True(t, right2Ok)
}

func TestClassifySources_OneToMany_RejectsMultipleLeftSources(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000100315")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000100316")
	id3 := uuid.MustParse("00000000-0000-0000-0000-000000100317")

	sources := []*ports.SourceInfo{
		{ID: id1, Type: ports.SourceTypeLedger, Side: fee.MatchingSideLeft},
		{ID: id2, Type: ports.SourceTypeFile, Side: fee.MatchingSideLeft},
		{ID: id3, Type: ports.SourceTypeAPI, Side: fee.MatchingSideRight},
	}

	_, _, err := classifySources(shared.ContextTypeOneToMany, sources)
	require.ErrorIs(t, err, ErrOneToManyRequiresExactlyOneLeftSource)
}

func TestClassifySources_EmptySlice(t *testing.T) {
	t.Parallel()

	_, _, err := classifySources(shared.ContextTypeOneToOne, []*ports.SourceInfo{})
	require.ErrorIs(t, err, ErrAtLeastTwoSourcesRequired)
}

// --- validateAndEnrichTenant tests ---

func TestValidateAndEnrichTenant_TenantIDMismatch(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	ctxTenantID := uuid.MustParse("00000000-0000-0000-0000-000000100400")
	inputTenantID := uuid.MustParse("00000000-0000-0000-0000-000000100401")

	ctx := context.WithValue(context.Background(), "tenant_id", ctxTenantID.String())

	_, err := uc.validateAndEnrichTenant(ctx, &RunMatchInput{
		TenantID: inputTenantID,
	})
	// Without tenant in context, it will try DefaultTenantID
	// The specific error depends on auth config, so just verify it returns an error
	require.Error(t, err)
}

// --- buildAdjustmentAuditChanges tests ---

func TestBuildAdjustmentAuditChanges_WithMatchGroupID(t *testing.T) {
	t.Parallel()

	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000100500")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100502")
	adjustmentID := uuid.MustParse("00000000-0000-0000-0000-000000100501")
	input := CreateAdjustmentInput{
		TenantID:     uuid.MustParse("00000000-0000-0000-0000-000000100503"),
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeBankFee),
		Amount:       decimal.NewFromInt(10),
		Currency:     "USD",
		Description:  "test",
		Reason:       "reason",
		CreatedBy:    "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustmentID, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.Contains(t, string(changes), "entity_id")
	assert.Contains(t, string(changes), "match_group_id")
}

func TestBuildAdjustmentAuditChanges_WithTransactionID(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100510")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100512")
	adjustmentID := uuid.MustParse("00000000-0000-0000-0000-000000100511")
	input := CreateAdjustmentInput{
		TenantID:      uuid.MustParse("00000000-0000-0000-0000-000000100513"),
		ContextID:     contextID,
		TransactionID: &txID,
		Type:          string(matchingEntities.AdjustmentTypeBankFee),
		Amount:        decimal.NewFromInt(10),
		Currency:      "USD",
		Description:   "test",
		Reason:        "reason",
		CreatedBy:     "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustmentID, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.Contains(t, string(changes), "entity_id")
	assert.Contains(t, string(changes), "transaction_id")
	assert.NotContains(t, string(changes), "match_group_id")
}

func TestBuildAdjustmentAuditChanges_WithoutOptionalTargets(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100521")
	adjustmentID := uuid.MustParse("00000000-0000-0000-0000-000000100520")
	input := CreateAdjustmentInput{
		TenantID:    uuid.MustParse("00000000-0000-0000-0000-000000100522"),
		ContextID:   contextID,
		Type:        string(matchingEntities.AdjustmentTypeBankFee),
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		Description: "test",
		Reason:      "reason",
		CreatedBy:   "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustmentID, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.Contains(t, string(changes), "entity_id")
	assert.NotContains(t, string(changes), "match_group_id")
	assert.NotContains(t, string(changes), "transaction_id")
}

// --- persistAdjustmentWithAudit - repository error ---

func TestPersistAdjustmentWithAudit_RepositoryError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		adjustmentRepo: &stubAdjustmentRepoWithError{err: errAdjTestDatabase},
	}

	adjustment := &matchingEntities.Adjustment{
		ID: uuid.New(),
	}

	_, err := uc.persistAdjustmentWithAudit(context.Background(), adjustment, CreateAdjustmentInput{
		TenantID:  uuid.New(),
		ContextID: uuid.New(),
		CreatedBy: "user@test.com",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist adjustment transaction")
}

// --- completeDryRun tests ---

func TestCompleteDryRun_RefreshFailed(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0},
		nil, refreshFailed,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

func TestCompleteDryRun_UpdateError(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{updateErr: errors.New("update failed")}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0},
		nil, &atomic.Bool{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update match run")
}

func TestCompleteDryRun_NilUpdatedRun(t *testing.T) {
	t.Parallel()

	// Return nil from UpdateWithTx
	matchRunRepo := &stubMatchRunRepoReturnsNil{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0},
		nil, &atomic.Bool{},
	)
	require.ErrorIs(t, err, ErrMatchRunPersistedNil)
}

func TestCompleteDryRun_Success(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	groups := []*matchingEntities.MatchGroup{
		{ID: uuid.New(), Confidence: confidence},
	}

	updatedRun, resultGroups, err := uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 1},
		groups, &atomic.Bool{},
	)
	require.NoError(t, err)
	require.NotNil(t, updatedRun)
	assert.Len(t, resultGroups, 1)
}

// stubMatchRunRepoReturnsNil returns nil from Update to test the nil check.
type stubMatchRunRepoReturnsNil struct {
	stubMatchRunRepo
}

func (s *stubMatchRunRepoReturnsNil) Update(
	_ context.Context,
	_ *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	return nil, nil
}

// --- commitMatchResults tests ---

func TestCommitMatchResults_RefreshFailed(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	_, err = uc.commitMatchResults(
		context.Background(), nil, run,
		nil, nil, nil, nil, nil, nil,
		refreshFailed,
		map[string]int{},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

// --- enqueueMatchConfirmedEvents tests ---

func TestEnqueueMatchConfirmedEvents_SkipsNonConfirmedGroups(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)
	// No CreateWithTx expectations -- means it should not be called.

	uc := &UseCase{outboxRepoTx: outboxRepo}

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000100700")
	confidence, _ := matchingVO.ParseConfidenceScore(50)
	groups := []*matchingEntities.MatchGroup{
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusProposed,
			Confidence: confidence,
		},
	}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	err := uc.enqueueMatchConfirmedEvents(ctx, new(sql.Tx), groups)
	require.NoError(t, err)
}

type stubFeeRuleProviderWithResult struct {
	rules []*fee.FeeRule
	err   error
}

func (s *stubFeeRuleProviderWithResult) FindByContextID(
	_ context.Context,
	_ uuid.UUID,
) ([]*fee.FeeRule, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.rules, nil
}

type stubFeeScheduleRepoWithResult struct {
	mockFeeScheduleRepo
	schedules     map[uuid.UUID]*fee.FeeSchedule
	scheduleCount int
	err           error
}

func (s *stubFeeScheduleRepoWithResult) GetByIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID]*fee.FeeSchedule, error) {
	if s.err != nil {
		return nil, s.err
	}

	if s.scheduleCount == 0 && s.schedules == nil {
		return map[uuid.UUID]*fee.FeeSchedule{}, nil
	}

	return s.schedules, nil
}

// --- ensureLockFresh additional tests ---

func TestEnsureLockFresh_NilLockPassed(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.ensureLockFresh(context.Background(), nil, nil, &atomic.Bool{})
	require.NoError(t, err)
}

func TestEnsureLockFresh_RefreshPreviouslyFailed(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	err := uc.ensureLockFresh(context.Background(), nil, nil, refreshFailed)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

// --- releaseMatchLock tests ---

func TestReleaseMatchLock_NilLock(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	// Should not panic
	uc.releaseMatchLock(context.Background(), nil, nil, nil)
}

func TestReleaseMatchLock_ReleaseError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	lock := &stubLockWithReleaseError{err: errors.New("release failed")}
	// Should not panic, just log
	uc.releaseMatchLock(context.Background(), nil, lock, &libLog.NopLogger{})
}

type stubLockWithReleaseError struct {
	err error
}

func (s *stubLockWithReleaseError) Release(_ context.Context) error {
	return s.err
}

// --- CreateAdjustment - InvalidDirection ---

func TestCreateAdjustment_InvalidDirection(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000100600")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100601")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000100602")

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:        matchGroupID,
				ContextID: contextID,
			},
		},
		txRepo:         &stubTxRepoForAdjustment{},
		adjustmentRepo: &stubAdjustmentRepo{},
	}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeBankFee),
		Direction:    "INVALID_DIRECTION",
		Amount:       decimal.NewFromInt(10),
		Currency:     "USD",
		Description:  "Test",
		Reason:       "Test",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrAdjustmentDirectionInvalid)
	require.Nil(t, result)
}

// --- CreateAdjustment - combined persist error ---

func TestCreateAdjustment_CombinedPersistError(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000100610")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000100611")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000100612")

	infraProv := &stubInfraProvider{}
	t.Cleanup(infraProv.Close)

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		matchGroupRepo: &stubMatchGroupRepoForAdjustment{
			findByIDResult: &matchingEntities.MatchGroup{
				ID:        matchGroupID,
				ContextID: contextID,
			},
		},
		txRepo:         &stubTxRepoForAdjustment{},
		adjustmentRepo: &stubAdjustmentRepoWithError{err: errAdjTestDatabase},
		infraProvider:  infraProv,
	}

	input := CreateAdjustmentInput{
		TenantID:     tenantID,
		ContextID:    contextID,
		MatchGroupID: &matchGroupID,
		Type:         string(matchingEntities.AdjustmentTypeBankFee),
		Direction:    string(matchingEntities.AdjustmentDirectionDebit),
		Amount:       decimal.NewFromInt(10),
		Currency:     "USD",
		Description:  "Test",
		Reason:       "Test",
		CreatedBy:    "user@test.com",
	}

	result, err := uc.CreateAdjustment(context.Background(), input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist adjustment transaction")
	require.Nil(t, result)
}
