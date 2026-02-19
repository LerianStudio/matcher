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

	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	"github.com/LerianStudio/matcher/internal/matching/domain/enums"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	outboxmocks "github.com/LerianStudio/matcher/internal/outbox/domain/repositories/mocks"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
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
		{ID: id, Type: ports.SourceTypeLedger},
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

func TestPerformFeeVerification_NilRateID(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	err := uc.performFeeVerification(context.Background(), nil, nil, nil, &feeVerificationInput{
		ctxInfo: &ports.ReconciliationContextInfo{
			RateID: nil,
		},
	})
	require.NoError(t, err)
}

// --- processFeeForItem tests ---

func TestProcessFeeForItem_NilItem(t *testing.T) {
	t.Parallel()

	result := processFeeForItem(
		context.Background(), nil, nil, nil, nil,
		&feeVerificationInput{}, &fee.Rate{}, fee.Tolerance{},
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
		feeIn, &fee.Rate{}, fee.Tolerance{},
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
		feeIn, &fee.Rate{}, fee.Tolerance{},
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
		feeIn, &fee.Rate{Currency: "USD"}, fee.Tolerance{},
	)
	require.NotNil(t, result)
	require.NotNil(t, result.exceptionInput)
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
	findings := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{}, &fee.Rate{}, fee.Tolerance{},
	)
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
	findings := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{}, &fee.Rate{}, fee.Tolerance{},
	)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
}

// --- loadFeeSchedules tests ---

func TestLoadFeeSchedules_NoScheduleIDs(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger},
	}
	leftIDs := map[uuid.UUID]struct{}{sources[0].ID: {}}

	left, right := uc.loadFeeSchedules(context.Background(), sources, leftIDs, map[uuid.UUID]struct{}{})
	assert.Nil(t, left)
	assert.Nil(t, right)
}

func TestLoadFeeSchedules_NilFeeScheduleRepo(t *testing.T) {
	t.Parallel()

	uc := &UseCase{feeScheduleRepo: nil}
	scheduleID := uuid.New()
	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger, FeeScheduleID: &scheduleID},
	}
	leftIDs := map[uuid.UUID]struct{}{sources[0].ID: {}}

	left, right := uc.loadFeeSchedules(context.Background(), sources, leftIDs, map[uuid.UUID]struct{}{})
	assert.Nil(t, left)
	assert.Nil(t, right)
}

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

func TestClassifySources_DirectedMode_PrimarySourceFound(t *testing.T) {
	t.Parallel()

	primaryID := uuid.MustParse("00000000-0000-0000-0000-000000100300")
	otherID := uuid.MustParse("00000000-0000-0000-0000-000000100301")

	sources := []*ports.SourceInfo{
		{ID: primaryID, Type: ports.SourceTypeLedger},
		{ID: otherID, Type: ports.SourceTypeFile},
	}

	left, right, err := classifySources(sources, &primaryID)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)

	_, leftOk := left[primaryID]
	assert.True(t, leftOk, "primary source should be in left set")

	_, rightOk := right[otherID]
	assert.True(t, rightOk, "other source should be in right set")
}

func TestClassifySources_SymmetricMode_NilPrimarySourceID(t *testing.T) {
	t.Parallel()

	firstID := uuid.MustParse("00000000-0000-0000-0000-000000100302")
	secondID := uuid.MustParse("00000000-0000-0000-0000-000000100303")

	sources := []*ports.SourceInfo{
		{ID: firstID, Type: ports.SourceTypeFile},
		{ID: secondID, Type: ports.SourceTypeFile},
	}

	left, right, err := classifySources(sources, nil)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)

	_, leftOk := left[firstID]
	assert.True(t, leftOk, "first source should be in left set")

	_, rightOk := right[secondID]
	assert.True(t, rightOk, "second source should be in right set")
}

func TestClassifySources_PrimarySourceNotInContext(t *testing.T) {
	t.Parallel()

	missingID := uuid.MustParse("00000000-0000-0000-0000-000000100304")
	sources := []*ports.SourceInfo{
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000100305"), Type: ports.SourceTypeLedger},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000100306"), Type: ports.SourceTypeFile},
	}

	_, _, err := classifySources(sources, &missingID)
	require.ErrorIs(t, err, ErrPrimarySourceNotInContext)
}

func TestClassifySources_OnlyOneSource(t *testing.T) {
	t.Parallel()

	sources := []*ports.SourceInfo{
		{ID: uuid.New(), Type: ports.SourceTypeLedger},
	}

	_, _, err := classifySources(sources, nil)
	require.ErrorIs(t, err, ErrAtLeastTwoSourcesRequired)
}

func TestClassifySources_NilSourcesSkipped(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000100310")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000100311")

	sources := []*ports.SourceInfo{
		nil,
		{ID: id1, Type: ports.SourceTypeLedger},
		nil,
		{ID: id2, Type: ports.SourceTypeFile},
	}

	left, right, err := classifySources(sources, nil)
	require.NoError(t, err)
	assert.Len(t, left, 1)
	assert.Len(t, right, 1)
}

func TestClassifySources_ThreePlusSources_WithPrimarySourceID(t *testing.T) {
	t.Parallel()

	primaryID := uuid.MustParse("00000000-0000-0000-0000-000000100312")
	otherID1 := uuid.MustParse("00000000-0000-0000-0000-000000100313")
	otherID2 := uuid.MustParse("00000000-0000-0000-0000-000000100314")

	sources := []*ports.SourceInfo{
		{ID: otherID1, Type: ports.SourceTypeFile},
		{ID: primaryID, Type: ports.SourceTypeLedger},
		{ID: otherID2, Type: ports.SourceTypeAPI},
	}

	left, right, err := classifySources(sources, &primaryID)
	require.NoError(t, err)
	assert.Len(t, left, 1, "only the primary source in left")
	assert.Len(t, right, 2, "remaining sources in right")

	_, leftOk := left[primaryID]
	assert.True(t, leftOk)
	_, right1Ok := right[otherID1]
	assert.True(t, right1Ok)
	_, right2Ok := right[otherID2]
	assert.True(t, right2Ok)
}

func TestClassifySources_ThreePlusSources_WithoutPrimarySourceID(t *testing.T) {
	t.Parallel()

	id1 := uuid.MustParse("00000000-0000-0000-0000-000000100315")
	id2 := uuid.MustParse("00000000-0000-0000-0000-000000100316")
	id3 := uuid.MustParse("00000000-0000-0000-0000-000000100317")

	sources := []*ports.SourceInfo{
		{ID: id1, Type: ports.SourceTypeLedger},
		{ID: id2, Type: ports.SourceTypeFile},
		{ID: id3, Type: ports.SourceTypeAPI},
	}

	left, right, err := classifySources(sources, nil)
	require.NoError(t, err)
	assert.Len(t, left, 1, "first source in left")
	assert.Len(t, right, 2, "remaining sources in right")

	_, leftOk := left[id1]
	assert.True(t, leftOk, "first source should be in left")
	_, right1Ok := right[id2]
	assert.True(t, right1Ok)
	_, right2Ok := right[id3]
	assert.True(t, right2Ok)
}

func TestClassifySources_EmptySlice(t *testing.T) {
	t.Parallel()

	_, _, err := classifySources([]*ports.SourceInfo{}, nil)
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
	adjustment := &matchingEntities.Adjustment{
		ID:          uuid.MustParse("00000000-0000-0000-0000-000000100501"),
		ContextID:   uuid.MustParse("00000000-0000-0000-0000-000000100502"),
		Type:        matchingEntities.AdjustmentTypeBankFee,
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		Description: "test",
		Reason:      "reason",
	}
	input := CreateAdjustmentInput{
		TenantID:     uuid.MustParse("00000000-0000-0000-0000-000000100503"),
		ContextID:    adjustment.ContextID,
		MatchGroupID: &matchGroupID,
		CreatedBy:    "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustment, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.Contains(t, string(changes), "match_group_id")
}

func TestBuildAdjustmentAuditChanges_WithTransactionID(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000100510")
	adjustment := &matchingEntities.Adjustment{
		ID:          uuid.MustParse("00000000-0000-0000-0000-000000100511"),
		ContextID:   uuid.MustParse("00000000-0000-0000-0000-000000100512"),
		Type:        matchingEntities.AdjustmentTypeBankFee,
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		Description: "test",
		Reason:      "reason",
	}
	input := CreateAdjustmentInput{
		TenantID:      uuid.MustParse("00000000-0000-0000-0000-000000100513"),
		ContextID:     adjustment.ContextID,
		TransactionID: &txID,
		CreatedBy:     "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustment, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.Contains(t, string(changes), "transaction_id")
	assert.NotContains(t, string(changes), "match_group_id")
}

func TestBuildAdjustmentAuditChanges_WithoutOptionalTargets(t *testing.T) {
	t.Parallel()

	adjustment := &matchingEntities.Adjustment{
		ID:          uuid.MustParse("00000000-0000-0000-0000-000000100520"),
		ContextID:   uuid.MustParse("00000000-0000-0000-0000-000000100521"),
		Type:        matchingEntities.AdjustmentTypeBankFee,
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		Description: "test",
		Reason:      "reason",
	}
	input := CreateAdjustmentInput{
		TenantID:  uuid.MustParse("00000000-0000-0000-0000-000000100522"),
		ContextID: adjustment.ContextID,
		CreatedBy: "user@test.com",
	}

	changes, err := buildAdjustmentAuditChanges(adjustment, input)
	require.NoError(t, err)
	require.NotNil(t, changes)
	assert.NotContains(t, string(changes), "match_group_id")
	assert.NotContains(t, string(changes), "transaction_id")
}

// --- persistAdjustmentWithAudit - connection error ---

func TestPersistAdjustmentWithAudit_ConnectionError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		infraProvider: &stubInfraProvider{connErr: errAdjTestDatabase},
	}

	adjustment := &matchingEntities.Adjustment{
		ID: uuid.New(),
	}

	_, err := uc.persistAdjustmentWithAudit(context.Background(), adjustment, CreateAdjustmentInput{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get postgres connection")
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

// --- loadFeeSchedules with actual schedules ---

func TestLoadFeeSchedules_WithSchedulesLoaded(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000100800")
	sourceIDLeft := uuid.MustParse("00000000-0000-0000-0000-000000100801")
	sourceIDRight := uuid.MustParse("00000000-0000-0000-0000-000000100802")

	schedule := &fee.FeeSchedule{
		ID:       scheduleID,
		Currency: "USD",
	}

	uc := &UseCase{
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			schedules: map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
		},
	}

	sources := []*ports.SourceInfo{
		{ID: sourceIDLeft, Type: ports.SourceTypeLedger, FeeScheduleID: &scheduleID},
		{ID: sourceIDRight, Type: ports.SourceTypeFile, FeeScheduleID: &scheduleID},
	}

	leftIDs := map[uuid.UUID]struct{}{sourceIDLeft: {}}
	rightIDs := map[uuid.UUID]struct{}{sourceIDRight: {}}

	left, right := uc.loadFeeSchedules(context.Background(), sources, leftIDs, rightIDs)
	require.NotNil(t, left)
	require.NotNil(t, right)
	assert.Equal(t, schedule, left[sourceIDLeft])
	assert.Equal(t, schedule, right[sourceIDRight])
}

func TestLoadFeeSchedules_GetByIDsError(t *testing.T) {
	t.Parallel()

	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000100810")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000100811")

	uc := &UseCase{
		feeScheduleRepo: &stubFeeScheduleRepoWithResult{
			err: errors.New("db error"),
		},
	}

	sources := []*ports.SourceInfo{
		{ID: sourceID, Type: ports.SourceTypeLedger, FeeScheduleID: &scheduleID},
	}
	leftIDs := map[uuid.UUID]struct{}{sourceID: {}}

	left, right := uc.loadFeeSchedules(context.Background(), sources, leftIDs, map[uuid.UUID]struct{}{})
	assert.Nil(t, left)
	assert.Nil(t, right)
}

type stubFeeScheduleRepoWithResult struct {
	mockFeeScheduleRepo
	schedules map[uuid.UUID]*fee.FeeSchedule
	err       error
}

func (s *stubFeeScheduleRepoWithResult) GetByIDs(
	_ context.Context,
	_ []uuid.UUID,
) (map[uuid.UUID]*fee.FeeSchedule, error) {
	if s.err != nil {
		return nil, s.err
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

// --- CreateAdjustment - AuditLog error ---

func TestCreateAdjustment_AuditLogError(t *testing.T) {
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
		adjustmentRepo: &stubAdjustmentRepo{},
		infraProvider:  infraProv,
		auditLogRepo:   &stubAuditLogRepo{createErr: errAdjTestDatabase},
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
	assert.Contains(t, err.Error(), "persist audit log")
	require.Nil(t, result)
}
