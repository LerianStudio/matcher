//go:build unit

package command

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matching "github.com/LerianStudio/matcher/internal/matching/domain/services"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

func TestAllocationMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		allocations []matching.Allocation
		wantLen     int
	}{
		{
			name:        "empty allocations",
			allocations: nil,
			wantLen:     0,
		},
		{
			name: "single allocation",
			allocations: []matching.Allocation{
				{TransactionID: uuid.MustParse("00000000-0000-0000-0000-000000200001"), AllocatedAmount: decimal.NewFromInt(50)},
			},
			wantLen: 1,
		},
		{
			name: "multiple allocations preserves all",
			allocations: []matching.Allocation{
				{TransactionID: uuid.MustParse("00000000-0000-0000-0000-000000200002"), AllocatedAmount: decimal.NewFromInt(50)},
				{TransactionID: uuid.MustParse("00000000-0000-0000-0000-000000200003"), AllocatedAmount: decimal.NewFromInt(75)},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := allocationMap(tt.allocations)
			assert.Len(t, result, tt.wantLen)
		})
	}
}

func TestAllocationCurrencyMap(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200010")

	tests := []struct {
		name        string
		allocations []matching.Allocation
		wantLen     int
		checkID     uuid.UUID
		wantCur     string
	}{
		{
			name:    "empty returns empty map",
			wantLen: 0,
		},
		{
			name: "maps currency per transaction",
			allocations: []matching.Allocation{
				{TransactionID: txID, Currency: "EUR"},
			},
			wantLen: 1,
			checkID: txID,
			wantCur: "EUR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := allocationCurrencyMap(tt.allocations)
			assert.Len(t, result, tt.wantLen)
			if tt.checkID != uuid.Nil {
				assert.Equal(t, tt.wantCur, result[tt.checkID])
			}
		})
	}
}

func TestAllocationUseBaseMap(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200020")

	tests := []struct {
		name        string
		allocations []matching.Allocation
		wantLen     int
		checkID     uuid.UUID
		wantBase    bool
	}{
		{
			name:    "empty returns empty map",
			wantLen: 0,
		},
		{
			name: "use base true",
			allocations: []matching.Allocation{
				{TransactionID: txID, UseBaseAmount: true},
			},
			wantLen:  1,
			checkID:  txID,
			wantBase: true,
		},
		{
			name: "use base false",
			allocations: []matching.Allocation{
				{TransactionID: txID, UseBaseAmount: false},
			},
			wantLen:  1,
			checkID:  txID,
			wantBase: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := allocationUseBaseMap(tt.allocations)
			assert.Len(t, result, tt.wantLen)
			if tt.checkID != uuid.Nil {
				assert.Equal(t, tt.wantBase, result[tt.checkID])
			}
		})
	}
}

func TestBuildExceptionInputs_MultipleDistinctIDs(t *testing.T) {
	t.Parallel()

	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000200030")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000200031")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200032")

	txn1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}
	txn2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(200),
		Currency: "EUR",
	}
	txByID := map[uuid.UUID]*shared.Transaction{txID1: txn1, txID2: txn2}
	sourceTypeByID := map[uuid.UUID]string{sourceID: "ledger"}
	reasons := map[uuid.UUID]string{txID1: "reason1", txID2: "reason2"}

	result := buildExceptionInputs([]uuid.UUID{txID1, txID2}, txByID, sourceTypeByID, reasons)
	require.Len(t, result, 2)
	assert.Equal(t, "reason1", result[0].Reason)
	assert.Equal(t, "reason2", result[1].Reason)
}

func TestBuildExceptionInputs_TxNotInMap(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200040")
	result := buildExceptionInputs(
		[]uuid.UUID{txID},
		map[uuid.UUID]*shared.Transaction{},
		nil,
		nil,
	)
	require.Len(t, result, 1)
	assert.Equal(t, txID, result[0].TransactionID)
	assert.Empty(t, result[0].Reason)
}

func TestBuildExceptionInputFromTx_WithSourceType(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200050")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200051")
	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}
	sourceTypeByID := map[uuid.UUID]string{sourceID: "file"}

	result := buildExceptionInputFromTx(txn, sourceTypeByID, "test-reason")
	require.NotNil(t, result)
	assert.Equal(t, "file", result.SourceType)
	assert.Equal(t, "test-reason", result.Reason)
	assert.True(t, result.AmountAbsBase.Equal(decimal.NewFromInt(100)))
}

func TestBuildExceptionInputFromTx_NilSourceTypeByID(t *testing.T) {
	t.Parallel()

	txn := &shared.Transaction{
		ID:       uuid.MustParse("00000000-0000-0000-0000-000000200060"),
		SourceID: uuid.MustParse("00000000-0000-0000-0000-000000200061"),
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
	}

	result := buildExceptionInputFromTx(txn, nil, "")
	require.NotNil(t, result)
	assert.Empty(t, result.SourceType)
}

func TestBuildExceptionInputFromTx_FXNotMissing(t *testing.T) {
	t.Parallel()

	base := decimal.NewFromInt(90)
	baseCur := "EUR"
	txn := &shared.Transaction{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000200070"),
		SourceID:     uuid.MustParse("00000000-0000-0000-0000-000000200071"),
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   &base,
		BaseCurrency: &baseCur,
	}

	result := buildExceptionInputFromTx(txn, nil, "")
	require.NotNil(t, result)
	assert.False(t, result.FXMissing)
}

func TestRecordGroupResults_TxNotInEitherSide(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200080")
	confidence, _ := matchingVO.ParseConfidenceScore(100)
	now := time.Now().UTC()
	group := &matchingEntities.MatchGroup{
		ID:          uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		Items:       []*matchingEntities.MatchItem{{TransactionID: txID}},
	}

	leftByID := map[uuid.UUID]*shared.Transaction{}
	rightByID := map[uuid.UUID]*shared.Transaction{}

	result := newEmptyProposalProcessingResult()
	recordGroupResults(result, group, leftByID, rightByID)

	assert.Len(t, result.groups, 1)
	assert.Len(t, result.autoMatchedIDs, 1)
	assert.Empty(t, result.leftConfirmed)
	assert.Empty(t, result.rightConfirmed)
}

func TestRecordGroupResults_RightPending(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200090")
	confidence, _ := matchingVO.ParseConfidenceScore(50)
	group := &matchingEntities.MatchGroup{
		ID:         uuid.New(),
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
		Items:      []*matchingEntities.MatchItem{{TransactionID: txID}},
	}

	rightByID := map[uuid.UUID]*shared.Transaction{txID: {ID: txID}}

	result := newEmptyProposalProcessingResult()
	recordGroupResults(result, group, map[uuid.UUID]*shared.Transaction{}, rightByID)

	assert.Len(t, result.pendingReviewIDs, 1)
	_, ok := result.rightPending[txID]
	assert.True(t, ok)
}

func newEmptyProposalProcessingResult() *proposalProcessingResult {
	return &proposalProcessingResult{
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
}
