// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
	"github.com/LerianStudio/matcher/internal/shared/domain/fee"
)

func TestCommitMatchResults_RefreshFailed_Returns_Error(t *testing.T) {
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

func TestCompleteDryRun_RefreshFailed_Returns_LockError(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	refreshFailed := &atomic.Bool{}
	refreshFailed.Store(true)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0}, nil, refreshFailed,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLockRefreshFailed)
}

func TestCompleteDryRun_UpdateError_Returns_Wrapped(t *testing.T) {
	t.Parallel()

	matchRunRepo := &stubMatchRunRepo{updateErr: errors.New("update failed")}
	uc := &UseCase{matchRunRepo: matchRunRepo}

	run, err := matchingEntities.NewMatchRun(context.Background(), uuid.New(), matchingVO.MatchRunModeDryRun)
	require.NoError(t, err)

	_, _, err = uc.completeDryRun(
		context.Background(), nil, run,
		map[string]int{"matches": 0}, nil, &atomic.Bool{},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update match run")
}

func TestCompleteDryRun_Success_ReturnsGroups(t *testing.T) {
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
		map[string]int{"matches": 1}, groups, &atomic.Bool{},
	)
	require.NoError(t, err)
	require.NotNil(t, updatedRun)
	assert.Len(t, resultGroups, 1)
}

func TestPersistFeeFindings_EmptyFindings_NoOp(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		exceptionCreator: &stubExceptionCreator{},
	}
	err := uc.persistFeeFindings(context.Background(), nil, nil, nil, &feeFindings{})
	require.NoError(t, err)
}

func TestPersistFeeFindings_VariancePersistError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo: &stubFeeVarianceRepo{err: errors.New("persist failed")},
	}
	run := &matchingEntities.MatchRun{ContextID: uuid.New()}
	findings := &feeFindings{
		variances: []*matchingEntities.FeeVariance{{}},
	}

	err := uc.persistFeeFindings(context.Background(), nil, nil, run, findings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist fee variances")
}

func TestPersistFeeFindings_NilFeeVarianceRepo_ReturnsError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	run := &matchingEntities.MatchRun{ContextID: uuid.New()}
	findings := &feeFindings{
		variances: []*matchingEntities.FeeVariance{{}},
	}

	err := uc.persistFeeFindings(context.Background(), nil, nil, run, findings)
	require.ErrorIs(t, err, ErrNilFeeVarianceRepository)
}

func TestPersistFeeFindings_ExceptionPersistError(t *testing.T) {
	t.Parallel()

	uc := &UseCase{
		feeVarianceRepo:  &stubFeeVarianceRepo{},
		exceptionCreator: &stubExceptionCreator{err: errors.New("exception failed")},
	}
	run := &matchingEntities.MatchRun{ID: uuid.New(), ContextID: uuid.New()}
	findings := &feeFindings{
		exceptionInputs: []ports.ExceptionTransactionInput{{TransactionID: uuid.New()}},
	}

	err := uc.persistFeeFindings(context.Background(), nil, nil, run, findings)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create fee exceptions")
}

func TestParseAmount_AllSupportedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   any
		wantErr bool
	}{
		{"string valid", "123.45", false},
		{"string invalid", "xyz", true},
		{"float64", float64(42.5), false},
		{"int", int(100), false},
		{"int64", int64(9999), false},
		{"bool unsupported", true, true},
		{"nil unsupported", nil, true},
		{"slice unsupported", []int{1}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, feeErr := parseAmount(tt.input)
			if tt.wantErr {
				require.NotNil(t, feeErr)
			} else {
				require.Nil(t, feeErr)
			}
		})
	}
}

// --- resolveScheduleForTransaction tests ---

func TestResolveScheduleForTransaction_NotFound(t *testing.T) {
	t.Parallel()

	missingID := uuid.MustParse("00000000-0000-0000-0000-000000200001")
	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{},
	}

	result := resolveScheduleForTransaction(missingID, feeIn)
	assert.Nil(t, result)
}

func TestResolveScheduleForTransaction_NilInput(t *testing.T) {
	t.Parallel()

	result := resolveScheduleForTransaction(uuid.New(), nil)
	assert.Nil(t, result)
}

func TestResolveScheduleForTransaction_NilTransaction(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200002")
	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{txID: nil},
	}

	result := resolveScheduleForTransaction(txID, feeIn)
	assert.Nil(t, result)
}

func TestResolveScheduleForTransaction_LeftSide(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200003")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200004")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200005")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200006")

	schedule := &fee.FeeSchedule{ID: scheduleID, Name: "left-schedule", Currency: "USD"}

	leftRule, err := fee.NewFeeRule(
		context.Background(), contextID, scheduleID,
		fee.MatchingSideLeft, "left-rule", 1, nil,
	)
	require.NoError(t, err)

	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{
			txID: {ID: txID, SourceID: sourceID, Metadata: map[string]any{}},
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{sourceID: {}},
		rightSourceIDs: map[uuid.UUID]struct{}{},
		leftRules:      []*fee.FeeRule{leftRule},
		rightRules:     nil,
		allSchedules:   map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
	}

	result := resolveScheduleForTransaction(txID, feeIn)
	require.NotNil(t, result)
	assert.Equal(t, scheduleID, result.ID)
}

func TestResolveScheduleForTransaction_RightSide(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200010")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200011")
	leftScheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200012")
	rightScheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200013")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200014")

	leftSchedule := &fee.FeeSchedule{ID: leftScheduleID, Name: "left-schedule", Currency: "USD"}
	rightSchedule := &fee.FeeSchedule{ID: rightScheduleID, Name: "right-schedule", Currency: "USD"}

	leftRule, err := fee.NewFeeRule(
		context.Background(), contextID, leftScheduleID,
		fee.MatchingSideLeft, "left-rule", 1, nil,
	)
	require.NoError(t, err)

	rightRule, err := fee.NewFeeRule(
		context.Background(), contextID, rightScheduleID,
		fee.MatchingSideRight, "right-rule", 1, nil,
	)
	require.NoError(t, err)

	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{
			txID: {ID: txID, SourceID: sourceID, Metadata: map[string]any{}},
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{},
		rightSourceIDs: map[uuid.UUID]struct{}{sourceID: {}},
		leftRules:      []*fee.FeeRule{leftRule},
		rightRules:     []*fee.FeeRule{rightRule},
		allSchedules: map[uuid.UUID]*fee.FeeSchedule{
			leftScheduleID:  leftSchedule,
			rightScheduleID: rightSchedule,
		},
	}

	result := resolveScheduleForTransaction(txID, feeIn)
	require.NotNil(t, result)
	assert.Equal(t, rightScheduleID, result.ID)
}

func TestResolveScheduleForTransaction_UnknownSourceReturnsNil(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200020")
	unknownSourceID := uuid.MustParse("00000000-0000-0000-0000-000000200021")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200022")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200023")

	schedule := &fee.FeeSchedule{ID: scheduleID, Name: "left-schedule", Currency: "USD"}

	leftRule, err := fee.NewFeeRule(
		context.Background(), contextID, scheduleID,
		fee.MatchingSideLeft, "default-left-rule", 1, nil,
	)
	require.NoError(t, err)

	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{
			txID: {ID: txID, SourceID: unknownSourceID, Metadata: map[string]any{}},
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{},
		rightSourceIDs: map[uuid.UUID]struct{}{},
		leftRules:      []*fee.FeeRule{leftRule},
		rightRules:     nil,
		allSchedules:   map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
	}

	result := resolveScheduleForTransaction(txID, feeIn)
	assert.Nil(t, result)
}

func TestResolveScheduleForTransaction_NoMatchingRule(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200030")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200031")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200032")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200033")

	schedule := &fee.FeeSchedule{ID: scheduleID, Name: "wire-only-schedule", Currency: "USD"}

	ruleWithPredicate, err := fee.NewFeeRule(
		context.Background(), contextID, scheduleID,
		fee.MatchingSideLeft, "strict-rule", 1,
		[]fee.FieldPredicate{{Field: "type", Operator: fee.PredicateOperatorEquals, Value: "wire"}},
	)
	require.NoError(t, err)

	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{
			txID: {
				ID:       txID,
				SourceID: sourceID,
				Metadata: map[string]any{"type": "ach"},
			},
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{sourceID: {}},
		rightSourceIDs: map[uuid.UUID]struct{}{},
		leftRules:      []*fee.FeeRule{ruleWithPredicate},
		rightRules:     nil,
		allSchedules:   map[uuid.UUID]*fee.FeeSchedule{scheduleID: schedule},
	}

	result := resolveScheduleForTransaction(txID, feeIn)
	assert.Nil(t, result)
}

// --- processFeeForItem happy path ---

func TestProcessFeeForItem_VarianceCreated(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200040")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200041")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200042")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200043")

	ctx := context.Background()

	item := &matchingEntities.MatchItem{TransactionID: txID}

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

	group := &matchingEntities.MatchGroup{ID: uuid.New()}

	createdRun, err := matchingEntities.NewMatchRun(ctx, contextID, matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	schedule := &fee.FeeSchedule{
		ID:               scheduleID,
		Name:             "flat-fee-schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "flat-fee",
				Priority:  1,
				Structure: fee.FlatFee{Amount: decimal.NewFromInt(10)},
			},
		},
	}

	feeIn := &feeVerificationInput{
		ctxInfo:        &ports.ReconciliationContextInfo{ID: contextID},
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}

	tolerance := fee.Tolerance{}

	result := processFeeForItem(ctx, nil, item, group, createdRun, feeIn, schedule, tolerance)

	require.NotNil(t, result)
	assert.Nil(t, result.fatalErr)
	require.NotNil(t, result.variance)
	assert.Equal(t, string(fee.VarianceOvercharge), result.variance.VarianceType)
	assert.Equal(t, "USD", result.variance.Currency)
	assert.True(t, result.variance.ExpectedFee.Equal(decimal.NewFromInt(10)))
	assert.True(t, result.variance.ActualFee.Equal(decimal.RequireFromString("50.00")))
	require.NotNil(t, result.exceptionInput)
}

func TestProcessFeeForItem_GrossNormalizationUsesGrossedUpExpectedFee(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200044")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200045")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000200046")
	scheduleID := uuid.MustParse("00000000-0000-0000-0000-000000200047")

	ctx := context.Background()
	item := &matchingEntities.MatchItem{TransactionID: txID}

	txn := &shared.Transaction{
		ID:       txID,
		SourceID: sourceID,
		Amount:   decimal.NewFromInt(1000),
		Currency: "USD",
		Metadata: map[string]any{
			"fee": map[string]any{
				"amount":   "15.23",
				"currency": "USD",
			},
		},
	}

	group := &matchingEntities.MatchGroup{ID: uuid.New()}
	createdRun, err := matchingEntities.NewMatchRun(ctx, contextID, matchingVO.MatchRunModeCommit)
	require.NoError(t, err)

	schedule := &fee.FeeSchedule{
		ID:               scheduleID,
		Name:             "gross-normalization-schedule",
		Currency:         "USD",
		ApplicationOrder: fee.ApplicationOrderParallel,
		RoundingScale:    2,
		RoundingMode:     fee.RoundingModeHalfUp,
		Items: []fee.FeeScheduleItem{
			{
				ID:        uuid.New(),
				Name:      "percent-fee",
				Priority:  1,
				Structure: fee.PercentageFee{Rate: decimal.RequireFromString("0.015")},
			},
		},
	}

	normalization := string(fee.NormalizationModeGross)
	feeIn := &feeVerificationInput{
		ctxInfo:        &ports.ReconciliationContextInfo{ID: contextID, FeeNormalization: &normalization},
		txByID:         map[uuid.UUID]*shared.Transaction{txID: txn},
		sourceTypeByID: map[uuid.UUID]string{sourceID: "file"},
	}

	result := processFeeForItem(ctx, nil, item, group, createdRun, feeIn, schedule, fee.Tolerance{})
	assert.Nil(t, result, "gross normalization should not create a false variance when actual fee matches the grossed-up expectation")
}

// --- collectFeeFindings edge cases ---

func TestCollectFeeFindings_SkipsNilItems(t *testing.T) {
	t.Parallel()

	confidence, _ := matchingVO.ParseConfidenceScore(90)
	groups := []*matchingEntities.MatchGroup{
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusConfirmed,
			Confidence: confidence,
			Items:      []*matchingEntities.MatchItem{nil, nil},
		},
	}

	findings, err := collectFeeFindings(
		context.Background(), nil, groups, nil,
		&feeVerificationInput{
			txByID: map[uuid.UUID]*shared.Transaction{},
		},
		fee.Tolerance{},
	)
	require.NoError(t, err)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
}

func TestCollectFeeFindings_SkipsWhenScheduleReturnsNil(t *testing.T) {
	t.Parallel()

	txID := uuid.MustParse("00000000-0000-0000-0000-000000200050")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000200051")
	confidence, _ := matchingVO.ParseConfidenceScore(90)

	groups := []*matchingEntities.MatchGroup{
		{
			ID:         uuid.New(),
			Status:     matchingVO.MatchGroupStatusConfirmed,
			Confidence: confidence,
			Items: []*matchingEntities.MatchItem{
				{TransactionID: txID},
			},
		},
	}

	feeIn := &feeVerificationInput{
		txByID: map[uuid.UUID]*shared.Transaction{
			txID: {ID: txID, SourceID: sourceID, Metadata: map[string]any{}},
		},
		leftSourceIDs:  map[uuid.UUID]struct{}{sourceID: {}},
		rightSourceIDs: map[uuid.UUID]struct{}{},
		leftRules:      nil,
		rightRules:     nil,
		allSchedules:   map[uuid.UUID]*fee.FeeSchedule{},
	}

	findings, err := collectFeeFindings(
		context.Background(), nil, groups, nil,
		feeIn, fee.Tolerance{},
	)
	require.NoError(t, err)
	assert.Empty(t, findings.variances)
	assert.Empty(t, findings.exceptionInputs)
	assert.Equal(t, 1, findings.skippedNoSchedule)
}
