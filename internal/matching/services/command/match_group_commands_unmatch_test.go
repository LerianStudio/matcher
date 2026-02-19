//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	repositoriesmocks "github.com/LerianStudio/matcher/internal/matching/domain/repositories/mocks"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	portsmocks "github.com/LerianStudio/matcher/internal/matching/ports/mocks"
	outboxmocks "github.com/LerianStudio/matcher/internal/outbox/domain/repositories/mocks"
)

var (
	errUnmatchTestDatabase     = errors.New("database error")
	errUnmatchTestDBConnection = errors.New("database connection failed")
)

func mustDefaultTenantUUID(t *testing.T) uuid.UUID {
	t.Helper()

	tenantID, err := uuid.Parse(auth.DefaultTenantID)
	require.NoError(t, err)

	return tenantID
}

func TestValidateUnmatchInput(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000030001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000030002")

	tests := []struct {
		name    string
		input   UnmatchInput
		wantErr error
	}{
		{
			name: "valid input",
			input: UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "User requested unmatch",
			},
			wantErr: nil,
		},
		{
			name: "missing tenant id",
			input: UnmatchInput{
				TenantID:     uuid.Nil,
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "User requested unmatch",
			},
			wantErr: ErrTenantIDRequired,
		},
		{
			name: "missing context id",
			input: UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    uuid.Nil,
				MatchGroupID: matchGroupID,
				Reason:       "User requested unmatch",
			},
			wantErr: ErrUnmatchContextIDRequired,
		},
		{
			name: "missing match group id",
			input: UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    contextID,
				MatchGroupID: uuid.Nil,
				Reason:       "User requested unmatch",
			},
			wantErr: ErrUnmatchMatchGroupIDRequired,
		},
		{
			name: "missing reason",
			input: UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "",
			},
			wantErr: ErrUnmatchReasonRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateUnmatchInput(tt.input)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUnmatch_TenantMismatch_ReturnsError(t *testing.T) {
	t.Parallel()

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	defaultTenantID := mustDefaultTenantUUID(t)
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, defaultTenantID.String())

	input := UnmatchInput{
		TenantID:     uuid.New(),
		ContextID:    uuid.New(),
		MatchGroupID: uuid.New(),
		Reason:       "tenant mismatch",
	}

	err := uc.Unmatch(ctx, input)
	require.ErrorIs(t, err, ErrTenantIDMismatch)
}

func newUnmatchMocks(
	t *testing.T,
) (*repositoriesmocks.MockMatchGroupRepository, *repositoriesmocks.MockMatchItemRepository, *portsmocks.MockTransactionRepository) {
	t.Helper()

	ctrl := gomock.NewController(t)

	return repositoriesmocks.NewMockMatchGroupRepository(ctrl),
		repositoriesmocks.NewMockMatchItemRepository(ctrl),
		portsmocks.NewMockTransactionRepository(ctrl)
}

// withTxExecutor returns a DoAndReturn function that executes the WithTx callback.
func withTxExecutor() func(context.Context, func(matchingRepositories.Tx) error) error {
	return func(_ context.Context, fn func(matchingRepositories.Tx) error) error {
		return fn(nil)
	}
}

// withSQLTxExecutor returns a DoAndReturn function that executes the WithTx
// callback with a zero-value *sql.Tx so that callers requiring a concrete
// *sql.Tx type assertion (e.g. enqueueUnmatchEvent) do not fail.
func withSQLTxExecutor() func(context.Context, func(matchingRepositories.Tx) error) error {
	return func(_ context.Context, fn func(matchingRepositories.Tx) error) error {
		return fn(new(sql.Tx))
	}
}

func TestUnmatch_MatchGroupNotFound(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000031001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000031002")

	tests := []struct {
		name       string
		findResult *matchingEntities.MatchGroup
		findErr    error
		wantErr    error
	}{
		{
			name:    "sql.ErrNoRows",
			findErr: sql.ErrNoRows,
			wantErr: ErrMatchGroupNotFound,
		},
		{
			name:       "nil result",
			findResult: nil,
			wantErr:    ErrMatchGroupNotFound,
		},
		{
			name:    "generic error",
			findErr: errUnmatchTestDBConnection,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
			matchGroupRepo.EXPECT().
				FindByID(gomock.Any(), contextID, matchGroupID).
				Return(tt.findResult, tt.findErr)

			uc := &UseCase{
				matchGroupRepo: matchGroupRepo,
				matchItemRepo:  matchItemRepo,
				txRepo:         txRepo,
			}

			input := UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "Test unmatch",
			}

			err := uc.Unmatch(context.Background(), input)
			require.Error(t, err)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.Contains(t, err.Error(), "find match group")
			}
		})
	}
}

func TestUnmatch_RejectGroupFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000032001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000032002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000032003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000032004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)

	tests := []struct {
		name        string
		group       *matchingEntities.MatchGroup
		errContains string
	}{
		{
			name: "group already rejected",
			group: &matchingEntities.MatchGroup{
				ID:         matchGroupID,
				ContextID:  contextID,
				RunID:      runID,
				RuleID:     ruleID,
				Status:     matchingVO.MatchGroupStatusRejected,
				Confidence: confidence,
			},
			errContains: "reject match group",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
			matchGroupRepo.EXPECT().
				FindByID(gomock.Any(), contextID, matchGroupID).
				Return(tt.group, nil)
			txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

			uc := &UseCase{
				matchGroupRepo: matchGroupRepo,
				matchItemRepo:  matchItemRepo,
				txRepo:         txRepo,
			}

			input := UnmatchInput{
				TenantID:     mustDefaultTenantUUID(t),
				ContextID:    contextID,
				MatchGroupID: matchGroupID,
				Reason:       "Test unmatch",
			}

			err := uc.Unmatch(context.Background(), input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

func TestUnmatch_UpdateGroupFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000033001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000033002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000033003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000033004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, errUnmatchTestDatabase)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Test unmatch",
	}

	err := uc.Unmatch(context.Background(), input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update match group")
}

func TestUnmatch_ListItemsFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000034001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000034002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000034003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000034004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().
		ListByMatchGroupID(gomock.Any(), matchGroupID).
		Return(nil, errUnmatchTestDatabase)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Test unmatch",
	}

	err := uc.Unmatch(context.Background(), input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list match items")
}

func TestUnmatch_MarkUnmatchedFails(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000035001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000035002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000035003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000035004")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000035005")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000035006")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	items := []*matchingEntities.MatchItem{
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID1,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID2,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().ListByMatchGroupID(gomock.Any(), matchGroupID).Return(items, nil)
	txRepo.EXPECT().
		MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), contextID, gomock.Any()).
		Return(errUnmatchTestDatabase)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Test unmatch",
	}

	err := uc.Unmatch(context.Background(), input)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark transactions unmatched")
}

func TestUnmatch_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000036001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000036002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000036003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000036004")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000036005")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000036006")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	items := []*matchingEntities.MatchItem{
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID1,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID2,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().ListByMatchGroupID(gomock.Any(), matchGroupID).Return(items, nil)

	var markedIDs []uuid.UUID

	txRepo.EXPECT().MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), contextID, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ matchingRepositories.Tx, _ uuid.UUID, transactionIDs []uuid.UUID) error {
			markedIDs = append([]uuid.UUID{}, transactionIDs...)
			return nil
		},
	)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "User requested unmatch",
	}

	err := uc.Unmatch(context.Background(), input)
	require.NoError(t, err)

	assert.Equal(t, matchingVO.MatchGroupStatusRejected, group.Status)
	assert.NotNil(t, group.RejectedReason)
	assert.Equal(t, "User requested unmatch", *group.RejectedReason)
	assert.Len(t, markedIDs, 2)
	assert.Contains(t, markedIDs, txID1)
	assert.Contains(t, markedIDs, txID2)
}

func TestUnmatch_NoItems(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000037001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000037002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000037003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000037004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().
		ListByMatchGroupID(gomock.Any(), matchGroupID).
		Return([]*matchingEntities.MatchItem{}, nil)
	txRepo.EXPECT().MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Test unmatch",
	}

	err := uc.Unmatch(context.Background(), input)
	require.NoError(t, err)

	assert.Equal(t, matchingVO.MatchGroupStatusRejected, group.Status)
	assert.NotNil(t, group.RejectedReason)
	assert.Equal(t, "Test unmatch", *group.RejectedReason)
}

func TestUnmatch_ManyItems(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000038001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000038002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000038003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000038004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	items := make([]*matchingEntities.MatchItem, 10)
	for i := range 10 {
		items[i] = &matchingEntities.MatchItem{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     uuid.New(),
			AllocatedAmount:   decimal.NewFromInt(int64(100 + i)),
			AllocatedCurrency: "USD",
		}
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().ListByMatchGroupID(gomock.Any(), matchGroupID).Return(items, nil)

	var markedIDs []uuid.UUID

	txRepo.EXPECT().MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), contextID, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ matchingRepositories.Tx, _ uuid.UUID, transactionIDs []uuid.UUID) error {
			markedIDs = append([]uuid.UUID{}, transactionIDs...)
			return nil
		},
	)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withTxExecutor())

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Test unmatch with many items",
	}

	err := uc.Unmatch(context.Background(), input)
	require.NoError(t, err)

	assert.Len(t, markedIDs, 10)
}

func TestLoadMatchGroup_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000039001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000039002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000039003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000039004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(85)
	require.NoError(t, confidenceErr)
	expectedGroup := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().
		FindByID(gomock.Any(), contextID, matchGroupID).
		Return(expectedGroup, nil)

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	group, err := uc.loadMatchGroup(context.Background(), nil, contextID, matchGroupID)
	require.NoError(t, err)
	require.NotNil(t, group)
	assert.Equal(t, matchGroupID, group.ID)
}

func TestRevertTransactionStatuses_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000040001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000040002")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000040003")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000040004")

	items := []*matchingEntities.MatchItem{
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID1,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
		{
			ID:                uuid.New(),
			MatchGroupID:      matchGroupID,
			TransactionID:     txID2,
			AllocatedAmount:   decimal.NewFromInt(100),
			AllocatedCurrency: "USD",
		},
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchItemRepo.EXPECT().ListByMatchGroupID(gomock.Any(), matchGroupID).Return(items, nil)

	var markedIDs []uuid.UUID

	txRepo.EXPECT().MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), contextID, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ matchingRepositories.Tx, _ uuid.UUID, transactionIDs []uuid.UUID) error {
			markedIDs = append([]uuid.UUID{}, transactionIDs...)
			return nil
		},
	)

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	err := uc.revertTransactionStatuses(context.Background(), nil, nil, contextID, matchGroupID)
	require.NoError(t, err)

	assert.Len(t, markedIDs, 2)
}

func TestRejectOrRevokeGroup_RejectSuccess(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000041001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000041002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000041003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000041004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(90)
	require.NoError(t, confidenceErr)
	group := &matchingEntities.MatchGroup{
		ID:         matchGroupID,
		ContextID:  contextID,
		RunID:      runID,
		RuleID:     ruleID,
		Status:     matchingVO.MatchGroupStatusProposed,
		Confidence: confidence,
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
	}

	err := uc.rejectOrRevokeGroup(context.Background(), nil, nil, group, "Test rejection reason", false)
	require.NoError(t, err)

	assert.Equal(t, matchingVO.MatchGroupStatusRejected, group.Status)
	assert.NotNil(t, group.RejectedReason)
	assert.Equal(t, "Test rejection reason", *group.RejectedReason)
}

func TestEnqueueUnmatchEvent_NilOutboxRepo(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000043001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000043002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000043003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000043004")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000043005")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	group := &matchingEntities.MatchGroup{
		ID:          matchGroupID,
		ContextID:   contextID,
		RunID:       runID,
		RuleID:      ruleID,
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items: []*matchingEntities.MatchItem{
			{
				ID:                uuid.New(),
				MatchGroupID:      matchGroupID,
				TransactionID:     txID1,
				AllocatedAmount:   decimal.NewFromInt(100),
				AllocatedCurrency: "USD",
			},
		},
	}

	// UseCase with outboxRepoTx deliberately nil
	uc := &UseCase{}

	err := uc.enqueueUnmatchEvent(context.Background(), new(sql.Tx), group, "some reason")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOutboxRepoNotConfigured)
}

func TestEnqueueUnmatchEvent_ListItemsError(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000044001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000044002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000044003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000044004")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	// Group has NO pre-loaded items (Items slice is empty), forcing a
	// ListByMatchGroupID call.
	group := &matchingEntities.MatchGroup{
		ID:          matchGroupID,
		ContextID:   contextID,
		RunID:       runID,
		RuleID:      ruleID,
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items:       nil,
	}

	ctrl := gomock.NewController(t)
	matchItemRepo := repositoriesmocks.NewMockMatchItemRepository(ctrl)
	matchItemRepo.EXPECT().
		ListByMatchGroupID(gomock.Any(), matchGroupID).
		Return(nil, errUnmatchTestDatabase)

	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc := &UseCase{
		matchItemRepo: matchItemRepo,
		outboxRepoTx:  outboxRepo,
	}

	err := uc.enqueueUnmatchEvent(context.Background(), new(sql.Tx), group, "revoke reason")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load match items for unmatch event")
	assert.ErrorIs(t, err, errUnmatchTestDatabase)
}

func TestEnqueueUnmatchEvent_InvalidEventData(t *testing.T) {
	t.Parallel()

	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000045001")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000045002")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	// Group has a valid match group ID but uuid.Nil for ContextID, which will
	// cause NewMatchUnmatchedEvent to return ErrMatchUnmatchedContextIDRequired.
	group := &matchingEntities.MatchGroup{
		ID:          matchGroupID,
		ContextID:   uuid.Nil, // Invalid: triggers event constructor error
		RunID:       uuid.New(),
		RuleID:      uuid.New(),
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items: []*matchingEntities.MatchItem{
			{
				ID:                uuid.New(),
				MatchGroupID:      matchGroupID,
				TransactionID:     txID1,
				AllocatedAmount:   decimal.NewFromInt(50),
				AllocatedCurrency: "USD",
			},
		},
	}

	ctrl := gomock.NewController(t)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc := &UseCase{
		outboxRepoTx: outboxRepo,
	}

	err := uc.enqueueUnmatchEvent(context.Background(), new(sql.Tx), group, "revoke reason")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "build match unmatched event")
}

func TestEnqueueUnmatchEvent_NonSQLTx(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000046001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000046002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000046003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000046004")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000046005")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	group := &matchingEntities.MatchGroup{
		ID:          matchGroupID,
		ContextID:   contextID,
		RunID:       runID,
		RuleID:      ruleID,
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items: []*matchingEntities.MatchItem{
			{
				ID:                uuid.New(),
				MatchGroupID:      matchGroupID,
				TransactionID:     txID1,
				AllocatedAmount:   decimal.NewFromInt(100),
				AllocatedCurrency: "USD",
			},
		},
	}

	ctrl := gomock.NewController(t)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)

	uc := &UseCase{
		outboxRepoTx: outboxRepo,
	}

	// Pass a non-*sql.Tx value to exercise the type-assertion guard.
	err := uc.enqueueUnmatchEvent(context.Background(), &fakeTx{}, group, "some reason")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOutboxRequiresSQLTx)
}

func TestUnmatch_ConfirmedGroup_Revokes(t *testing.T) {
	t.Parallel()

	contextID := uuid.MustParse("00000000-0000-0000-0000-000000042001")
	matchGroupID := uuid.MustParse("00000000-0000-0000-0000-000000042002")
	runID := uuid.MustParse("00000000-0000-0000-0000-000000042003")
	ruleID := uuid.MustParse("00000000-0000-0000-0000-000000042004")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000042005")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000042006")

	confidence, confidenceErr := matchingVO.ParseConfidenceScore(95)
	require.NoError(t, confidenceErr)
	now := time.Now().UTC()

	group := &matchingEntities.MatchGroup{
		ID:          matchGroupID,
		ContextID:   contextID,
		RunID:       runID,
		RuleID:      ruleID,
		Status:      matchingVO.MatchGroupStatusConfirmed,
		Confidence:  confidence,
		ConfirmedAt: &now,
		CreatedAt:   now,
		UpdatedAt:   now,
		Items: []*matchingEntities.MatchItem{
			{
				ID:                uuid.New(),
				MatchGroupID:      matchGroupID,
				TransactionID:     txID1,
				AllocatedAmount:   decimal.NewFromInt(100),
				AllocatedCurrency: "USD",
			},
			{
				ID:                uuid.New(),
				MatchGroupID:      matchGroupID,
				TransactionID:     txID2,
				AllocatedAmount:   decimal.NewFromInt(100),
				AllocatedCurrency: "USD",
			},
		},
	}

	matchGroupRepo, matchItemRepo, txRepo := newUnmatchMocks(t)
	matchGroupRepo.EXPECT().FindByID(gomock.Any(), contextID, matchGroupID).Return(group, nil)
	matchGroupRepo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(group, nil)
	matchItemRepo.EXPECT().ListByMatchGroupID(gomock.Any(), matchGroupID).Return(group.Items, nil)

	var markedIDs []uuid.UUID

	txRepo.EXPECT().MarkUnmatchedWithTx(gomock.Any(), gomock.Any(), contextID, gomock.Any()).DoAndReturn(
		func(_ context.Context, _ matchingRepositories.Tx, _ uuid.UUID, transactionIDs []uuid.UUID) error {
			markedIDs = append([]uuid.UUID{}, transactionIDs...)
			return nil
		},
	)
	txRepo.EXPECT().WithTx(gomock.Any(), gomock.Any()).DoAndReturn(withSQLTxExecutor())

	ctrl := gomock.NewController(t)
	outboxRepo := outboxmocks.NewMockOutboxRepository(ctrl)
	outboxRepo.EXPECT().CreateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()

	uc := &UseCase{
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		txRepo:         txRepo,
		outboxRepoTx:   outboxRepo,
	}

	input := UnmatchInput{
		TenantID:     mustDefaultTenantUUID(t),
		ContextID:    contextID,
		MatchGroupID: matchGroupID,
		Reason:       "Revoke confirmed group",
	}

	err := uc.Unmatch(context.Background(), input)
	require.NoError(t, err)

	assert.Equal(t, matchingVO.MatchGroupStatusRevoked, group.Status)
	assert.NotNil(t, group.ConfirmedAt, "ConfirmedAt must be preserved after revocation for audit trail")
	assert.NotNil(t, group.RejectedReason)
	assert.Equal(t, "Revoke confirmed group", *group.RejectedReason)
	assert.Len(t, markedIDs, 2)
	assert.Contains(t, markedIDs, txID1)
	assert.Contains(t, markedIDs, txID2)
}
