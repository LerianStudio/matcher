// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

//nolint:dupl
package command

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepositories "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/matching/ports"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var (
	errTestDatabase     = errors.New("database error")
	errTestDBConnection = errors.New("database connection failed")
	errTestRunCreate    = errors.New("run creation failed")
	errTestGroupCreate  = errors.New("group creation failed")
	errTestItemCreate   = errors.New("item creation failed")
	errTestMarkFailed   = errors.New("mark failed")
)

// stubOutboxTxCreatorForManual is a no-op outbox transactional creator used
// by manual-match tests to satisfy the outbox event emission step.
type stubOutboxTxCreatorForManual struct{}

func (s *stubOutboxTxCreatorForManual) CreateWithTx(
	_ context.Context,
	_ *sql.Tx,
	event *shared.OutboxEvent,
) (*shared.OutboxEvent, error) {
	return event, nil
}

func TestValidateManualMatchInput(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000010001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000010002")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000010003")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000010004")

	tests := []struct {
		name    string
		input   ManualMatchInput
		wantErr error
	}{
		{
			name: "valid input",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
				Notes:          "manual match",
			},
			wantErr: nil,
		},
		{
			name: "missing tenant id",
			input: ManualMatchInput{
				TenantID:       uuid.Nil,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			},
			wantErr: ErrTenantIDRequired,
		},
		{
			name: "missing context id",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      uuid.Nil,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			},
			wantErr: ErrRunMatchContextIDRequired,
		},
		{
			name: "single transaction",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1},
			},
			wantErr: ErrMinimumTransactionsRequired,
		},
		{
			name: "empty transaction list",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{},
			},
			wantErr: ErrMinimumTransactionsRequired,
		},
		{
			name: "nil transaction list",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: nil,
			},
			wantErr: ErrMinimumTransactionsRequired,
		},
		{
			name: "duplicate transaction ids",
			input: ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID1},
			},
			wantErr: ErrDuplicateTransactionIDs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{}
			err := uc.validateManualMatchInput(tt.input)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestManualMatch_ContextValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000011001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000011002")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000011003")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000011004")

	tests := []struct {
		name        string
		ctxProvider stubContextProvider
		wantErr     error
	}{
		{
			name: "context not found - provider error",
			ctxProvider: stubContextProvider{
				contextInfo: nil,
				err:         errTestDatabase,
			},
			wantErr: ErrContextNotFound,
		},
		{
			name: "context not found - nil result",
			ctxProvider: stubContextProvider{
				contextInfo: nil,
				err:         nil,
			},
			wantErr: ErrContextNotFound,
		},
		{
			name: "context not active",
			ctxProvider: stubContextProvider{
				contextInfo: &ports.ReconciliationContextInfo{
					ID:     contextID,
					Active: false,
				},
			},
			wantErr: ErrContextNotActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{
				contextProvider: tt.ctxProvider,
			}

			input := ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			}

			result, err := uc.ManualMatch(context.Background(), input)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErr)
			require.Nil(t, result)
		})
	}
}

func TestManualMatch_TransactionValidation(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000012001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000012002")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000012003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000012004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000012005")
	txID3 := uuid.MustParse("00000000-0000-0000-0000-000000012006")

	tests := []struct {
		name        string
		txRepo      *stubTxRepo
		txIDs       []uuid.UUID
		wantErr     error
		errContains string
	}{
		{
			name: "transaction not found",
			txRepo: &stubTxRepo{
				transactions: []*shared.Transaction{
					{
						ID:       txID1,
						SourceID: sourceID,
						Amount:   decimal.NewFromInt(100),
						Currency: "USD",
						Status:   shared.TransactionStatusUnmatched,
					},
				},
			},
			txIDs:   []uuid.UUID{txID1, txID2},
			wantErr: ErrTransactionNotFound,
		},
		{
			name: "transaction already matched",
			txRepo: &stubTxRepo{
				transactions: []*shared.Transaction{
					{
						ID:       txID1,
						SourceID: sourceID,
						Amount:   decimal.NewFromInt(100),
						Currency: "USD",
						Status:   shared.TransactionStatusUnmatched,
					},
					{
						ID:       txID2,
						SourceID: sourceID,
						Amount:   decimal.NewFromInt(100),
						Currency: "USD",
						Status:   shared.TransactionStatusMatched,
					},
				},
			},
			txIDs:       []uuid.UUID{txID1, txID2},
			wantErr:     ErrTransactionNotUnmatched,
			errContains: txID2.String(),
		},
		{
			name: "repository error",
			txRepo: &stubTxRepo{
				listErr: errTestDBConnection,
			},
			txIDs:       []uuid.UUID{txID1, txID2, txID3},
			errContains: "find transactions",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{
				contextProvider: stubContextProvider{
					contextInfo: &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: true,
					},
				},
				txRepo: tt.txRepo,
			}

			input := ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: tt.txIDs,
			}

			result, err := uc.ManualMatch(context.Background(), input)
			require.Error(t, err)
			require.Nil(t, result)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}

			if tt.errContains != "" {
				assert.Contains(t, err.Error(), tt.errContains)
			}
		})
	}
}

func TestManualMatch_Success(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000013001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000013002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000013003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000013004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000013005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000013006")

	tx1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID1,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}

	txRepo := &stubTxRepo{
		transactions: []*shared.Transaction{tx1, tx2},
	}
	matchRunRepo := &stubMatchRunRepo{}
	matchGroupRepo := &stubMatchGroupRepo{}
	matchItemRepo := &stubMatchItemRepo{}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         txRepo,
		matchRunRepo:   matchRunRepo,
		matchGroupRepo: matchGroupRepo,
		matchItemRepo:  matchItemRepo,
		outboxRepoTx:   &stubOutboxTxCreatorForManual{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: []uuid.UUID{txID1, txID2},
		Notes:          "manual match test",
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, contextID, result.ContextID)
	assert.Equal(t, uuid.Nil, result.RuleID)
	assert.Equal(t, matchingVO.MatchGroupStatusConfirmed, result.Status)
	assert.NotNil(t, result.ConfirmedAt)
	assert.Len(t, result.Items, 2)

	assert.NotNil(t, matchRunRepo.created)
	assert.True(t, matchGroupRepo.called)
	assert.True(t, matchItemRepo.called)
	assert.Equal(t, 1, txRepo.markCalls)
	assert.Len(t, txRepo.markedIDs, 2)
}

func TestManualMatch_WithBaseAmount(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000014001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000014002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000014003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000014004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000014005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000014006")

	baseAmount := decimal.NewFromInt(85)
	baseCurrency := "EUR"

	tx1 := &shared.Transaction{
		ID:           txID1,
		SourceID:     sourceID1,
		Amount:       decimal.NewFromInt(100),
		Currency:     "USD",
		AmountBase:   &baseAmount,
		BaseCurrency: &baseCurrency,
		Status:       shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(85),
		Currency: "EUR",
		Status:   shared.TransactionStatusUnmatched,
	}

	txRepo := &stubTxRepo{
		transactions: []*shared.Transaction{tx1, tx2},
	}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         txRepo,
		matchRunRepo:   &stubMatchRunRepo{},
		matchGroupRepo: &stubMatchGroupRepo{},
		matchItemRepo:  &stubMatchItemRepo{},
		outboxRepoTx:   &stubOutboxTxCreatorForManual{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: []uuid.UUID{txID1, txID2},
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Items, 2)
}

func TestManualMatch_RepositoryErrors(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000015001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000015002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000015003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000015004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000015005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000015006")

	tx1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID1,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}

	tests := []struct {
		name           string
		matchRunRepo   *stubMatchRunRepo
		matchGroupRepo *stubMatchGroupRepo
		matchItemRepo  *stubMatchItemRepo
		txRepo         *stubTxRepo
		errContains    string
	}{
		{
			name:           "match run creation fails",
			matchRunRepo:   &stubMatchRunRepo{createErr: errTestRunCreate},
			matchGroupRepo: &stubMatchGroupRepo{},
			matchItemRepo:  &stubMatchItemRepo{},
			txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
			errContains:    "create match run",
		},
		{
			name:           "match group creation fails",
			matchRunRepo:   &stubMatchRunRepo{},
			matchGroupRepo: &stubMatchGroupRepo{createErr: errTestGroupCreate},
			matchItemRepo:  &stubMatchItemRepo{},
			txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
			errContains:    "persist match group",
		},
		{
			name:           "match item creation fails",
			matchRunRepo:   &stubMatchRunRepo{},
			matchGroupRepo: &stubMatchGroupRepo{},
			matchItemRepo:  &stubMatchItemRepo{createErr: errTestItemCreate},
			txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
			errContains:    "persist match items",
		},
		{
			name:           "mark matched fails",
			matchRunRepo:   &stubMatchRunRepo{},
			matchGroupRepo: &stubMatchGroupRepo{},
			matchItemRepo:  &stubMatchItemRepo{},
			txRepo: &stubTxRepo{
				transactions: []*shared.Transaction{tx1, tx2},
				markErr:      errTestMarkFailed,
			},
			errContains: "mark transactions matched",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{
				contextProvider: stubContextProvider{
					contextInfo: &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: true,
					},
				},
				txRepo:         tt.txRepo,
				matchRunRepo:   tt.matchRunRepo,
				matchGroupRepo: tt.matchGroupRepo,
				matchItemRepo:  tt.matchItemRepo,
			}

			input := ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: []uuid.UUID{txID1, txID2},
			}

			result, err := uc.ManualMatch(context.Background(), input)
			require.Error(t, err)
			require.Nil(t, result)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}

type stubMatchGroupRepoEmpty struct {
	stubMatchGroupRepo
}

func (s *stubMatchGroupRepoEmpty) CreateBatchWithTx(
	_ context.Context,
	_ matchingRepositories.Tx,
	_ []*matchingEntities.MatchGroup,
) ([]*matchingEntities.MatchGroup, error) {
	return []*matchingEntities.MatchGroup{}, nil
}

func TestManualMatch_NoGroupCreated(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000016001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000016002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000016003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000016004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000016005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000016006")

	tx1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID1,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
		matchRunRepo:   &stubMatchRunRepo{},
		matchGroupRepo: &stubMatchGroupRepoEmpty{},
		matchItemRepo:  &stubMatchItemRepo{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: []uuid.UUID{txID1, txID2},
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.Error(t, err)
	require.Nil(t, result)
	require.ErrorIs(t, err, ErrManualMatchNoGroupCreated)
}

func TestManualMatch_MultipleTransactions(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000017001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000017002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000017003")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000017004")

	sourceIDs := []uuid.UUID{sourceID1, sourceID2, sourceID1, sourceID2, sourceID1}
	txIDs := make([]uuid.UUID, 5)
	transactions := make([]*shared.Transaction, 5)

	for i := range 5 {
		txIDs[i] = uuid.New()
		transactions[i] = &shared.Transaction{
			ID:       txIDs[i],
			SourceID: sourceIDs[i],
			Amount:   decimal.NewFromInt(int64(100 + i)),
			Currency: "USD",
			Status:   shared.TransactionStatusUnmatched,
		}
	}

	txRepo := &stubTxRepo{transactions: transactions}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         txRepo,
		matchRunRepo:   &stubMatchRunRepo{},
		matchGroupRepo: &stubMatchGroupRepo{},
		matchItemRepo:  &stubMatchItemRepo{},
		outboxRepoTx:   &stubOutboxTxCreatorForManual{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: txIDs,
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Items, 5)
	assert.Len(t, txRepo.markedIDs, 5)
}

type stubMatchRunRepoForManualMatch struct {
	stubMatchRunRepo
	updateCalled bool
	updatedRun   *matchingEntities.MatchRun
}

func (s *stubMatchRunRepoForManualMatch) UpdateWithTx(
	ctx context.Context,
	tx matchingRepositories.Tx,
	run *matchingEntities.MatchRun,
) (*matchingEntities.MatchRun, error) {
	s.updateCalled = true
	s.updatedRun = run

	return run, nil
}

func TestManualMatch_RunIsCompleted(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000018001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000018002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000018003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000018004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000018005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000018006")

	tx1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID1,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}

	matchRunRepo := &stubMatchRunRepoForManualMatch{}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
		matchRunRepo:   matchRunRepo,
		matchGroupRepo: &stubMatchGroupRepo{},
		matchItemRepo:  &stubMatchItemRepo{},
		outboxRepoTx:   &stubOutboxTxCreatorForManual{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: []uuid.UUID{txID1, txID2},
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.True(t, matchRunRepo.updateCalled)
	assert.NotNil(t, matchRunRepo.updatedRun)
	assert.Equal(t, matchingVO.MatchRunStatusCompleted, matchRunRepo.updatedRun.Status)
	assert.NotNil(t, matchRunRepo.updatedRun.CompletedAt)
}

func TestManualMatch_ConfidenceIs100(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000019001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000019002")
	sourceID1 := uuid.MustParse("00000000-0000-0000-0000-000000019003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000019004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000019005")
	sourceID2 := uuid.MustParse("00000000-0000-0000-0000-000000019006")

	tx1 := &shared.Transaction{
		ID:       txID1,
		SourceID: sourceID1,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}
	tx2 := &shared.Transaction{
		ID:       txID2,
		SourceID: sourceID2,
		Amount:   decimal.NewFromInt(100),
		Currency: "USD",
		Status:   shared.TransactionStatusUnmatched,
	}

	uc := &UseCase{
		contextProvider: stubContextProvider{
			contextInfo: &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			},
		},
		txRepo:         &stubTxRepo{transactions: []*shared.Transaction{tx1, tx2}},
		matchRunRepo:   &stubMatchRunRepo{},
		matchGroupRepo: &stubMatchGroupRepo{},
		matchItemRepo:  &stubMatchItemRepo{},
		outboxRepoTx:   &stubOutboxTxCreatorForManual{},
	}

	input := ManualMatchInput{
		TenantID:       tenantID,
		ContextID:      contextID,
		TransactionIDs: []uuid.UUID{txID1, txID2},
	}

	result, err := uc.ManualMatch(context.Background(), input)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 100, result.Confidence.Value())
}

func TestManualMatch_SourceDiversityRequired(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000020001")
	contextID := uuid.MustParse("00000000-0000-0000-0000-000000020002")
	sourceID := uuid.MustParse("00000000-0000-0000-0000-000000020003")
	txID1 := uuid.MustParse("00000000-0000-0000-0000-000000020004")
	txID2 := uuid.MustParse("00000000-0000-0000-0000-000000020005")
	txID3 := uuid.MustParse("00000000-0000-0000-0000-000000020006")

	tests := []struct {
		name         string
		transactions []*shared.Transaction
		txIDs        []uuid.UUID
	}{
		{
			name: "two transactions same source",
			transactions: []*shared.Transaction{
				{
					ID:       txID1,
					SourceID: sourceID,
					Amount:   decimal.NewFromInt(100),
					Currency: "USD",
					Status:   shared.TransactionStatusUnmatched,
				},
				{
					ID:       txID2,
					SourceID: sourceID,
					Amount:   decimal.NewFromInt(200),
					Currency: "USD",
					Status:   shared.TransactionStatusUnmatched,
				},
			},
			txIDs: []uuid.UUID{txID1, txID2},
		},
		{
			name: "three transactions same source",
			transactions: []*shared.Transaction{
				{
					ID:       txID1,
					SourceID: sourceID,
					Amount:   decimal.NewFromInt(100),
					Currency: "USD",
					Status:   shared.TransactionStatusUnmatched,
				},
				{
					ID:       txID2,
					SourceID: sourceID,
					Amount:   decimal.NewFromInt(200),
					Currency: "USD",
					Status:   shared.TransactionStatusUnmatched,
				},
				{
					ID:       txID3,
					SourceID: sourceID,
					Amount:   decimal.NewFromInt(300),
					Currency: "USD",
					Status:   shared.TransactionStatusUnmatched,
				},
			},
			txIDs: []uuid.UUID{txID1, txID2, txID3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uc := &UseCase{
				contextProvider: stubContextProvider{
					contextInfo: &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: true,
					},
				},
				txRepo: &stubTxRepo{transactions: tt.transactions},
			}

			input := ManualMatchInput{
				TenantID:       tenantID,
				ContextID:      contextID,
				TransactionIDs: tt.txIDs,
			}

			result, err := uc.ManualMatch(context.Background(), input)
			require.Error(t, err)
			require.Nil(t, result)
			require.ErrorIs(t, err, ErrManualMatchSourcesNotDiverse)
		})
	}
}
