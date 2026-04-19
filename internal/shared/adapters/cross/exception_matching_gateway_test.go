//go:build unit

package cross

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"

	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	ingestionEntities "github.com/LerianStudio/matcher/internal/ingestion/domain/entities"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

var (
	errLookupFailed = errors.New("lookup failed")
	errDBError      = errors.New("db error")
)

type stubAdjustmentRepo struct {
	created *matchingEntities.Adjustment
	err     error
}

func (stub *stubAdjustmentRepo) Create(_ context.Context, adj *matchingEntities.Adjustment) (*matchingEntities.Adjustment, error) {
	stub.created = adj
	if stub.err != nil {
		return nil, stub.err
	}
	return adj, nil
}

func (stub *stubAdjustmentRepo) CreateWithTx(ctx context.Context, _ *sql.Tx, adj *matchingEntities.Adjustment) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) CreateWithAuditLog(ctx context.Context, adj *matchingEntities.Adjustment, _ *shared.AuditLog) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) CreateWithAuditLogWithTx(ctx context.Context, _ *sql.Tx, adj *matchingEntities.Adjustment, _ *shared.AuditLog) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) FindByID(_ context.Context, _, _ uuid.UUID) (*matchingEntities.Adjustment, error) {
	return nil, nil
}

func (stub *stubAdjustmentRepo) ListByContextID(_ context.Context, _ uuid.UUID, _ matchingRepos.CursorFilter) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (stub *stubAdjustmentRepo) ListByMatchGroupID(_ context.Context, _, _ uuid.UUID) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

type stubTransactionRepo struct {
	tx                *shared.Transaction
	findErr           error
	markMatchedErr    error
	markMatchedCalled bool
	contextID         uuid.UUID
	transactionIDs    []uuid.UUID
}

func (stub *stubTransactionRepo) FindByID(_ context.Context, _ uuid.UUID) (*shared.Transaction, error) {
	return stub.tx, stub.findErr
}

func (stub *stubTransactionRepo) MarkMatched(_ context.Context, contextID uuid.UUID, txIDs []uuid.UUID) error {
	stub.markMatchedCalled = true
	stub.contextID = contextID
	stub.transactionIDs = txIDs
	return stub.markMatchedErr
}

type stubJobFinder struct {
	job *ingestionEntities.IngestionJob
	err error
}

func (stub *stubJobFinder) FindByID(_ context.Context, _ uuid.UUID) (*ingestionEntities.IngestionJob, error) {
	return stub.job, stub.err
}

type stubSourceContextFinder struct {
	contextID uuid.UUID
	err       error
}

func (stub *stubSourceContextFinder) GetContextIDBySourceID(_ context.Context, _ uuid.UUID) (uuid.UUID, error) {
	return stub.contextID, stub.err
}

func TestNewExceptionMatchingGateway(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	jobFinder := &stubJobFinder{}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, jobFinder, nil)
	require.NoError(t, err)
	assert.NotNil(t, gateway)
}

func TestNewExceptionMatchingGateway_NilDependencies(t *testing.T) {
	t.Parallel()

	t.Run("nil adjustment repo", func(t *testing.T) {
		t.Parallel()
		gateway, err := NewExceptionMatchingGateway(nil, &stubTransactionRepo{}, &stubJobFinder{}, nil)
		require.ErrorIs(t, err, ErrNilAdjustmentRepository)
		assert.Nil(t, gateway)
	})

	t.Run("nil transaction repo", func(t *testing.T) {
		t.Parallel()
		gateway, err := NewExceptionMatchingGateway(&stubAdjustmentRepo{}, nil, &stubJobFinder{}, nil)
		require.ErrorIs(t, err, ErrNilTransactionRepository)
		assert.Nil(t, gateway)
	})

	t.Run("nil job finder", func(t *testing.T) {
		t.Parallel()
		gateway, err := NewExceptionMatchingGateway(&stubAdjustmentRepo{}, &stubTransactionRepo{}, nil, nil)
		require.ErrorIs(t, err, ErrNilJobFinder)
		assert.Nil(t, gateway)
	})
}

func TestExceptionMatchingGateway_CreateForceMatch_NilReceiverGuard(t *testing.T) {
	t.Parallel()

	var gateway *ExceptionMatchingGateway

	err := gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: uuid.New()})
	require.ErrorIs(t, err, ErrNilTransactionRepository)
}

func TestExceptionMatchingGateway_CreateAdjustment_NilReceiverGuard(t *testing.T) {
	t.Parallel()

	var gateway *ExceptionMatchingGateway

	err := gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{TransactionID: uuid.New()})
	require.ErrorIs(t, err, ErrNilAdjustmentRepository)
}

func TestExceptionMatchingGateway_CreateForceMatch_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	transactionID := uuid.New()
	jobID := uuid.New()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}}
	jobFinder := &stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, jobFinder, nil)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
	require.NoError(t, err)
	assert.True(t, txRepo.markMatchedCalled)
	assert.Equal(t, contextID, txRepo.contextID)
	assert.Equal(t, []uuid.UUID{transactionID}, txRepo.transactionIDs)
}

func TestExceptionMatchingGateway_CreateForceMatch_MarkMatchedError(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	errMarkMatched := errors.New("mark matched failed")

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}, markMatchedErr: errMarkMatched},
		&stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}},
		nil,
	)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
	require.Error(t, err)
	require.ErrorIs(t, err, errMarkMatched)
}

func TestExceptionMatchingGateway_CreateForceMatch_ContextResolutionErrors(t *testing.T) {
	t.Parallel()

	t.Run("transaction lookup error", func(t *testing.T) {
		t.Parallel()
		gateway, err := NewExceptionMatchingGateway(&stubAdjustmentRepo{}, &stubTransactionRepo{findErr: errDBError}, &stubJobFinder{}, nil)
		require.NoError(t, err)
		err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: uuid.New()})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve context ID")
	})

	t.Run("transaction not found normalizes wrapped sql err", func(t *testing.T) {
		t.Parallel()
		transactionID := uuid.New()
		gateway, err := NewExceptionMatchingGateway(
			&stubAdjustmentRepo{},
			&stubTransactionRepo{findErr: fmt.Errorf("failed to find transaction: %w", sql.ErrNoRows)},
			&stubJobFinder{},
			nil,
		)
		require.NoError(t, err)
		err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
		require.ErrorIs(t, err, ErrTransactionNotFound)
	})

	t.Run("job lookup error with no fallback", func(t *testing.T) {
		t.Parallel()
		transactionID := uuid.New()
		jobID := uuid.New()
		gateway, err := NewExceptionMatchingGateway(
			&stubAdjustmentRepo{},
			&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}},
			&stubJobFinder{err: errLookupFailed},
			nil,
		)
		require.NoError(t, err)
		err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve context ID")
	})

	t.Run("job not found normalizes wrapped sql err", func(t *testing.T) {
		t.Parallel()
		transactionID := uuid.New()
		jobID := uuid.New()
		gateway, err := NewExceptionMatchingGateway(
			&stubAdjustmentRepo{},
			&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}},
			&stubJobFinder{err: fmt.Errorf("failed to find job: %w", sql.ErrNoRows)},
			nil,
		)
		require.NoError(t, err)
		err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
		require.ErrorIs(t, err, ErrIngestionJobNotFound)
	})
}

func TestExceptionMatchingGateway_UsesSourceFallback(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	sourceID := uuid.New()
	contextID := uuid.New()

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID, SourceID: sourceID}},
		&stubJobFinder{err: errLookupFailed},
		&stubSourceContextFinder{contextID: contextID},
	)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
	require.NoError(t, err)
}

func TestExceptionMatchingGateway_SourceFallbackNotFoundPreservesPrimaryNotFoundSemantics(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	sourceID := uuid.New()

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID, SourceID: sourceID}},
		&stubJobFinder{err: errLookupFailed},
		&stubSourceContextFinder{err: fmt.Errorf("find context id by source id: %w", sql.ErrNoRows)},
	)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
	require.ErrorIs(t, err, errLookupFailed)
}

func TestMapSourceLookupError_NotFoundNormalization(t *testing.T) {
	t.Parallel()

	err := mapSourceLookupError(ErrIngestionJobNotFound, sql.ErrNoRows)
	require.ErrorIs(t, err, ErrSourceNotFound)
}

func TestExceptionMatchingGateway_SourceFallbackPreservesPrimaryOperationalError(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	sourceID := uuid.New()

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID, SourceID: sourceID}},
		&stubJobFinder{err: errLookupFailed},
		&stubSourceContextFinder{err: fmt.Errorf("find context id by source id: %w", sql.ErrNoRows)},
	)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{TransactionID: transactionID})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve context ID")
	assert.Contains(t, err.Error(), errLookupFailed.Error())
}

func TestExceptionMatchingGateway_CreateAdjustment_Success(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}}
	jobFinder := &stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, jobFinder, nil)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		TransactionID: transactionID,
		Direction:     "DEBIT",
		Amount:        decimal.NewFromInt(10),
		Currency:      "USD",
		Reason:        "MANUAL_CORRECTION",
		Notes:         "fix",
		Actor:         "tester",
	})
	require.NoError(t, err)
	require.NotNil(t, adjRepo.created)
	assert.Equal(t, contextID, adjRepo.created.ContextID)
	assert.Equal(t, transactionID, *adjRepo.created.TransactionID)
}

func TestExceptionMatchingGateway_CreateAdjustment_InvalidDirection(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}}
	jobFinder := &stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, jobFinder, nil)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		TransactionID: transactionID,
		Direction:     "SIDEWAYS",
		Amount:        decimal.NewFromInt(10),
		Currency:      "USD",
		Reason:        "MANUAL_CORRECTION",
	})
	require.ErrorIs(t, err, ErrInvalidDirection)
}

func TestExceptionMatchingGateway_CreateAdjustment_PersistError(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()
	errPersist := errors.New("persist failed")

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{err: errPersist},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}},
		&stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}},
		nil,
	)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		TransactionID: transactionID,
		Direction:     "DEBIT",
		Amount:        decimal.NewFromInt(10),
		Currency:      "USD",
		Reason:        "MANUAL_CORRECTION",
		Notes:         "persist me",
		Actor:         "tester",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errPersist)
}

func TestExceptionMatchingGateway_CreateAdjustment_EntityValidationError(t *testing.T) {
	t.Parallel()

	transactionID := uuid.New()
	jobID := uuid.New()
	contextID := uuid.New()

	gateway, err := NewExceptionMatchingGateway(
		&stubAdjustmentRepo{},
		&stubTransactionRepo{tx: &shared.Transaction{ID: transactionID, IngestionJobID: jobID}},
		&stubJobFinder{job: &ingestionEntities.IngestionJob{ID: jobID, ContextID: contextID}},
		nil,
	)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		TransactionID: transactionID,
		Direction:     "DEBIT",
		Amount:        decimal.NewFromInt(10),
		Currency:      "",
		Reason:        "MANUAL_CORRECTION",
		Notes:         "invalid currency",
		Actor:         "tester",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create adjustment entity")
}

func TestMapReasonToAdjustmentType(t *testing.T) {
	t.Parallel()

	assert.Equal(t, matchingEntities.AdjustmentTypeFXDifference, mapReasonToAdjustmentType("CURRENCY_CORRECTION"))
	assert.Equal(t, matchingEntities.AdjustmentTypeMiscellaneous, mapReasonToAdjustmentType("MANUAL_CORRECTION"))
}

func TestExceptionMatchingGateway_ImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ exceptionPorts.MatchingGateway = (*ExceptionMatchingGateway)(nil)
}
