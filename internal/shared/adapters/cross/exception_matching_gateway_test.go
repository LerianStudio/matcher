//go:build unit

package cross

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

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"

	exceptionPorts "github.com/LerianStudio/matcher/internal/exception/ports"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	matchingRepos "github.com/LerianStudio/matcher/internal/matching/domain/repositories"
	shared "github.com/LerianStudio/matcher/internal/shared/domain"
)

// Static errors for testing (err113 compliance).
var (
	errLookupFailed = errors.New("lookup failed")
	errDBError      = errors.New("db error")
)

// stubAdjustmentRepo is a test stub for AdjustmentRepository.
type stubAdjustmentRepo struct {
	created *matchingEntities.Adjustment
	err     error
}

func (stub *stubAdjustmentRepo) Create(
	_ context.Context,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	stub.created = adj
	if stub.err != nil {
		return nil, stub.err
	}

	return adj, nil
}

func (stub *stubAdjustmentRepo) CreateWithTx(
	ctx context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) CreateWithAuditLog(
	ctx context.Context,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) CreateWithAuditLogWithTx(
	ctx context.Context,
	_ *sql.Tx,
	adj *matchingEntities.Adjustment,
	_ *shared.AuditLog,
) (*matchingEntities.Adjustment, error) {
	return stub.Create(ctx, adj)
}

func (stub *stubAdjustmentRepo) FindByID(
	_ context.Context,
	_, _ uuid.UUID,
) (*matchingEntities.Adjustment, error) {
	return nil, nil
}

func (stub *stubAdjustmentRepo) ListByContextID(
	_ context.Context,
	_ uuid.UUID,
	_ matchingRepos.CursorFilter,
) ([]*matchingEntities.Adjustment, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (stub *stubAdjustmentRepo) ListByMatchGroupID(
	_ context.Context,
	_, _ uuid.UUID,
) ([]*matchingEntities.Adjustment, error) {
	return nil, nil
}

// stubTransactionRepo is a test stub for TransactionRepository.
type stubTransactionRepo struct {
	markMatchedErr    error
	markMatchedCalled bool
	contextID         uuid.UUID
	transactionIDs    []uuid.UUID

	markMatchedWithTxErr   error
	markMatchedWithTxCalls []markMatchedWithTxCall

	withTxErr   error
	withTxCalls []withTxCall
}

type markMatchedWithTxCall struct {
	contextID      uuid.UUID
	transactionIDs []uuid.UUID
}

type withTxCall struct {
	hasFn bool
}

func (stub *stubTransactionRepo) ListUnmatchedByContext(
	_ context.Context,
	_ uuid.UUID,
	_, _ *time.Time,
	_, _ int,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (stub *stubTransactionRepo) FindByContextAndIDs(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) ([]*shared.Transaction, error) {
	return nil, nil
}

func (stub *stubTransactionRepo) MarkMatched(
	_ context.Context,
	contextID uuid.UUID,
	txIDs []uuid.UUID,
) error {
	stub.markMatchedCalled = true
	stub.contextID = contextID
	stub.transactionIDs = txIDs

	return stub.markMatchedErr
}

func (stub *stubTransactionRepo) MarkMatchedWithTx(
	_ context.Context,
	_ matchingRepos.Tx,
	contextID uuid.UUID,
	transactionIDs []uuid.UUID,
) error {
	stub.markMatchedWithTxCalls = append(stub.markMatchedWithTxCalls, markMatchedWithTxCall{
		contextID:      contextID,
		transactionIDs: append([]uuid.UUID(nil), transactionIDs...),
	})

	if stub.markMatchedWithTxErr != nil {
		return stub.markMatchedWithTxErr
	}

	return nil
}

func (stub *stubTransactionRepo) MarkPendingReview(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (stub *stubTransactionRepo) MarkPendingReviewWithTx(
	_ context.Context,
	_ matchingRepos.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (stub *stubTransactionRepo) MarkUnmatched(
	_ context.Context,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (stub *stubTransactionRepo) MarkUnmatchedWithTx(
	_ context.Context,
	_ matchingRepos.Tx,
	_ uuid.UUID,
	_ []uuid.UUID,
) error {
	return nil
}

func (stub *stubTransactionRepo) WithTx(_ context.Context, fn func(matchingRepos.Tx) error) error {
	stub.withTxCalls = append(stub.withTxCalls, withTxCall{hasFn: fn != nil})

	if stub.withTxErr != nil {
		return stub.withTxErr
	}

	if fn == nil {
		return nil
	}

	return fn(nil)
}

// stubContextLookup is a test stub for ExceptionContextLookup.
type stubContextLookup struct {
	contextID uuid.UUID
	err       error
}

func (stub *stubContextLookup) GetContextIDByTransactionID(
	_ context.Context,
	_ uuid.UUID,
) (uuid.UUID, error) {
	return stub.contextID, stub.err
}

func TestNewExceptionMatchingGateway(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: uuid.New()}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)

	require.NoError(t, err)
	assert.NotNil(t, gateway)
}

func TestNewExceptionMatchingGateway_NilAdjustmentRepo(t *testing.T) {
	t.Parallel()

	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: uuid.New()}

	gateway, err := NewExceptionMatchingGateway(nil, txRepo, lookup)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilAdjustmentRepository)
	assert.Nil(t, gateway)
}

func TestNewExceptionMatchingGateway_NilTransactionRepo(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	lookup := &stubContextLookup{contextID: uuid.New()}

	gateway, err := NewExceptionMatchingGateway(adjRepo, nil, lookup)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilTransactionRepository)
	assert.Nil(t, gateway)
}

func TestNewExceptionMatchingGateway_NilContextLookup(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilContextLookup)
	assert.Nil(t, gateway)
}

func TestExceptionMatchingGateway_CreateForceMatch_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	transactionID := uuid.New()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: contextID}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{
		ExceptionID:    uuid.New(),
		TransactionID:  transactionID,
		Notes:          "Test force match",
		OverrideReason: "POLICY_EXCEPTION",
		Actor:          "test@example.com",
	})

	require.NoError(t, err)
	assert.True(t, txRepo.markMatchedCalled)
	assert.Equal(t, contextID, txRepo.contextID)
	assert.Contains(t, txRepo.transactionIDs, transactionID)
}

func TestExceptionMatchingGateway_CreateForceMatch_ContextLookupError(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{err: errLookupFailed}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{
		ExceptionID:    uuid.New(),
		TransactionID:  uuid.New(),
		Notes:          "Test force match",
		OverrideReason: "POLICY_EXCEPTION",
		Actor:          "test@example.com",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve context ID")
}

func TestExceptionMatchingGateway_CreateForceMatch_MarkMatchedError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{markMatchedErr: errDBError}
	lookup := &stubContextLookup{contextID: contextID}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateForceMatch(context.Background(), exceptionPorts.ForceMatchInput{
		ExceptionID:    uuid.New(),
		TransactionID:  uuid.New(),
		Notes:          "Test force match",
		OverrideReason: "POLICY_EXCEPTION",
		Actor:          "test@example.com",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "mark transaction matched")
}

func TestExceptionMatchingGateway_CreateAdjustment_Success(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	transactionID := uuid.New()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: contextID}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		ExceptionID:   uuid.New(),
		TransactionID: transactionID,
		Amount:        decimal.NewFromFloat(100.50),
		Currency:      "USD",
		Direction:     "DEBIT",
		Reason:        "AMOUNT_CORRECTION",
		Notes:         "Test adjustment",
		Actor:         "test@example.com",
	})

	require.NoError(t, err)
	require.NotNil(t, adjRepo.created)
	assert.Equal(t, contextID, adjRepo.created.ContextID)
	assert.Equal(t, transactionID, *adjRepo.created.TransactionID)
	assert.True(t, adjRepo.created.Amount.Equal(decimal.NewFromFloat(100.50)))
	assert.Equal(t, "USD", adjRepo.created.Currency)
}

func TestExceptionMatchingGateway_CreateAdjustment_ContextLookupError(t *testing.T) {
	t.Parallel()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{err: errLookupFailed}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		ExceptionID:   uuid.New(),
		TransactionID: uuid.New(),
		Amount:        decimal.NewFromFloat(100.50),
		Currency:      "USD",
		Direction:     "DEBIT",
		Reason:        "AMOUNT_CORRECTION",
		Notes:         "Test adjustment",
		Actor:         "test@example.com",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve context ID")
}

func TestExceptionMatchingGateway_CreateAdjustment_CreateError(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	adjRepo := &stubAdjustmentRepo{err: errDBError}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: contextID}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		ExceptionID:   uuid.New(),
		TransactionID: uuid.New(),
		Amount:        decimal.NewFromFloat(100.50),
		Currency:      "USD",
		Direction:     "CREDIT",
		Reason:        "AMOUNT_CORRECTION",
		Notes:         "Test adjustment",
		Actor:         "test@example.com",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "persist adjustment")
}

func TestExceptionMatchingGateway_CreateAdjustment_InvalidDirection(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()

	adjRepo := &stubAdjustmentRepo{}
	txRepo := &stubTransactionRepo{}
	lookup := &stubContextLookup{contextID: contextID}

	gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
	require.NoError(t, err)

	err = gateway.CreateAdjustment(context.Background(), exceptionPorts.CreateAdjustmentInput{
		ExceptionID:   uuid.New(),
		TransactionID: uuid.New(),
		Amount:        decimal.NewFromFloat(100.50),
		Currency:      "USD",
		Direction:     "INVALID_DIRECTION",
		Reason:        "AMOUNT_CORRECTION",
		Notes:         "Test adjustment",
		Actor:         "test@example.com",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate direction")
	require.ErrorIs(t, err, ErrInvalidDirection)
}

func TestExceptionMatchingGateway_CreateAdjustment_AllReasonTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		reason string
	}{
		{name: "amount correction", reason: "AMOUNT_CORRECTION"},
		{name: "currency correction", reason: "CURRENCY_CORRECTION"},
		{name: "date correction", reason: "DATE_CORRECTION"},
		{name: "other", reason: "OTHER"},
		{name: "unknown reason", reason: "UNKNOWN_REASON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			contextID := uuid.New()
			transactionID := uuid.New()

			adjRepo := &stubAdjustmentRepo{}
			txRepo := &stubTransactionRepo{}
			lookup := &stubContextLookup{contextID: contextID}

			gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
			require.NoError(t, err)

			err = gateway.CreateAdjustment(
				context.Background(),
				exceptionPorts.CreateAdjustmentInput{
					ExceptionID:   uuid.New(),
					TransactionID: transactionID,
					Amount:        decimal.NewFromFloat(50.00),
					Currency:      "EUR",
					Direction:     "DEBIT",
					Reason:        tt.reason,
					Notes:         "Test adjustment with " + tt.reason,
					Actor:         "test@example.com",
				},
			)

			require.NoError(t, err)
			require.NotNil(t, adjRepo.created)
			assert.Equal(t, contextID, adjRepo.created.ContextID)
			assert.Equal(t, tt.reason, adjRepo.created.Reason)
		})
	}
}

func TestStubTransactionRepo_MarkMatchedWithTx_ErrorTracksCall(t *testing.T) {
	t.Parallel()

	contextID := uuid.New()
	transactionIDs := []uuid.UUID{uuid.New(), uuid.New()}
	txRepo := &stubTransactionRepo{markMatchedWithTxErr: errDBError}

	err := txRepo.MarkMatchedWithTx(context.Background(), nil, contextID, transactionIDs)

	require.ErrorIs(t, err, errDBError)
	require.Len(t, txRepo.markMatchedWithTxCalls, 1)
	assert.Equal(t, contextID, txRepo.markMatchedWithTxCalls[0].contextID)
	assert.Equal(t, transactionIDs, txRepo.markMatchedWithTxCalls[0].transactionIDs)
}

func TestStubTransactionRepo_WithTx_ErrorTracksCall(t *testing.T) {
	t.Parallel()

	txRepo := &stubTransactionRepo{withTxErr: errDBError}

	err := txRepo.WithTx(context.Background(), func(matchingRepos.Tx) error {
		return nil
	})

	require.ErrorIs(t, err, errDBError)
	require.Len(t, txRepo.withTxCalls, 1)
	assert.True(t, txRepo.withTxCalls[0].hasFn)
}

func TestStubTransactionRepo_WithTx_CallsFn(t *testing.T) {
	t.Parallel()

	txRepo := &stubTransactionRepo{}
	called := false

	err := txRepo.WithTx(context.Background(), func(matchingRepos.Tx) error {
		called = true
		return nil
	})

	require.NoError(t, err)
	require.Len(t, txRepo.withTxCalls, 1)
	assert.True(t, txRepo.withTxCalls[0].hasFn)
	assert.True(t, called)
}

func TestMapReasonToAdjustmentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		reason   string
		expected matchingEntities.AdjustmentType
	}{
		{
			name:     "amount correction",
			reason:   "AMOUNT_CORRECTION",
			expected: matchingEntities.AdjustmentTypeMiscellaneous,
		},
		{
			name:     "currency correction",
			reason:   "CURRENCY_CORRECTION",
			expected: matchingEntities.AdjustmentTypeFXDifference,
		},
		{
			name:     "date correction",
			reason:   "DATE_CORRECTION",
			expected: matchingEntities.AdjustmentTypeMiscellaneous,
		},
		{
			name:     "other",
			reason:   "OTHER",
			expected: matchingEntities.AdjustmentTypeMiscellaneous,
		},
		{
			name:     "unknown",
			reason:   "UNKNOWN_REASON",
			expected: matchingEntities.AdjustmentTypeMiscellaneous,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := mapReasonToAdjustmentType(tt.reason)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExceptionMatchingGateway_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ exceptionPorts.MatchingGateway = (*ExceptionMatchingGateway)(nil)
}

func TestExceptionMatchingGateway_CreateAdjustment_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		amount    float64
		currency  string
		direction string
		reason    string
		notes     string
		actor     string
	}{
		{
			name:      "negative amount",
			amount:    -100.50,
			currency:  "USD",
			direction: "DEBIT",
			reason:    "AMOUNT_CORRECTION",
			notes:     "Test adjustment",
			actor:     "test@example.com",
		},
		{
			name:      "empty currency",
			amount:    100.50,
			currency:  "",
			direction: "DEBIT",
			reason:    "AMOUNT_CORRECTION",
			notes:     "Test adjustment",
			actor:     "test@example.com",
		},
		{
			name:      "empty reason",
			amount:    100.50,
			currency:  "USD",
			direction: "DEBIT",
			reason:    "",
			notes:     "Test adjustment",
			actor:     "test@example.com",
		},
		{
			name:      "empty actor",
			amount:    100.50,
			currency:  "USD",
			direction: "CREDIT",
			reason:    "AMOUNT_CORRECTION",
			notes:     "Test adjustment",
			actor:     "",
		},
		{
			name:      "empty notes",
			amount:    100.50,
			currency:  "USD",
			direction: "CREDIT",
			reason:    "AMOUNT_CORRECTION",
			notes:     "",
			actor:     "test@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			contextID := uuid.New()

			adjRepo := &stubAdjustmentRepo{}
			txRepo := &stubTransactionRepo{}
			lookup := &stubContextLookup{contextID: contextID}

			gateway, err := NewExceptionMatchingGateway(adjRepo, txRepo, lookup)
			require.NoError(t, err)

			err = gateway.CreateAdjustment(
				context.Background(),
				exceptionPorts.CreateAdjustmentInput{
					ExceptionID:   uuid.New(),
					TransactionID: uuid.New(),
					Amount:        decimal.NewFromFloat(tt.amount),
					Currency:      tt.currency,
					Direction:     tt.direction,
					Reason:        tt.reason,
					Notes:         tt.notes,
					Actor:         tt.actor,
				},
			)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "create adjustment entity")
		})
	}
}
