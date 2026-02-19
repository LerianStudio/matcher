//go:build unit

package resolution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/repositories"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
)

// Static errors for testing (err113 compliance).
var (
	errNotFound    = errors.New("not found")
	errGatewayFail = errors.New("gateway error")
)

// stubExceptionRepo is a test stub for ExceptionRepository.
type stubExceptionRepo struct {
	exception *entities.Exception
	err       error
}

var _ repositories.ExceptionRepository = (*stubExceptionRepo)(nil)

func (s *stubExceptionRepo) FindByID(_ context.Context, _ uuid.UUID) (*entities.Exception, error) {
	return s.exception, s.err
}

func (s *stubExceptionRepo) List(
	_ context.Context,
	_ repositories.ExceptionFilter,
	_ repositories.CursorFilter,
) ([]*entities.Exception, libHTTP.CursorPagination, error) {
	return nil, libHTTP.CursorPagination{}, nil
}

func (s *stubExceptionRepo) Update(
	_ context.Context,
	_ *entities.Exception,
) (*entities.Exception, error) {
	return nil, nil
}

func (s *stubExceptionRepo) UpdateWithTx(
	ctx context.Context,
	_ repositories.Tx,
	e *entities.Exception,
) (*entities.Exception, error) {
	return s.Update(ctx, e) // delegate to existing method
}

// stubMatchingGateway is a test stub for MatchingGateway.
type stubMatchingGateway struct {
	forceMatchErr  error
	adjustmentErr  error
	forceMatchCall *ports.ForceMatchInput
	adjustmentCall *ports.CreateAdjustmentInput
}

func (s *stubMatchingGateway) CreateForceMatch(
	_ context.Context,
	input ports.ForceMatchInput,
) error {
	s.forceMatchCall = &input
	return s.forceMatchErr
}

func (s *stubMatchingGateway) CreateAdjustment(
	_ context.Context,
	input ports.CreateAdjustmentInput,
) error {
	s.adjustmentCall = &input
	return s.adjustmentErr
}

// stubActorExtractor is a test stub for ActorExtractor.
type stubActorExtractor struct {
	actor string
}

func (s *stubActorExtractor) GetActor(_ context.Context) string {
	return s.actor
}

func createTestException(t *testing.T) *entities.Exception {
	t.Helper()

	return &entities.Exception{
		ID:            uuid.New(),
		TransactionID: uuid.New(),
		Severity:      value_objects.ExceptionSeverityMedium,
		Status:        value_objects.ExceptionStatusOpen,
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
	}
}

func TestNewExecutor(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)

	require.NoError(t, err)
	assert.NotNil(t, executor)
}

func TestNewExecutor_NilExceptionRepo(t *testing.T) {
	t.Parallel()

	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(nil, gateway, actor)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, executor)
}

func TestNewExecutor_NilMatchingGateway(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, nil, actor)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilMatchingGateway)
	assert.Nil(t, executor)
}

func TestNewExecutor_NilActorExtractor(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	gateway := &stubMatchingGateway{}

	executor, err := NewExecutor(repo, gateway, nil)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrNilActorExtractor)
	assert.Nil(t, executor)
}

func TestForceMatch_Success(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.ForceMatch(
		context.Background(),
		exception.ID,
		"Test notes",
		value_objects.OverrideReasonPolicyException,
	)

	require.NoError(t, err)
	require.NotNil(t, gateway.forceMatchCall)
	assert.Equal(t, exception.ID, gateway.forceMatchCall.ExceptionID)
	assert.Equal(t, exception.TransactionID, gateway.forceMatchCall.TransactionID)
	assert.Equal(t, "Test notes", gateway.forceMatchCall.Notes)
	assert.Equal(
		t,
		string(value_objects.OverrideReasonPolicyException),
		gateway.forceMatchCall.OverrideReason,
	)
	assert.Equal(t, "test@example.com", gateway.forceMatchCall.Actor)
}

func TestForceMatch_ExceptionNotFound(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{err: errNotFound}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		err = executor.ForceMatch(
			context.Background(),
			uuid.New(),
			"Test notes",
			value_objects.OverrideReasonPolicyException,
		)
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "find exception")
}

func TestForceMatch_NilException(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{exception: nil}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.ForceMatch(
		context.Background(),
		uuid.New(),
		"Test notes",
		value_objects.OverrideReasonPolicyException,
	)

	require.Error(t, err)
	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
}

func TestForceMatch_GatewayError(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{forceMatchErr: errGatewayFail}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.ForceMatch(
		context.Background(),
		exception.ID,
		"Test notes",
		value_objects.OverrideReasonPolicyException,
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create force match")
}

func TestForceMatch_AllOverrideReasons(t *testing.T) {
	t.Parallel()

	reasons := []value_objects.OverrideReason{
		value_objects.OverrideReasonPolicyException,
		value_objects.OverrideReasonOpsApproval,
		value_objects.OverrideReasonCustomerDispute,
	}

	for _, reason := range reasons {
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()

			exception := createTestException(t)
			repo := &stubExceptionRepo{exception: exception}
			gateway := &stubMatchingGateway{}
			actor := &stubActorExtractor{actor: "test@example.com"}

			executor, err := NewExecutor(repo, gateway, actor)
			require.NoError(t, err)

			err = executor.ForceMatch(
				context.Background(),
				exception.ID,
				"Test notes for "+string(reason),
				reason,
			)

			require.NoError(t, err)
			require.NotNil(t, gateway.forceMatchCall)
			assert.Equal(t, string(reason), gateway.forceMatchCall.OverrideReason)
		})
	}
}

func TestAdjustEntry_Success(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.AdjustEntry(
		context.Background(),
		exception.ID,
		ports.AdjustmentInput{
			Amount:      decimal.NewFromFloat(100.50),
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Test adjustment",
		},
	)

	require.NoError(t, err)
	require.NotNil(t, gateway.adjustmentCall)
	assert.Equal(t, exception.ID, gateway.adjustmentCall.ExceptionID)
	assert.Equal(t, exception.TransactionID, gateway.adjustmentCall.TransactionID)
	assert.Equal(t, "DEBIT", gateway.adjustmentCall.Direction)
	assert.True(t, gateway.adjustmentCall.Amount.Equal(decimal.NewFromFloat(100.50)))
	assert.Equal(t, "USD", gateway.adjustmentCall.Currency)
	assert.Equal(
		t,
		string(value_objects.AdjustmentReasonAmountCorrection),
		gateway.adjustmentCall.Reason,
	)
	assert.Equal(t, "Test adjustment", gateway.adjustmentCall.Notes)
	assert.Equal(t, "test@example.com", gateway.adjustmentCall.Actor)
}

func TestAdjustEntry_ZeroAmount(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.AdjustEntry(
		context.Background(),
		exception.ID,
		ports.AdjustmentInput{
			Amount:      decimal.Zero,
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Test adjustment",
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidAdjustment)
}

func TestAdjustEntry_ExceptionNotFound(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{err: errNotFound}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.AdjustEntry(
		context.Background(),
		uuid.New(),
		ports.AdjustmentInput{
			Amount:      decimal.NewFromFloat(100.50),
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Test adjustment",
		},
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "find exception")
}

func TestAdjustEntry_NilException(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{exception: nil}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	err = executor.AdjustEntry(
		context.Background(),
		uuid.New(),
		ports.AdjustmentInput{
			Amount:      decimal.NewFromFloat(100.50),
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Test adjustment",
		},
	)

	require.Error(t, err)
	require.ErrorIs(t, err, entities.ErrExceptionNotFound)
}

func TestAdjustEntry_GatewayError(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{adjustmentErr: errGatewayFail}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		err = executor.AdjustEntry(
			context.Background(),
			exception.ID,
			ports.AdjustmentInput{
				Amount:      decimal.NewFromFloat(100.50),
				Currency:    "USD",
				EffectiveAt: time.Now(),
				Reason:      value_objects.AdjustmentReasonAmountCorrection,
				Notes:       "Test adjustment",
			},
		)
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "create adjustment")
}

func TestAdjustEntry_NegativeAmount(t *testing.T) {
	t.Parallel()

	exception := createTestException(t)
	repo := &stubExceptionRepo{exception: exception}
	gateway := &stubMatchingGateway{}
	actor := &stubActorExtractor{actor: "test@example.com"}

	executor, err := NewExecutor(repo, gateway, actor)
	require.NoError(t, err)

	// Negative amounts should be allowed (for reversals/credits)
	err = executor.AdjustEntry(
		context.Background(),
		exception.ID,
		ports.AdjustmentInput{
			Amount:      decimal.NewFromFloat(-50.00),
			Currency:    "USD",
			EffectiveAt: time.Now(),
			Reason:      value_objects.AdjustmentReasonAmountCorrection,
			Notes:       "Negative adjustment",
		},
	)

	require.NoError(t, err)
	require.NotNil(t, gateway.adjustmentCall)
	assert.Equal(t, "CREDIT", gateway.adjustmentCall.Direction)
}

func TestExecutor_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ ports.ResolutionExecutor = (*Executor)(nil)
}
