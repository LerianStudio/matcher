//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/dispute"
	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
)

func TestCloseDispute_Win(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	result, err := uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "Bank refunded the fee",
		Won:        true,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, dispute.DisputeStateWon, result.State)
	require.NotNil(t, result.Resolution)
	require.Equal(t, "Bank refunded the fee", *result.Resolution)
	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "DISPUTE_WON", audit.lastEvent.Action)
	require.Equal(t, "analyst-1", audit.lastEvent.Actor)
}

func TestCloseDispute_Lose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		infra,
	)
	require.NoError(t, err)

	result, err := uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "Bank denied the dispute",
		Won:        false,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, dispute.DisputeStateLost, result.State)
	require.NotNil(t, result.Resolution)
	require.Equal(t, "Bank denied the dispute", *result.Resolution)
	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "DISPUTE_LOST", audit.lastEvent.Action)
}

func TestCloseDispute_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		cmd         CloseDisputeCommand
		expectedErr error
	}{
		{
			name: "nil dispute id",
			cmd: CloseDisputeCommand{
				DisputeID:  uuid.Nil,
				Resolution: "resolution",
				Won:        true,
			},
			expectedErr: ErrDisputeIDRequired,
		},
		{
			name: "empty resolution",
			cmd: CloseDisputeCommand{
				DisputeID:  existingDispute.ID,
				Resolution: "",
				Won:        true,
			},
			expectedErr: ErrDisputeResolutionRequired,
		},
		{
			name: "whitespace resolution",
			cmd: CloseDisputeCommand{
				DisputeID:  existingDispute.ID,
				Resolution: "   ",
				Won:        true,
			},
			expectedErr: ErrDisputeResolutionRequired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := uc.CloseDispute(ctx, tc.cmd)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestCloseDispute_ActorRequired(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	emptyActor := actorExtractor("")

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		emptyActor,
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(context.Background(), CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "resolution",
		Won:        true,
	})
	require.ErrorIs(t, err, ErrActorRequired)
}

func TestCloseDispute_DisputeNotFound(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{findErr: errTestDisputeFind}
	audit := &stubAuditPublisher{}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  uuid.New(),
		Resolution: "resolution",
		Won:        true,
	})
	require.ErrorIs(t, err, errTestDisputeFind)
}

func TestCloseDispute_InvalidStateTransition(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	require.NoError(t, existingDispute.Win(ctx, "already won"))

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "try to win again",
		Won:        true,
	})
	require.ErrorIs(t, err, dispute.ErrInvalidDisputeTransition)
}

func TestCloseDispute_UpdateError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute, updateErr: errTestDisputeUpdate}
	audit := &stubAuditPublisher{}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "resolution",
		Won:        true,
	})
	require.ErrorIs(t, err, errTestDisputeUpdate)
}

func TestCloseDispute_AuditPublishError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{err: errTestAudit}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  existingDispute.ID,
		Resolution: "resolution",
		Won:        true,
	})
	require.ErrorIs(t, err, errTestAudit)
	require.NotNil(t, audit.lastEvent)
	require.WithinDuration(t, time.Now().UTC(), audit.lastEvent.OccurredAt, time.Second)
}
