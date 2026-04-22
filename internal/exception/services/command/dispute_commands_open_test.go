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
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

func TestOpenDispute_Success(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}
	ctx := context.Background()
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

	result, err := uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "Bank charged incorrect fee",
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, exception.ID, result.ExceptionID)
	require.Equal(t, dispute.DisputeCategoryBankFeeError, result.Category)
	require.Equal(t, dispute.DisputeStateOpen, result.State)
	require.Equal(t, "analyst-1", result.OpenedBy)
	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "DISPUTE_OPENED", audit.lastEvent.Action)
	require.Equal(t, "analyst-1", audit.lastEvent.Actor)
}

func TestOpenDispute_ValidationErrors(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
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

	testCases := []struct {
		name        string
		cmd         OpenDisputeCommand
		expectedErr error
	}{
		{
			name: "nil exception id",
			cmd: OpenDisputeCommand{
				ExceptionID: uuid.Nil,
				Category:    "BANK_FEE_ERROR",
				Description: "desc",
			},
			expectedErr: ErrExceptionIDRequired,
		},
		{
			name: "empty category",
			cmd: OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    "",
				Description: "desc",
			},
			expectedErr: ErrDisputeCategoryRequired,
		},
		{
			name: "whitespace category",
			cmd: OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    "   ",
				Description: "desc",
			},
			expectedErr: ErrDisputeCategoryRequired,
		},
		{
			name: "invalid category",
			cmd: OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    "INVALID",
				Description: "desc",
			},
			expectedErr: dispute.ErrInvalidDisputeCategory,
		},
		{
			name: "empty description",
			cmd: OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    "BANK_FEE_ERROR",
				Description: "",
			},
			expectedErr: ErrDisputeDescriptionRequired,
		},
		{
			name: "whitespace description",
			cmd: OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    "BANK_FEE_ERROR",
				Description: "   ",
			},
			expectedErr: ErrDisputeDescriptionRequired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := uc.OpenDispute(ctx, tc.cmd)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestOpenDispute_ActorRequired(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
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

	_, err = uc.OpenDispute(context.Background(), OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "desc",
	})
	require.ErrorIs(t, err, ErrActorRequired)

	whitespaceActor := actorExtractor("  ")
	uc2, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		whitespaceActor,
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	_, err = uc2.OpenDispute(context.Background(), OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "desc",
	})
	require.ErrorIs(t, err, ErrActorRequired)
}

func TestOpenDispute_ExceptionNotFound(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception, findErr: errTestFind}
	disputeRepo := &stubDisputeRepo{}
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
	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "desc",
	})
	require.ErrorIs(t, err, errTestFind)
}

func TestOpenDispute_RepositoryCreateError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{createErr: errTestDisputeCreate}
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
	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "desc",
	})
	require.ErrorIs(t, err, errTestDisputeCreate)
}

func TestOpenDispute_AuditPublishError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	exceptionRepo := &stubExceptionRepo{exception: exception}
	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{err: errTestAudit}

	uc, err := NewDisputeUseCase(
		disputeRepo,
		exceptionRepo,
		audit,
		actorExtractor("analyst-1"),
		&stubInfraProvider{},
	)
	require.NoError(t, err)

	ctx := context.Background()
	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "desc",
	})
	require.ErrorIs(t, err, errTestAudit)
	require.NotNil(t, audit.lastEvent)
	require.WithinDuration(t, time.Now().UTC(), audit.lastEvent.OccurredAt, time.Second)
}

func TestOpenDispute_AllCategories(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		category string
		expected dispute.DisputeCategory
	}{
		{
			name:     "BankFeeError",
			category: "BANK_FEE_ERROR",
			expected: dispute.DisputeCategoryBankFeeError,
		},
		{
			name:     "UnrecognizedCharge",
			category: "UNRECOGNIZED_CHARGE",
			expected: dispute.DisputeCategoryUnrecognizedCharge,
		},
		{
			name:     "DuplicateTransaction",
			category: "DUPLICATE_TRANSACTION",
			expected: dispute.DisputeCategoryDuplicateTransaction,
		},
		{name: "Other", category: "OTHER", expected: dispute.DisputeCategoryOther},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				uuid.New(),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			exceptionRepo := &stubExceptionRepo{exception: exception}
			disputeRepo := &stubDisputeRepo{}
			audit := &stubAuditPublisher{}
			ctx := context.Background()
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

			result, err := uc.OpenDispute(ctx, OpenDisputeCommand{
				ExceptionID: exception.ID,
				Category:    tc.category,
				Description: "test description",
			})

			require.NoError(t, err)
			require.Equal(t, tc.expected, result.Category)
		})
	}
}
