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

func TestSubmitEvidence_Success(t *testing.T) {
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

	result, err := uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "Here is my evidence",
		FileURL:   nil,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Evidence, 1)
	require.Equal(t, "Here is my evidence", result.Evidence[0].Comment)
	require.NotNil(t, audit.lastEvent)
	require.Equal(t, "EVIDENCE_SUBMITTED", audit.lastEvent.Action)
	require.Equal(t, "analyst-1", audit.lastEvent.Actor)
}

func TestSubmitEvidence_WithFileURL(t *testing.T) {
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

	fileURL := "https://example.com/evidence.pdf"
	result, err := uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "Evidence with file",
		FileURL:   &fileURL,
	})

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result.Evidence, 1)
	require.NotNil(t, result.Evidence[0].FileURL)
	require.Equal(t, fileURL, *result.Evidence[0].FileURL)
	require.NotNil(t, audit.lastEvent)
	require.Equal(t, fileURL, audit.lastEvent.Metadata["file_url"])
}

func TestSubmitEvidence_ValidationErrors(t *testing.T) {
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
		cmd         SubmitEvidenceCommand
		expectedErr error
	}{
		{
			name:        "nil dispute id",
			cmd:         SubmitEvidenceCommand{DisputeID: uuid.Nil, Comment: "comment"},
			expectedErr: ErrDisputeIDRequired,
		},
		{
			name:        "empty comment",
			cmd:         SubmitEvidenceCommand{DisputeID: existingDispute.ID, Comment: ""},
			expectedErr: ErrDisputeCommentRequired,
		},
		{
			name:        "whitespace comment",
			cmd:         SubmitEvidenceCommand{DisputeID: existingDispute.ID, Comment: "   "},
			expectedErr: ErrDisputeCommentRequired,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := uc.SubmitEvidence(ctx, tc.cmd)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestSubmitEvidence_ActorRequired(t *testing.T) {
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

	_, err = uc.SubmitEvidence(context.Background(), SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "comment",
	})
	require.ErrorIs(t, err, ErrActorRequired)
}

func TestSubmitEvidence_DisputeNotFound(t *testing.T) {
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
	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: uuid.New(),
		Comment:   "comment",
	})
	require.ErrorIs(t, err, errTestDisputeFind)
}

func TestSubmitEvidence_WrongState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	exceptionID := uuid.New()
	existingDispute := createTestDispute(ctx, t, exceptionID)

	require.NoError(t, existingDispute.Win(ctx, "won the dispute"))

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

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "comment",
	})
	require.ErrorIs(t, err, dispute.ErrCannotAddEvidenceInCurrentState)
}

func TestSubmitEvidence_UpdateError(t *testing.T) {
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

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "comment",
	})
	require.ErrorIs(t, err, errTestDisputeUpdate)
}

func TestSubmitEvidence_AuditPublishError(t *testing.T) {
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

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "comment",
	})
	require.ErrorIs(t, err, errTestAudit)
	require.NotNil(t, audit.lastEvent)
	require.WithinDuration(t, time.Now().UTC(), audit.lastEvent.OccurredAt, time.Second)
}
