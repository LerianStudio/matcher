// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/exception/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	portMocks "github.com/LerianStudio/matcher/internal/exception/ports/mocks"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
)

func TestForceMatch_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")
	ctx := context.Background()
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	// Update to PENDING_RESOLUTION before gateway call.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "resolved after review", gomock.Any()).Return(nil)
	repo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sql.Tx, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})

	var capturedEvent *ports.AuditEvent
	audit.EXPECT().PublishExceptionEventWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sql.Tx, event ports.AuditEvent) error {
			capturedEvent = &event
			return nil
		})

	uc, err := NewExceptionUseCase(repo, actor, audit, infra, WithResolutionExecutor(exec))
	require.NoError(t, err)

	result, err := uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "resolved after review",
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)
	require.NotNil(t, capturedEvent)
	require.Equal(t, "FORCE_MATCH", capturedEvent.Action)
	require.Equal(t, "analyst-1", capturedEvent.Actor)
}

func TestForceMatch_ValidationErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	ctx := context.Background()

	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{ExceptionID: uuid.Nil, Notes: "note", OverrideReason: "POLICY_EXCEPTION"},
	)
	require.ErrorIs(t, err, ErrExceptionIDRequired)

	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{
			ExceptionID:    exception.ID,
			Notes:          " ",
			OverrideReason: "POLICY_EXCEPTION",
		},
	)
	require.ErrorIs(t, err, entities.ErrResolutionNotesRequired)

	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{ExceptionID: exception.ID, Notes: "note", OverrideReason: "BAD"},
	)
	require.ErrorIs(t, err, value_objects.ErrInvalidOverrideReason)
}

func TestForceMatch_ActorRequired(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	emptyActor := actorExtractor("")

	uc, err := NewExceptionUseCase(repo, emptyActor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "resolved after review",
	})
	require.ErrorIs(t, err, ErrActorRequired)

	whitespaceActor := actorExtractor("  ")
	uc2, err := NewExceptionUseCase(repo, whitespaceActor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc2.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "resolved after review",
	})
	require.ErrorIs(t, err, ErrActorRequired)
}

func TestForceMatch_DependencyErrors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	// Three sequential FindByID calls with different return behaviors.
	gomock.InOrder(
		repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(nil, errTestFind),
		repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil),
		repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil),
	)

	// Update to PENDING_RESOLUTION on the 3rd call (before gateway).
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})

	// ForceMatch executor is reached only on the 3rd call.
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "note", gomock.Any()).Return(errTestExecutor)

	// Revert on gateway failure.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	ctx := context.Background()

	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{
			ExceptionID:    exception.ID,
			Notes:          "note",
			OverrideReason: "POLICY_EXCEPTION",
		},
	)
	require.ErrorIs(t, err, errTestFind)

	exception.Status = value_objects.ExceptionStatusResolved
	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{
			ExceptionID:    exception.ID,
			Notes:          "note",
			OverrideReason: "POLICY_EXCEPTION",
		},
	)
	require.ErrorIs(t, err, value_objects.ErrInvalidResolutionTransition)

	exception.Status = value_objects.ExceptionStatusOpen
	_, err = uc.ForceMatch(
		ctx,
		ForceMatchCommand{
			ExceptionID:    exception.ID,
			Notes:          "note",
			OverrideReason: "POLICY_EXCEPTION",
		},
	)
	require.ErrorIs(t, err, errTestExecutor)
}

func TestForceMatch_AuditError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	// Update to PENDING_RESOLUTION.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "note", gomock.Any()).Return(nil)
	repo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sql.Tx, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})

	var capturedEvent *ports.AuditEvent
	audit.EXPECT().PublishExceptionEventWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sql.Tx, event ports.AuditEvent) error {
			capturedEvent = &event
			return errTestAudit
		})

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc.ForceMatch(
		context.Background(),
		ForceMatchCommand{
			ExceptionID:    exception.ID,
			Notes:          "note",
			OverrideReason: "POLICY_EXCEPTION",
		},
	)
	require.ErrorIs(t, err, errTestAudit)
	require.NotNil(t, capturedEvent)
	require.WithinDuration(t, time.Now().UTC(), capturedEvent.OccurredAt, time.Second)
}

func TestForceMatch_AssignedExceptionResolvesFromPending(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	// Assign with a valid assignee.
	require.NoError(t, exception.Assign(context.Background(), "analyst-1", nil))
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")
	ctx := context.Background()

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	// Update to PENDING_RESOLUTION.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "note", gomock.Any()).Return(nil)
	repo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ *sql.Tx, exc *entities.Exception) (*entities.Exception, error) {
			return exc, nil
		})
	audit.EXPECT().PublishExceptionEventWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	uc, err := NewExceptionUseCase(repo, actor, audit, infra, WithResolutionExecutor(exec))
	require.NoError(t, err)

	result, err := uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		Notes:          "note",
		OverrideReason: "POLICY_EXCEPTION",
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)
	// Assignee should be preserved through PENDING_RESOLUTION.
	require.NotNil(t, result.AssignedTo)
	require.Equal(t, "analyst-1", *result.AssignedTo)
}

func TestNewUseCase_Validations(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	actor := actorExtractor("analyst-1")
	infra := &stubInfraProvider{}

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)

	_, err := NewExceptionUseCase(nil, actor, audit, infra, WithResolutionExecutor(exec))
	require.ErrorIs(t, err, ErrNilExceptionRepository)

	// The resolution executor is optional at construction time; its
	// absence now surfaces when a resolution operation (ForceMatch) is
	// called without having wired the executor via the option.
	ucWithoutExec, err := NewExceptionUseCase(repo, actor, audit, infra)
	require.NoError(t, err)

	_, err = ucWithoutExec.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    uuid.New(),
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "test",
	})
	require.ErrorIs(t, err, ErrNilResolutionExecutor)

	_, err = NewExceptionUseCase(repo, actor, nil, infra, WithResolutionExecutor(exec))
	require.ErrorIs(t, err, ErrNilAuditPublisher)

	_, err = NewExceptionUseCase(repo, nil, audit, infra, WithResolutionExecutor(exec))
	require.ErrorIs(t, err, ErrNilActorExtractor)

	_, err = NewExceptionUseCase(repo, actor, audit, nil, WithResolutionExecutor(exec))
	require.ErrorIs(t, err, ErrNilInfraProvider)
}

func TestForceMatch_AllOverrideReasons(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		reason string
	}{
		{name: "PolicyException", reason: "POLICY_EXCEPTION"},
		{name: "OpsApproval", reason: "OPS_APPROVAL"},
		{name: "CustomerDispute", reason: "CUSTOMER_DISPUTE"},
		{name: "DataCorrection", reason: "DATA_CORRECTION"},
		{name: "LowercaseNormalized", reason: "policy_exception"},
		{name: "MixedCaseNormalized", reason: "Ops_Approval"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)

			exception, err := entities.NewException(
				context.Background(),
				uuid.New(),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			repo := repoMocks.NewMockExceptionRepository(ctrl)
			exec := portMocks.NewMockResolutionExecutor(ctrl)
			audit := portMocks.NewMockAuditPublisher(ctrl)
			actor := actorExtractor("analyst-1")

			repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
			// Update to PENDING_RESOLUTION.
			repo.EXPECT().Update(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
					return exc, nil
				})
			exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, gomock.Any(), gomock.Any()).Return(nil)
			repo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *sql.Tx, exc *entities.Exception) (*entities.Exception, error) {
					return exc, nil
				})

			var capturedEvent *ports.AuditEvent
			audit.EXPECT().PublishExceptionEventWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *sql.Tx, event ports.AuditEvent) error {
					capturedEvent = &event
					return nil
				})

			uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
			require.NoError(t, err)

			result, err := uc.ForceMatch(context.Background(), ForceMatchCommand{
				ExceptionID:    exception.ID,
				OverrideReason: tc.reason,
				Notes:          "resolved for reason: " + tc.reason,
			})
			require.NoError(t, err)
			require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)
			require.NotNil(t, capturedEvent)
			require.Equal(t, "FORCE_MATCH", capturedEvent.Action)
		})
	}
}

func TestForceMatch_PendingResolutionRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	// Direct field mutation: simulating an already-PENDING_RESOLUTION exception
	// that can't be reached via domain methods in a unit test context (would require
	// a concurrent gateway call to be in-flight). This tests the guard's rejection path.
	exception.Status = value_objects.ExceptionStatusPendingResolution

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	// StartResolution should fail, so no executor call, no update, no audit.

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "should fail",
	})
	require.ErrorIs(t, err, entities.ErrExceptionPendingResolution)
}

func TestForceMatch_GatewayFailureRevertsStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	// First call: FindByID returns OPEN exception.
	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)

	// Update to PENDING_RESOLUTION.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusPendingResolution, exc.Status)
			return exc, nil
		})

	// Gateway call fails.
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "note", gomock.Any()).Return(errTestExecutor)

	// Revert: Update back to OPEN.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusOpen, exc.Status)
			return exc, nil
		})

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "note",
	})
	require.ErrorIs(t, err, errTestExecutor)
	require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
}

func TestForceMatch_GatewayFailureRevertsAssignedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, exception.Assign(context.Background(), "analyst-1", nil))
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)

	// Update to PENDING_RESOLUTION.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusPendingResolution, exc.Status)
			return exc, nil
		})

	// Gateway fails.
	exec.EXPECT().ForceMatch(gomock.Any(), exception.ID, "note", gomock.Any()).Return(errTestExecutor)

	// Revert: Update back to ASSIGNED.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusAssigned, exc.Status)
			return exc, nil
		})

	uc, err := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
	require.NoError(t, err)

	_, err = uc.ForceMatch(context.Background(), ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "note",
	})
	require.ErrorIs(t, err, errTestExecutor)
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
}

func TestForceMatch_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	const numGoroutines = 10

	ctx := context.Background()
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			newException, innerErr := entities.NewException(
				ctx,
				uuid.New(),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			if innerErr != nil {
				errChan <- innerErr
				return
			}

			ctrl := gomock.NewController(t)

			repo := repoMocks.NewMockExceptionRepository(ctrl)
			exec := portMocks.NewMockResolutionExecutor(ctrl)
			audit := portMocks.NewMockAuditPublisher(ctrl)
			actor := actorExtractor("analyst-1")

			repo.EXPECT().FindByID(gomock.Any(), newException.ID).Return(newException, nil)
			// Update to PENDING_RESOLUTION.
			repo.EXPECT().Update(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
					return exc, nil
				})
			exec.EXPECT().ForceMatch(gomock.Any(), newException.ID, "concurrent resolution", gomock.Any()).Return(nil)
			repo.EXPECT().UpdateWithTx(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, _ *sql.Tx, exc *entities.Exception) (*entities.Exception, error) {
					return exc, nil
				})
			audit.EXPECT().PublishExceptionEventWithTx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

			localUC, innerErr := NewExceptionUseCase(repo, actor, audit, &stubInfraProvider{}, WithResolutionExecutor(exec))
			if innerErr != nil {
				errChan <- innerErr
				return
			}

			_, innerErr = localUC.ForceMatch(ctx, ForceMatchCommand{
				ExceptionID:    newException.ID,
				OverrideReason: "POLICY_EXCEPTION",
				Notes:          "concurrent resolution",
			})
			errChan <- innerErr
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		err := <-errChan
		require.NoError(t, err)
	}

	close(errChan)
}
