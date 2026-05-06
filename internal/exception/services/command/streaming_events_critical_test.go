// Copyright 2026 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	tmcore "github.com/LerianStudio/lib-commons/v5/commons/tenant-manager/core"
	streaming "github.com/LerianStudio/lib-streaming"
	"github.com/shopspring/decimal"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/services"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	portsmocks "github.com/LerianStudio/matcher/internal/exception/ports/mocks"
	"github.com/LerianStudio/matcher/internal/streaming/emission"
)

type failingExceptionEmitter struct {
	err error
}

func (emitter failingExceptionEmitter) Emit(context.Context, streaming.EmitRequest) error {
	return emitter.err
}

func (emitter failingExceptionEmitter) Close() error { return nil }

func (emitter failingExceptionEmitter) Healthy(context.Context) error { return nil }

func TestEmitExceptionCriticalNilEmitterFails(t *testing.T) {
	uc := &ExceptionUseCase{}
	exception := newOpenException(t)

	err := uc.emitExceptionCritical(testStreamingContext(), nil, nil, "exception.resolved", exception, nil)

	require.ErrorIs(t, err, emission.ErrCriticalOutboxTxRequired)
}

func TestResolveSingleRollsBackOnStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	tenantID := "018f4f95-0000-7000-8000-000000000001"
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), tenantID)
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	repo := &stubExceptionRepo{}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		repo,
		actorExtractor("analyst"),
		audit,
		provider,
		WithResolutionExecutor(&stubResolutionExecutor{}),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	err = uc.resolveSingle(ctx, map[uuid.UUID]*entities.Exception{exception.ID: exception}, exception.ID, "ACCEPTED", "matched manually", "analyst")

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, audit.getCallCount())
	assert.NoError(t, mock.ExpectationsWereMet())
	// TODO(streaming): resolveSingle has no pre-tx side effects and no
	// external connector call, so tx rollback alone is sufficient
	// compensation for streaming failure. No additional compensating action
	// to assert.
}

func TestAdjustEntryRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	repo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		repo,
		actorExtractor("analyst"),
		audit,
		provider,
		WithResolutionExecutor(&stubResolutionExecutor{}),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "adjust through ledger",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, repo.updateWithTxCalls)
	assert.Same(t, tx, repo.updateWithTxTx)
	assert.Equal(t, 1, audit.getCallCount())
	assert.NoError(t, mock.ExpectationsWereMet())
	// TODO(streaming): AdjustEntry persists the PENDING_RESOLUTION transition
	// via a non-tx Update() BEFORE BeginTx (see adjust_entry_commands.go
	// processAdjustEntry, ~line 171). When streaming fails inside the tx,
	// only the second update (Resolved) is rolled back — the first
	// PENDING_RESOLUTION write is left orphaned. The existing
	// executeWithRevert helper compensates only for resolutionExecutor
	// failures, not for streaming failures. Consider extending revert to
	// fire on tx-level failures as well, or keying both writes off the
	// same tx.
}

func TestForceMatchRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	repo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		repo,
		actorExtractor("analyst"),
		audit,
		provider,
		WithResolutionExecutor(&stubResolutionExecutor{}),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	_, err = uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    exception.ID,
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "force match after review",
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, repo.updateWithTxCalls)
	assert.Same(t, tx, repo.updateWithTxTx)
	assert.Equal(t, 1, audit.getCallCount())
	assert.NoError(t, mock.ExpectationsWereMet())
	// TODO(streaming): ForceMatch shares the same orphaned-PENDING_RESOLUTION
	// hazard as AdjustEntry — the pre-tx Update() write is not rolled back
	// when streaming fails inside the tx. See the AdjustEntry test above
	// for the full rationale. Same compensation gap; same suggested fix.
}

func TestProcessCallbackRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	repo := &stubExceptionRepo{exception: exception}
	audit := &stubAuditPublisher{}
	idempotencyRepo := &stubIdempotencyRepo{acquired: true}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		repo,
		actorExtractor("system"),
		audit,
		provider,
		WithIdempotencyRepository(idempotencyRepo),
		WithCallbackRateLimiter(&stubCallbackRateLimiter{allowed: true}),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	err = uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "jira:MATCH-123:callback",
		ExceptionID:     exception.ID,
		CallbackType:    "jira",
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "RESOLVED",
		ResolutionNotes: "resolved by JIRA",
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, repo.updateWithTxCalls)
	assert.Same(t, tx, repo.updateWithTxTx)
	assert.Equal(t, 1, audit.getCallCount())
	assert.Equal(t, 1, idempotencyRepo.markFailedCalls)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestOpenDisputeRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	disputeRepo := &stubDisputeRepo{}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		&stubExceptionRepo{exception: exception},
		actorExtractor("analyst"),
		audit,
		provider,
		WithDisputeRepository(disputeRepo),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	_, err = uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: exception.ID,
		Category:    "BANK_FEE_ERROR",
		Description: "incorrect fee charged",
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, disputeRepo.createWithTxCalls)
	assert.Same(t, tx, disputeRepo.createWithTxTx)
	assert.Equal(t, 1, audit.getCallCount())
	assert.NoError(t, mock.ExpectationsWereMet())
	// TODO(streaming): OpenDispute is fully transactional — the dispute
	// row create, audit publish, and streaming emit all live inside the
	// same tx. Rollback alone is sufficient compensation; no external
	// side-effects to undo.
}

func TestCloseDisputeRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	testCases := []struct {
		name string
		won  bool
	}{
		{name: "won", won: true},
		{name: "lost", won: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			streamErr := errors.New("streaming outbox write failed")
			ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
			tx, mock, err := newMockTx(ctx)
			require.NoError(t, err)
			mock.ExpectRollback()

			existingDispute := createTestDispute(ctx, t, uuid.New())
			disputeRepo := &stubDisputeRepo{dispute: existingDispute}
			audit := &stubAuditPublisher{}
			provider := &stubInfraProvider{tx: tx}
			uc, err := NewExceptionUseCase(
				&stubExceptionRepo{exception: newOpenException(t)},
				actorExtractor("analyst"),
				audit,
				provider,
				WithDisputeRepository(disputeRepo),
				WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
			)
			require.NoError(t, err)

			_, err = uc.CloseDispute(ctx, CloseDisputeCommand{
				DisputeID:  existingDispute.ID,
				Resolution: "bank accepted evidence",
				Won:        tc.won,
			})

			require.ErrorIs(t, err, streamErr)
			assert.Equal(t, int64(1), provider.beginTxCall.Load())
			assert.Equal(t, 1, disputeRepo.findWithTxCalls)
			assert.Same(t, tx, disputeRepo.findWithTxTx)
			assert.Equal(t, 1, disputeRepo.updateWithTxCalls)
			assert.Same(t, tx, disputeRepo.updateWithTxTx)
			assert.Equal(t, 1, audit.getCallCount())
			assert.NoError(t, mock.ExpectationsWereMet())
			// TODO(streaming): CloseDispute is fully transactional — the
			// dispute row update, audit publish, and streaming emit all
			// live inside the same tx. Rollback alone is sufficient
			// compensation.
		})
	}
}

// TestDispatchRollsBackOnCriticalStreamingEmitFailure verifies that when the
// CRITICAL streaming emit fails inside Dispatch's tx, the audit row is rolled
// back and the use case surfaces the streaming error to the caller. Unlike
// AdjustEntry/ForceMatch, Dispatch has NO compensating action for streaming
// failure today: the external connector call has already been delivered and
// cannot be undone, so rollback only protects the audit row from being
// committed without its paired streaming event.
func TestDispatchRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	exception := newOpenException(t)
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	ctrl := gomock.NewController(t)
	finder := portsmocks.NewMockExceptionFinder(ctrl)
	finder.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)

	connector := portsmocks.NewMockExternalConnector(ctrl)
	// External delivery succeeds before the audit/emit tx opens; the
	// streaming failure inside the tx cannot un-deliver this call. The
	// rollback assertion + the absence of a compensation surface is the
	// point of the test.
	connector.EXPECT().
		Dispatch(gomock.Any(), exception.ID.String(), gomock.Any(), gomock.Any()).
		Return(ports.DispatchResult{
			Target:            services.RoutingTargetJira,
			ExternalReference: "JIRA-12345",
			Acknowledged:      true,
		}, nil)

	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		wrapFinder(finder),
		actorExtractor("analyst"),
		audit,
		provider,
		WithExternalConnector(connector),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	_, err = uc.Dispatch(ctx, DispatchCommand{
		ExceptionID:  exception.ID,
		TargetSystem: "JIRA",
		Queue:        "support-queue",
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, audit.getCallCount(), "audit publish runs on the begin'd tx and is rolled back")
	lastEvent := audit.getLastEvent()
	require.NotNil(t, lastEvent)
	assert.Equal(t, "DISPATCH", lastEvent.Action)
	// sqlmock saw Begin + Rollback but NOT Commit — this is the durability
	// guarantee for the audit row.
	assert.NoError(t, mock.ExpectationsWereMet())

	// TODO(streaming): the external dispatch connector has already delivered
	// the work item to JIRA/ServiceNow/etc. before the tx opens; there is no
	// compensating action for a CRITICAL streaming failure today. If the
	// failure mode requires it, consider an outbox-driven cancel/notify flow
	// keyed on result.ExternalReference.
}

func TestSubmitEvidenceRollsBackOnCriticalStreamingEmitFailure(t *testing.T) {
	streamErr := errors.New("streaming outbox write failed")
	ctx := tmcore.ContextWithTenantID(testStreamingContext(), "018f4f95-0000-7000-8000-000000000001")
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	existingDispute := createTestDispute(ctx, t, uuid.New())
	disputeRepo := &stubDisputeRepo{dispute: existingDispute}
	audit := &stubAuditPublisher{}
	provider := &stubInfraProvider{tx: tx}
	uc, err := NewExceptionUseCase(
		&stubExceptionRepo{exception: newOpenException(t)},
		actorExtractor("analyst"),
		audit,
		provider,
		WithDisputeRepository(disputeRepo),
		WithStreamingEmitter(failingExceptionEmitter{err: streamErr}),
	)
	require.NoError(t, err)

	_, err = uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: existingDispute.ID,
		Comment:   "receipt and statement attached",
	})

	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, int64(1), provider.beginTxCall.Load())
	assert.Equal(t, 1, disputeRepo.findWithTxCalls)
	assert.Same(t, tx, disputeRepo.findWithTxTx)
	assert.Equal(t, 1, disputeRepo.updateWithTxCalls)
	assert.Same(t, tx, disputeRepo.updateWithTxTx)
	assert.Equal(t, 1, audit.getCallCount())
	assert.NoError(t, mock.ExpectationsWereMet())
	// TODO(streaming): SubmitEvidence is fully transactional — the dispute
	// row update, audit publish, and streaming emit all live inside the
	// same tx. Rollback alone is sufficient compensation.
}
