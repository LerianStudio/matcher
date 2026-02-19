//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	repoMocks "github.com/LerianStudio/matcher/internal/exception/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/exception/ports"
	portMocks "github.com/LerianStudio/matcher/internal/exception/ports/mocks"
)

// fixedTestTime returns a deterministic timestamp for test reproducibility.
func fixedTestTime() time.Time {
	return time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)
}

// capturingResolutionExecutor records the AdjustEntry call arguments for assertion.
type capturingResolutionExecutor struct {
	stubResolutionExecutor
	capturedExceptionID uuid.UUID
	capturedInput       ports.AdjustmentInput
}

func (exec *capturingResolutionExecutor) AdjustEntry(
	_ context.Context,
	exceptionID uuid.UUID,
	input ports.AdjustmentInput,
) error {
	exec.capturedExceptionID = exceptionID
	exec.capturedInput = input

	return exec.adjustEntryErr
}

// TestAdjustEntry_ValidationOrder verifies that validateAdjustEntry checks
// fields in a deterministic order: dependencies -> ExceptionID -> Actor ->
// Notes -> Amount -> Currency -> ReasonCode. When multiple fields are invalid,
// the first in sequence is the one that surfaces.
func TestAdjustEntry_ValidationOrder(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		actor       string
		cmd         AdjustEntryCommand
		expectedErr error
	}{
		{
			name:  "ExceptionIDCheckedBeforeActor",
			actor: "", // also invalid
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.Nil,
				Notes:       "",
				Amount:      decimal.Zero,
				Currency:    "XXX",
				ReasonCode:  "BAD",
			},
			expectedErr: ErrExceptionIDRequired,
		},
		{
			name:  "ActorCheckedBeforeNotes",
			actor: "", // empty actor (second check)
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.New(),
				Notes:       "",
				Amount:      decimal.Zero,
				Currency:    "XXX",
				ReasonCode:  "BAD",
			},
			expectedErr: ErrActorRequired,
		},
		{
			name:  "NotesCheckedBeforeAmount",
			actor: "analyst",
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.New(),
				Notes:       " ",          // blank after trim
				Amount:      decimal.Zero, // also invalid
				Currency:    "XXX",
				ReasonCode:  "BAD",
			},
			expectedErr: entities.ErrResolutionNotesRequired,
		},
		{
			name:  "AmountCheckedBeforeCurrency",
			actor: "analyst",
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.New(),
				Notes:       "valid",
				Amount:      decimal.Zero, // zero is invalid
				Currency:    "XXX",        // also invalid
				ReasonCode:  "BAD",
			},
			expectedErr: ErrZeroAdjustmentAmount,
		},
		{
			name:  "NegativeAmountRejected",
			actor: "analyst",
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.New(),
				Notes:       "valid",
				Amount:      decimal.NewFromInt(-1), // negative boundary
				Currency:    "USD",                  // valid currency; amount error should surface
				ReasonCode:  "BAD",
			},
			expectedErr: ErrNegativeAdjustmentAmount,
		},
		{
			name:  "CurrencyCheckedBeforeReason",
			actor: "analyst",
			cmd: AdjustEntryCommand{
				ExceptionID: uuid.New(),
				Notes:       "valid",
				Amount:      decimal.NewFromInt(10),
				Currency:    "XXX", // invalid
				ReasonCode:  "BAD", // also invalid
			},
			expectedErr: value_objects.ErrInvalidCurrencyCode,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := &stubExceptionRepo{}
			exec := &stubResolutionExecutor{}
			audit := &stubAuditPublisher{}

			uc, err := NewUseCase(repo, exec, audit, actorExtractor(tc.actor), &stubInfraProvider{})
			require.NoError(t, err)

			_, err = uc.AdjustEntry(context.Background(), tc.cmd)
			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

// TestAdjustEntry_NotesTrimmedInAuditEvent verifies that leading/trailing
// whitespace on notes is stripped and the trimmed value flows into the audit
// event, not the raw input.
func TestAdjustEntry_NotesTrimmedInAuditEvent(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{called: make(chan struct{})}

	ctx := context.Background()

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "  padded notes  ",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.NoError(t, err)

	audit.waitForPublish(t)

	evt := audit.getLastEvent()
	require.NotNil(t, evt)
	assert.Equal(t, "padded notes", evt.Notes, "notes should be trimmed in audit event")
}

// TestAdjustEntry_ActorTrimmedInAuditEvent verifies that the actor value
// returned by the extractor is trimmed before being written to the audit event.
func TestAdjustEntry_ActorTrimmedInAuditEvent(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{called: make(chan struct{})}

	ctx := context.Background()

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	// Actor with leading/trailing whitespace.
	uc, err := NewUseCase(repo, exec, audit, actorExtractor("  spaced-analyst  "), infra)
	require.NoError(t, err)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.NoError(t, err)

	audit.waitForPublish(t)

	evt := audit.getLastEvent()
	require.NotNil(t, evt)
	assert.Equal(t, "spaced-analyst", evt.Actor, "actor should be trimmed in audit event")
}

// TestAdjustEntry_AuditEventFieldCompleteness verifies every field of the
// AuditEvent produced by AdjustEntry, going beyond the selective assertions
// in the sibling test file.
func TestAdjustEntry_AuditEventFieldCompleteness(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{called: make(chan struct{})}

	ctx := context.Background()

	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	before := time.Now().UTC()

	result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "CURRENCY_CORRECTION",
		Notes:       "full audit check",
		Amount:      decimal.RequireFromString("99.99"),
		Currency:    "EUR",
		EffectiveAt: fixedTestTime(),
	})
	require.NoError(t, err)

	after := time.Now().UTC()

	audit.waitForPublish(t)

	evt := audit.getLastEvent()
	require.NotNil(t, evt)

	assert.Equal(t, result.ID, evt.ExceptionID, "audit exception ID must match returned exception")
	assert.Equal(t, "ADJUST_ENTRY", evt.Action, "audit action must be ADJUST_ENTRY")
	assert.Equal(t, "analyst-1", evt.Actor, "audit actor must match extractor value")
	assert.Equal(t, "full audit check", evt.Notes, "audit notes must match input")

	require.NotNil(t, evt.ReasonCode, "audit reason code pointer must be non-nil")
	assert.Equal(t, "CURRENCY_CORRECTION", *evt.ReasonCode, "audit reason code must match parsed reason")

	assert.False(t, evt.OccurredAt.IsZero(), "audit occurred_at must be set")
	assert.True(t,
		!evt.OccurredAt.Before(before) && !evt.OccurredAt.After(after),
		"audit occurred_at must fall between before and after wall-clock bounds",
	)

	require.NotNil(t, evt.Metadata, "audit metadata map must be non-nil")
	assert.Equal(t, "EUR", evt.Metadata["currency"], "audit metadata must contain normalized currency")
}

// TestAdjustEntry_AdjustmentInputPassthrough verifies that the AdjustmentInput
// sent to the ResolutionExecutor has correctly transformed fields: trimmed
// notes, normalized currency, parsed reason, and verbatim amount/effectiveAt.
func TestAdjustEntry_AdjustmentInputPassthrough(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	capExec := &capturingResolutionExecutor{}
	audit := &stubAuditPublisher{}

	ctx := context.Background()

	uc, err := NewUseCase(repo, capExec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	effectiveAt := fixedTestTime()
	amount := decimal.RequireFromString("42.50")

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "DATE_CORRECTION",
		Notes:       "  trimmed notes  ",
		Amount:      amount,
		Currency:    " eur ",
		EffectiveAt: effectiveAt,
	})
	require.NoError(t, err)

	assert.Equal(t, exception.ID, capExec.capturedExceptionID,
		"executor must receive the correct exception ID")
	assert.True(t, amount.Equal(capExec.capturedInput.Amount),
		"executor must receive the verbatim amount")
	assert.Equal(t, "EUR", capExec.capturedInput.Currency,
		"executor must receive normalized (uppercase, trimmed) currency")
	assert.Equal(t, effectiveAt, capExec.capturedInput.EffectiveAt,
		"executor must receive the verbatim effective-at timestamp")
	assert.Equal(t, value_objects.AdjustmentReasonDateCorrection, capExec.capturedInput.Reason,
		"executor must receive the parsed adjustment reason enum")
	assert.Equal(t, "trimmed notes", capExec.capturedInput.Notes,
		"executor must receive trimmed notes")
}

// TestAdjustEntry_ZeroValueCommand verifies that passing a default
// AdjustEntryCommand{} hits the first validation check (ExceptionID required).
func TestAdjustEntry_ZeroValueCommand(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.AdjustEntry(context.Background(), AdjustEntryCommand{})
	require.ErrorIs(t, err, ErrExceptionIDRequired)
}

// TestAdjustEntry_DecimalPrecisionPreserved ensures that decimal amounts with
// many fractional digits are preserved exactly through the pipeline (no
// floating-point truncation).
func TestAdjustEntry_DecimalPrecisionPreserved(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	capExec := &capturingResolutionExecutor{}
	audit := &stubAuditPublisher{}

	ctx := context.Background()

	uc, err := NewUseCase(repo, capExec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	// 8 decimal places -- more than most currencies need, exercises precision.
	preciseAmount := decimal.RequireFromString("123456.78901234")

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "precision test",
		Amount:      preciseAmount,
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.NoError(t, err)

	assert.True(t, preciseAmount.Equal(capExec.capturedInput.Amount),
		"decimal amount must survive the pipeline without precision loss")
}

// TestAdjustEntry_PendingResolutionRejected verifies that when an exception
// is already in PENDING_RESOLUTION status, AdjustEntry returns
// ErrExceptionPendingResolution without reaching the executor or audit trail.
func TestAdjustEntry_PendingResolutionRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	// Simulate exception already in PENDING_RESOLUTION.
	exception.Status = value_objects.ExceptionStatusPendingResolution

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)
	// StartResolution should fail, so no executor call, no update, no audit.

	uc, err := NewUseCase(repo, exec, audit, actor, &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.AdjustEntry(context.Background(), AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "should fail",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.ErrorIs(t, err, entities.ErrExceptionPendingResolution)
}

// TestAdjustEntry_GatewayFailureRevertsStatus verifies that when an exception
// starts in OPEN status and the gateway call (AdjustEntry executor) fails,
// the status is reverted from PENDING_RESOLUTION back to OPEN.
func TestAdjustEntry_GatewayFailureRevertsStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)

	repo := repoMocks.NewMockExceptionRepository(ctrl)
	exec := portMocks.NewMockResolutionExecutor(ctrl)
	audit := portMocks.NewMockAuditPublisher(ctrl)
	actor := actorExtractor("analyst-1")

	// FindByID returns OPEN exception.
	repo.EXPECT().FindByID(gomock.Any(), exception.ID).Return(exception, nil)

	// Update to PENDING_RESOLUTION.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusPendingResolution, exc.Status)
			return exc, nil
		})

	// Gateway call fails.
	exec.EXPECT().AdjustEntry(gomock.Any(), exception.ID, gomock.Any()).Return(errTestExecutor)

	// Revert: Update back to OPEN.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusOpen, exc.Status)
			return exc, nil
		})

	uc, err := NewUseCase(repo, exec, audit, actor, &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.AdjustEntry(context.Background(), AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.ErrorIs(t, err, errTestExecutor)
	require.Equal(t, value_objects.ExceptionStatusOpen, exception.Status)
}

// TestAdjustEntry_GatewayFailureRevertsAssignedStatus verifies that when an
// exception starts in ASSIGNED status and the gateway call fails, the status
// is reverted from PENDING_RESOLUTION back to ASSIGNED (not OPEN).
func TestAdjustEntry_GatewayFailureRevertsAssignedStatus(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	exception, err := entities.NewException(
		context.Background(),
		uuid.New(),
		value_objects.ExceptionSeverityHigh,
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
	exec.EXPECT().AdjustEntry(gomock.Any(), exception.ID, gomock.Any()).Return(errTestExecutor)

	// Revert: Update back to ASSIGNED.
	repo.EXPECT().Update(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, exc *entities.Exception) (*entities.Exception, error) {
			require.Equal(t, value_objects.ExceptionStatusAssigned, exc.Status)
			return exc, nil
		})

	uc, err := NewUseCase(repo, exec, audit, actor, &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.AdjustEntry(context.Background(), AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
		EffectiveAt: fixedTestTime(),
	})
	require.ErrorIs(t, err, errTestExecutor)
	require.Equal(t, value_objects.ExceptionStatusAssigned, exception.Status)
}
