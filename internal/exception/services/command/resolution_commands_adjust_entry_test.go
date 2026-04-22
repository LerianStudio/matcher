//go:build unit

package command

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/exception/domain/entities"
	"github.com/LerianStudio/matcher/internal/exception/domain/value_objects"
	sharedexception "github.com/LerianStudio/matcher/internal/shared/domain/exception"
	"github.com/LerianStudio/matcher/internal/shared/testutil"
)

func TestAdjustEntry_Success(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-success"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{called: make(chan struct{})}
	actor := actorExtractor("analyst-1")
	ctx := context.Background()
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectCommit()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actor, infra)
	require.NoError(t, err)

	result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "adjusted amount",
		Amount:      decimal.NewFromInt(25),
		Currency:    "USD",
		EffectiveAt: testutil.FixedTime(),
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)

	audit.waitForPublish(t)

	evt := audit.getLastEvent()
	require.NotNil(t, evt)
	require.Equal(t, "ADJUST_ENTRY", evt.Action)
	require.Equal(t, "analyst-1", evt.Actor)
	require.Equal(t, "USD", evt.Metadata["currency"])
}

func TestAdjustEntry_NegativeAmountRejected(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-negative"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	ctx := context.Background()
	result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "negative adjustment",
		Amount:      decimal.NewFromInt(-50),
		Currency:    "EUR",
		EffectiveAt: testutil.FixedTime(),
	})
	require.ErrorIs(t, err, ErrNegativeAdjustmentAmount)
	require.Nil(t, result)
}

func TestAdjustEntry_ValidationErrors(t *testing.T) {
	t.Parallel()

	exceptionID := testutil.MustDeterministicUUID("adjust-entry-validation")

	exception, err := entities.NewException(
		context.Background(),
		exceptionID,
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	tests := []struct {
		name        string
		input       AdjustEntryCommand
		expectedErr error
	}{
		{
			name: "NilExceptionID",
			input: AdjustEntryCommand{
				ExceptionID: uuid.Nil,
				Notes:       "note",
				ReasonCode:  "AMOUNT_CORRECTION",
				Amount:      decimal.NewFromInt(10),
				Currency:    "USD",
			},
			expectedErr: ErrExceptionIDRequired,
		},
		{
			name: "BlankNotes",
			input: AdjustEntryCommand{
				ExceptionID: exception.ID,
				Notes:       " ",
				ReasonCode:  "AMOUNT_CORRECTION",
				Amount:      decimal.NewFromInt(10),
				Currency:    "USD",
			},
			expectedErr: entities.ErrResolutionNotesRequired,
		},
		{
			name: "InvalidReasonCode",
			input: AdjustEntryCommand{
				ExceptionID: exception.ID,
				Notes:       "note",
				ReasonCode:  "BAD",
				Amount:      decimal.NewFromInt(10),
				Currency:    "USD",
			},
			expectedErr: value_objects.ErrInvalidAdjustmentReason,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			repo := &stubExceptionRepo{exception: exception}
			exec := &stubResolutionExecutor{}
			audit := &stubAuditPublisher{}

			uc, ucErr := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
			require.NoError(t, ucErr)

			_, adjustErr := uc.AdjustEntry(context.Background(), tc.input)
			require.ErrorIs(t, adjustErr, tc.expectedErr)
		})
	}
}

func TestAdjustEntry_ActorRequired(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-actor-required"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	emptyActor := actorExtractor("")

	uc, err := NewUseCase(repo, exec, audit, emptyActor, &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc.AdjustEntry(context.Background(), AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})
	require.ErrorIs(t, err, ErrActorRequired)

	whitespaceActor := actorExtractor("  ")
	uc2, err := NewUseCase(repo, exec, audit, whitespaceActor, &stubInfraProvider{})
	require.NoError(t, err)

	_, err = uc2.AdjustEntry(context.Background(), AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})
	require.ErrorIs(t, err, ErrActorRequired)
}

func TestAdjustEntry_ZeroAmountRejected(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-zero-amount"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "zero adjustment",
		Amount:      decimal.Zero,
		Currency:    "USD",
	})
	require.ErrorIs(t, err, ErrZeroAdjustmentAmount)
}

func TestAdjustEntry_InvalidCurrency(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-invalid-currency"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	ctx := context.Background()

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "XXX",
	})
	require.ErrorIs(t, err, value_objects.ErrInvalidCurrencyCode)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "",
	})
	require.ErrorIs(t, err, value_objects.ErrInvalidCurrencyCode)

	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "TOOLONG",
	})
	require.ErrorIs(t, err, value_objects.ErrInvalidCurrencyCode)
}

func TestAdjustEntry_DependencyErrors(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-dependency-errors"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception, findErr: errTestFind}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	ctx := context.Background()

	_, err = uc.AdjustEntry(
		ctx,
		AdjustEntryCommand{
			ExceptionID: exception.ID,
			Notes:       "note",
			ReasonCode:  "AMOUNT_CORRECTION",
			Amount:      decimal.NewFromInt(10),
			Currency:    "USD",
		},
	)
	require.ErrorIs(t, err, errTestFind)

	exception.Status = value_objects.ExceptionStatusResolved
	repo.findErr = nil
	_, err = uc.AdjustEntry(
		ctx,
		AdjustEntryCommand{
			ExceptionID: exception.ID,
			Notes:       "note",
			ReasonCode:  "AMOUNT_CORRECTION",
			Amount:      decimal.NewFromInt(10),
			Currency:    "USD",
		},
	)
	require.ErrorIs(t, err, value_objects.ErrInvalidResolutionTransition)

	exception.Status = value_objects.ExceptionStatusOpen
	exec.adjustEntryErr = errTestExecutor
	_, err = uc.AdjustEntry(
		ctx,
		AdjustEntryCommand{
			ExceptionID: exception.ID,
			Notes:       "note",
			ReasonCode:  "AMOUNT_CORRECTION",
			Amount:      decimal.NewFromInt(10),
			Currency:    "USD",
		},
	)
	require.ErrorIs(t, err, errTestExecutor)
}

func TestAdjustEntry_AuditError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-audit-error"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{err: errTestAudit}
	ctx := context.Background()
	tx, mock, err := newMockTx(ctx)
	require.NoError(t, err)
	mock.ExpectRollback()

	infra := &stubInfraProvider{tx: tx}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), infra)
	require.NoError(t, err)

	// Audit errors propagate atomically — if audit publish fails within the
	// transaction, the entire transaction rolls back (SOX compliance).
	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "note",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})
	require.ErrorIs(t, err, errTestAudit)

	evt := audit.getLastEvent()
	require.NotNil(t, evt)
	require.WithinDuration(t, time.Now().UTC(), evt.OccurredAt, time.Second)
}

func TestAdjustEntry_UpdateError(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-update-error"),
		sharedexception.ExceptionSeverityHigh,
		nil,
	)
	require.NoError(t, err)

	repo := &stubExceptionRepo{exception: exception, updateErr: errTestUpdate}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}

	uc, err := NewUseCase(repo, exec, audit, actorExtractor("analyst-1"), &stubInfraProvider{})
	require.NoError(t, err)

	ctx := context.Background()
	_, err = uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		Notes:       "note",
		ReasonCode:  "AMOUNT_CORRECTION",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})
	require.ErrorIs(t, err, errTestUpdate)
}

func TestAdjustEntry_CurrencyNormalization(t *testing.T) {
	t.Parallel()

	exception, err := entities.NewException(
		context.Background(),
		testutil.MustDeterministicUUID("adjust-entry-currency-normalization"),
		sharedexception.ExceptionSeverityHigh,
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

	result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: exception.ID,
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "normalized currency",
		Amount:      decimal.NewFromInt(10),
		Currency:    " usd ",
		EffectiveAt: testutil.FixedTime(),
	})
	require.NoError(t, err)
	require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)

	audit.waitForPublish(t)

	evt := audit.getLastEvent()
	require.Equal(t, "USD", evt.Metadata["currency"])
}

func TestAdjustEntry_AllAdjustmentReasons(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name   string
		reason string
	}{
		{name: "AmountCorrection", reason: "AMOUNT_CORRECTION"},
		{name: "CurrencyCorrection", reason: "CURRENCY_CORRECTION"},
		{name: "DateCorrection", reason: "DATE_CORRECTION"},
		{name: "Other", reason: "OTHER"},
		{name: "LowercaseNormalized", reason: "amount_correction"},
		{name: "MixedCaseNormalized", reason: "Date_Correction"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				testutil.MustDeterministicUUID("adjust-entry-reason-"+tc.name),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			repo := &stubExceptionRepo{exception: exception}
			exec := &stubResolutionExecutor{}
			audit := &stubAuditPublisher{called: make(chan struct{})}

			uc, err := NewUseCase(
				repo,
				exec,
				audit,
				actorExtractor("analyst-1"),
				&stubInfraProvider{},
			)
			require.NoError(t, err)

			ctx := context.Background()
			result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
				ExceptionID: exception.ID,
				ReasonCode:  tc.reason,
				Notes:       "adjustment for reason: " + tc.reason,
				Amount:      decimal.NewFromInt(100),
				Currency:    "USD",
				EffectiveAt: testutil.FixedTime(),
			})
			require.NoError(t, err)
			require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)

			audit.waitForPublish(t)

			evt := audit.getLastEvent()
			require.NotNil(t, evt)
			require.Equal(t, "ADJUST_ENTRY", evt.Action)
		})
	}
}

func TestAdjustEntry_AllCurrencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		currency string
		expected string
	}{
		{name: "USD", currency: "USD", expected: "USD"},
		{name: "EUR", currency: "EUR", expected: "EUR"},
		{name: "BRL", currency: "BRL", expected: "BRL"},
		{name: "GBP", currency: "GBP", expected: "GBP"},
		{name: "JPY", currency: "JPY", expected: "JPY"},
		{name: "CNY", currency: "CNY", expected: "CNY"},
		{name: "CHF", currency: "CHF", expected: "CHF"},
		{name: "CAD", currency: "CAD", expected: "CAD"},
		{name: "AUD", currency: "AUD", expected: "AUD"},
		{name: "MXN", currency: "MXN", expected: "MXN"},
		{name: "LowercaseNormalized", currency: "eur", expected: "EUR"},
		{name: "WhitespaceNormalized", currency: " brl ", expected: "BRL"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				testutil.MustDeterministicUUID("adjust-entry-currency-"+tc.name),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			repo := &stubExceptionRepo{exception: exception}
			exec := &stubResolutionExecutor{}
			audit := &stubAuditPublisher{called: make(chan struct{})}

			uc, err := NewUseCase(
				repo,
				exec,
				audit,
				actorExtractor("analyst-1"),
				&stubInfraProvider{},
			)
			require.NoError(t, err)

			ctx := context.Background()
			result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
				ExceptionID: exception.ID,
				ReasonCode:  "AMOUNT_CORRECTION",
				Notes:       "adjustment in " + tc.currency,
				Amount:      decimal.NewFromInt(500),
				Currency:    tc.currency,
				EffectiveAt: testutil.FixedTime(),
			})
			require.NoError(t, err)
			require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)

			audit.waitForPublish(t)

			evt := audit.getLastEvent()
			require.NotNil(t, evt)
			require.Equal(t, tc.expected, evt.Metadata["currency"])
		})
	}
}

func TestAdjustEntry_EffectiveAtBoundaries(t *testing.T) {
	t.Parallel()

	now := testutil.FixedTime()

	testCases := []struct {
		name        string
		effectiveAt time.Time
	}{
		{name: "PastDate", effectiveAt: now.AddDate(-1, 0, 0)},
		{name: "YesterdayDate", effectiveAt: now.AddDate(0, 0, -1)},
		{name: "PresentDate", effectiveAt: now},
		{name: "TomorrowDate", effectiveAt: now.AddDate(0, 0, 1)},
		{name: "FutureDate", effectiveAt: now.AddDate(1, 0, 0)},
		{name: "ZeroDate", effectiveAt: time.Time{}},
		{name: "FarPast", effectiveAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
		{name: "FarFuture", effectiveAt: time.Date(2050, 12, 31, 23, 59, 59, 0, time.UTC)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exception, err := entities.NewException(
				context.Background(),
				testutil.MustDeterministicUUID("adjust-entry-effective-"+tc.name),
				sharedexception.ExceptionSeverityHigh,
				nil,
			)
			require.NoError(t, err)

			repo := &stubExceptionRepo{exception: exception}
			exec := &stubResolutionExecutor{}
			audit := &stubAuditPublisher{called: make(chan struct{})}

			uc, err := NewUseCase(
				repo,
				exec,
				audit,
				actorExtractor("analyst-1"),
				&stubInfraProvider{},
			)
			require.NoError(t, err)

			ctx := context.Background()
			result, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
				ExceptionID: exception.ID,
				ReasonCode:  "DATE_CORRECTION",
				Notes:       "effective at boundary test",
				Amount:      decimal.NewFromInt(200),
				Currency:    "USD",
				EffectiveAt: tc.effectiveAt,
			})
			require.NoError(t, err)
			require.Equal(t, value_objects.ExceptionStatusResolved, result.Status)

			audit.waitForPublish(t)

			evt := audit.getLastEvent()
			require.NotNil(t, evt)
			require.Equal(t, "ADJUST_ENTRY", evt.Action)
		})
	}
}
