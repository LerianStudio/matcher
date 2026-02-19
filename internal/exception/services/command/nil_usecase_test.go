//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Nil UseCase method calls.
func TestAdjustEntry_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	_, err := uc.AdjustEntry(ctx, AdjustEntryCommand{
		ExceptionID: uuid.New(),
		ReasonCode:  "AMOUNT_CORRECTION",
		Notes:       "test",
		Amount:      decimal.NewFromInt(10),
		Currency:    "USD",
	})

	require.ErrorIs(t, err, ErrNilExceptionRepository)
}

func TestForceMatch_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *UseCase

	ctx := context.Background()
	_, err := uc.ForceMatch(ctx, ForceMatchCommand{
		ExceptionID:    uuid.New(),
		OverrideReason: "POLICY_EXCEPTION",
		Notes:          "test",
	})

	require.ErrorIs(t, err, ErrNilExceptionRepository)
}

func TestOpenDispute_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *DisputeUseCase

	ctx := context.Background()
	_, err := uc.OpenDispute(ctx, OpenDisputeCommand{
		ExceptionID: uuid.New(),
		Category:    "BANK_FEE_ERROR",
		Description: "test",
	})

	require.ErrorIs(t, err, ErrNilDisputeRepository)
}

func TestCloseDispute_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *DisputeUseCase

	ctx := context.Background()
	_, err := uc.CloseDispute(ctx, CloseDisputeCommand{
		DisputeID:  uuid.New(),
		Resolution: "test",
		Won:        true,
	})

	require.ErrorIs(t, err, ErrNilDisputeRepository)
}

func TestSubmitEvidence_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *DisputeUseCase

	ctx := context.Background()
	_, err := uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: uuid.New(),
		Comment:   "test",
	})

	require.ErrorIs(t, err, ErrNilDisputeRepository)
}

func TestProcessCallback_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *CallbackUseCase

	ctx := context.Background()
	err := uc.ProcessCallback(ctx, ProcessCallbackCommand{
		IdempotencyKey:  "valid-key",
		ExceptionID:     uuid.New(),
		ExternalSystem:  "JIRA",
		ExternalIssueID: "PROJ-123",
		Status:          "OPEN",
	})

	require.ErrorIs(t, err, ErrNilIdempotencyRepository)
}

// Partial nil dependency validation.
func TestAdjustEntry_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *UseCase
		expectedErr error
	}{
		{
			name:        "nil exception repo",
			uc:          &UseCase{},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil resolution executor",
			uc: &UseCase{
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilResolutionExecutor,
		},
		{
			name: "nil audit publisher",
			uc: &UseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &UseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
				auditPublisher:     &stubAuditPublisher{},
			},
			expectedErr: ErrNilActorExtractor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := tc.uc.AdjustEntry(ctx, AdjustEntryCommand{
				ExceptionID: uuid.New(),
				ReasonCode:  "AMOUNT_CORRECTION",
				Notes:       "test",
				Amount:      decimal.NewFromInt(10),
				Currency:    "USD",
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestForceMatch_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *UseCase
		expectedErr error
	}{
		{
			name:        "nil exception repo",
			uc:          &UseCase{},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil resolution executor",
			uc: &UseCase{
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilResolutionExecutor,
		},
		{
			name: "nil audit publisher",
			uc: &UseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &UseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
				auditPublisher:     &stubAuditPublisher{},
			},
			expectedErr: ErrNilActorExtractor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := tc.uc.ForceMatch(ctx, ForceMatchCommand{
				ExceptionID:    uuid.New(),
				OverrideReason: "POLICY_EXCEPTION",
				Notes:          "test",
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestOpenDispute_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *DisputeUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &DisputeUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &DisputeUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &DisputeUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &DisputeUseCase{
				disputeRepo:    &stubDisputeRepo{},
				exceptionRepo:  &stubExceptionRepo{},
				auditPublisher: &stubAuditPublisher{},
			},
			expectedErr: ErrNilActorExtractor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := tc.uc.OpenDispute(ctx, OpenDisputeCommand{
				ExceptionID: uuid.New(),
				Category:    "BANK_FEE_ERROR",
				Description: "test",
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestCloseDispute_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *DisputeUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &DisputeUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &DisputeUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &DisputeUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &DisputeUseCase{
				disputeRepo:    &stubDisputeRepo{},
				exceptionRepo:  &stubExceptionRepo{},
				auditPublisher: &stubAuditPublisher{},
			},
			expectedErr: ErrNilActorExtractor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := tc.uc.CloseDispute(ctx, CloseDisputeCommand{
				DisputeID:  uuid.New(),
				Resolution: "test",
				Won:        true,
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestSubmitEvidence_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *DisputeUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &DisputeUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &DisputeUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &DisputeUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &DisputeUseCase{
				disputeRepo:    &stubDisputeRepo{},
				exceptionRepo:  &stubExceptionRepo{},
				auditPublisher: &stubAuditPublisher{},
			},
			expectedErr: ErrNilActorExtractor,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := tc.uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
				DisputeID: uuid.New(),
				Comment:   "test",
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

func TestProcessCallback_PartialNilDependencies(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		uc          *CallbackUseCase
		expectedErr error
	}{
		{
			name:        "nil idempotency repo",
			uc:          &CallbackUseCase{},
			expectedErr: ErrNilIdempotencyRepository,
		},
		{
			name: "nil exception repo",
			uc: &CallbackUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &CallbackUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
				exceptionRepo:   &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil infra provider",
			uc: &CallbackUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
				exceptionRepo:   &stubExceptionRepo{},
				auditPublisher:  &stubAuditPublisher{},
			},
			expectedErr: ErrNilInfraProvider,
		},
		{
			name: "nil rate limiter",
			uc: &CallbackUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
				exceptionRepo:   &stubExceptionRepo{},
				auditPublisher:  &stubAuditPublisher{},
				infraProvider:   &stubInfraProvider{},
			},
			expectedErr: ErrNilCallbackRateLimiter,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			err := tc.uc.ProcessCallback(ctx, ProcessCallbackCommand{
				IdempotencyKey:  "valid-key",
				ExceptionID:     uuid.New(),
				ExternalSystem:  "JIRA",
				ExternalIssueID: "PROJ-123",
				Status:          "OPEN",
			})

			require.ErrorIs(t, err, tc.expectedErr)
		})
	}
}

// Test UseCase validates infra provider via constructor.
func TestNewUseCase_NilInfraProviderDependency(t *testing.T) {
	t.Parallel()

	repo := &stubExceptionRepo{}
	exec := &stubResolutionExecutor{}
	audit := &stubAuditPublisher{}
	actor := actorExtractor("analyst-1")

	uc, err := NewUseCase(repo, exec, audit, actor, nil)

	require.ErrorIs(t, err, ErrNilInfraProvider)
	require.Nil(t, uc)
}

// Test all dependency errors are returned correctly.
func TestNewCallbackUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewCallbackUseCase(nil, nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrNilIdempotencyRepository)
	assert.Nil(t, uc)
}

func TestNewDisputeUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewDisputeUseCase(nil, nil, nil, nil, nil)

	require.ErrorIs(t, err, ErrNilDisputeRepository)
	assert.Nil(t, uc)
}
