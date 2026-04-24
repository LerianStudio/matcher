// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

	var uc *ExceptionUseCase

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

	var uc *ExceptionUseCase

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

	var uc *ExceptionUseCase

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

	var uc *ExceptionUseCase

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

	var uc *ExceptionUseCase

	ctx := context.Background()
	_, err := uc.SubmitEvidence(ctx, SubmitEvidenceCommand{
		DisputeID: uuid.New(),
		Comment:   "test",
	})

	require.ErrorIs(t, err, ErrNilDisputeRepository)
}

func TestProcessCallback_NilUseCase(t *testing.T) {
	t.Parallel()

	var uc *ExceptionUseCase

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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil exception repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil resolution executor",
			uc: &ExceptionUseCase{
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilResolutionExecutor,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &ExceptionUseCase{
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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil exception repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil resolution executor",
			uc: &ExceptionUseCase{
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilResolutionExecutor,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				exceptionRepo:      &stubExceptionRepo{},
				resolutionExecutor: &stubResolutionExecutor{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &ExceptionUseCase{
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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &ExceptionUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &ExceptionUseCase{
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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &ExceptionUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &ExceptionUseCase{
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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil dispute repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilDisputeRepository,
		},
		{
			name: "nil exception repo",
			uc: &ExceptionUseCase{
				disputeRepo: &stubDisputeRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				disputeRepo:   &stubDisputeRepo{},
				exceptionRepo: &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil actor extractor",
			uc: &ExceptionUseCase{
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
		uc          *ExceptionUseCase
		expectedErr error
	}{
		{
			name:        "nil idempotency repo",
			uc:          &ExceptionUseCase{},
			expectedErr: ErrNilIdempotencyRepository,
		},
		{
			name: "nil exception repo",
			uc: &ExceptionUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
			},
			expectedErr: ErrNilExceptionRepository,
		},
		{
			name: "nil audit publisher",
			uc: &ExceptionUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
				exceptionRepo:   &stubExceptionRepo{},
			},
			expectedErr: ErrNilAuditPublisher,
		},
		{
			name: "nil infra provider",
			uc: &ExceptionUseCase{
				idempotencyRepo: &stubIdempotencyRepo{},
				exceptionRepo:   &stubExceptionRepo{},
				auditPublisher:  &stubAuditPublisher{},
			},
			expectedErr: ErrNilInfraProvider,
		},
		{
			name: "nil rate limiter",
			uc: &ExceptionUseCase{
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

	uc, err := NewExceptionUseCase(repo, actor, audit, nil, WithResolutionExecutor(exec))

	require.ErrorIs(t, err, ErrNilInfraProvider)
	require.Nil(t, uc)
}

// TestNewCallbackUseCase_AllDependenciesNil verifies the first required
// dependency (exception repo) is validated first, independent of which
// optional options were supplied. The callback-specific deps (idempotency,
// rate limiter) are optional at construction time and their nil checks
// live on ProcessCallback itself.
func TestNewCallbackUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewExceptionUseCase(nil, actorExtractor("system"), nil, nil, WithIdempotencyRepository(nil), WithCallbackRateLimiter(nil))

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}

// TestNewDisputeUseCase_AllDependenciesNil verifies the first required
// dependency (exception repo) is validated first, independent of which
// optional options were supplied. The dispute-specific dep (dispute repo)
// is optional at construction time and its nil check lives on
// OpenDispute / CloseDispute / SubmitEvidence themselves.
func TestNewDisputeUseCase_AllDependenciesNil(t *testing.T) {
	t.Parallel()

	uc, err := NewExceptionUseCase(nil, nil, nil, nil, WithDisputeRepository(nil))

	require.ErrorIs(t, err, ErrNilExceptionRepository)
	assert.Nil(t, uc)
}
