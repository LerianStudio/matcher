// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package command

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingVO "github.com/LerianStudio/matcher/internal/matching/domain/value_objects"
)

func TestRunMatch_NilContextID_Returns_Error(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000280001")
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  &stubContextProvider{},
		SourceProvider:   &stubSourceProvider{},
		RuleProvider:     &stubRuleProvider{},
		TxRepo:           &stubTxRepo{},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	_, _, err = uc.RunMatch(ctx, RunMatchInput{
		TenantID:  tenantID,
		ContextID: uuid.Nil,
		Mode:      matchingVO.MatchRunModeCommit,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrRunMatchContextIDRequired)
}

func TestRunMatch_InvalidMode_Returns_Error(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000280010")
	outboxRepo := newMockOutboxRepository(t, nil)

	uc, err := New(UseCaseDeps{
		ContextProvider:  &stubContextProvider{},
		SourceProvider:   &stubSourceProvider{},
		RuleProvider:     &stubRuleProvider{},
		TxRepo:           &stubTxRepo{},
		LockManager:      &stubLockManager{},
		MatchRunRepo:     &stubMatchRunRepo{},
		MatchGroupRepo:   &stubMatchGroupRepo{},
		MatchItemRepo:    &stubMatchItemRepo{},
		ExceptionCreator: &stubExceptionCreator{},
		OutboxRepo:       outboxRepo,
		FeeVarianceRepo:  &stubFeeVarianceRepo{},
		AdjustmentRepo:   &stubAdjustmentRepo{},
		InfraProvider:    &stubInfraProviderForRun{},
		AuditLogRepo:     &stubAuditLogRepoForRun{},
		FeeScheduleRepo:  &mockFeeScheduleRepo{},
		FeeRuleProvider:  &mockFeeRuleProvider{},
	})
	require.NoError(t, err)

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	_, _, err = uc.RunMatch(ctx, RunMatchInput{
		TenantID:  tenantID,
		ContextID: uuid.New(),
		Mode:      matchingVO.MatchRunMode("INVALID"),
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrMatchRunModeRequired)
}

func TestValidateAndEnrichTenant_NilTenantInInputGetsEnriched(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000280020")
	uc := &UseCase{}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, tenantID.String())
	input := &RunMatchInput{TenantID: uuid.Nil, ContextID: uuid.New(), Mode: matchingVO.MatchRunModeCommit}

	_, err := uc.validateAndEnrichTenant(ctx, input)
	require.NoError(t, err)
	assert.Equal(t, tenantID, input.TenantID)
}

func TestValidateAndEnrichTenant_MismatchReturnsError(t *testing.T) {
	t.Parallel()

	ctxTenantID := uuid.MustParse("00000000-0000-0000-0000-000000280030")
	inputTenantID := uuid.MustParse("00000000-0000-0000-0000-000000280031")
	uc := &UseCase{}

	ctx := context.WithValue(context.Background(), auth.TenantIDKey, ctxTenantID.String())
	input := &RunMatchInput{TenantID: inputTenantID}

	_, err := uc.validateAndEnrichTenant(ctx, input)
	require.ErrorIs(t, err, ErrTenantIDMismatch)
}

func TestValidateAndEnrichTenant_InvalidUUIDInContext(t *testing.T) {
	t.Parallel()

	uc := &UseCase{}
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "not-a-uuid")
	input := &RunMatchInput{TenantID: uuid.New()}

	_, err := uc.validateAndEnrichTenant(ctx, input)
	require.ErrorIs(t, err, ErrTenantIDRequired)
}

func TestRunMatch_ValidatesDependencies(t *testing.T) {
	t.Parallel()

	uc := &UseCase{} // All nil
	_, _, err := uc.RunMatch(context.Background(), RunMatchInput{})
	require.Error(t, err)
}
