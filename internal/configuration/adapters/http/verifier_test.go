//go:build unit

package http

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libHTTP "github.com/LerianStudio/lib-commons/v4/commons/net/http"
	"github.com/LerianStudio/matcher/internal/configuration/domain/entities"
	"github.com/LerianStudio/matcher/internal/configuration/domain/repositories/mocks"
	"github.com/LerianStudio/matcher/internal/configuration/domain/value_objects"
	"github.com/LerianStudio/matcher/internal/configuration/services/query"
)

var errMockQuery = errors.New("mock query error")

func createMockRepositories(ctrl *gomock.Controller) (
	*mocks.MockContextRepository,
	*mocks.MockSourceRepository,
	*mocks.MockFieldMapRepository,
	*mocks.MockMatchRuleRepository,
) {
	return mocks.NewMockContextRepository(ctrl),
		mocks.NewMockSourceRepository(ctrl),
		mocks.NewMockFieldMapRepository(ctrl),
		mocks.NewMockMatchRuleRepository(ctrl)
}

func setupMockContext(
	ctxRepo *mocks.MockContextRepository,
	contextID, tenantID uuid.UUID,
	status value_objects.ContextStatus,
	returnErr error,
) {
	if returnErr != nil {
		ctxRepo.EXPECT().FindByID(gomock.Any(), contextID).Return(nil, returnErr)
	} else {
		ctxRepo.EXPECT().FindByID(gomock.Any(), contextID).Return(&entities.ReconciliationContext{
			ID:       contextID,
			TenantID: tenantID,
			Name:     "test-context",
			Type:     value_objects.ContextTypeOneToOne,
			Status:   status,
		}, nil)
	}
}

func createVerifierWithMocks(
	t *testing.T,
	ctxRepo *mocks.MockContextRepository,
	srcRepo *mocks.MockSourceRepository,
	fmRepo *mocks.MockFieldMapRepository,
	mrRepo *mocks.MockMatchRuleRepository,
) libHTTP.TenantOwnershipVerifier {
	t.Helper()

	uc, err := query.NewUseCase(ctxRepo, srcRepo, fmRepo, mrRepo)
	require.NoError(t, err)

	return NewTenantOwnershipVerifier(uc)
}

func TestTenantOwnershipVerifier_VerifyOwnership(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	differentTenantID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	t.Run("nil query use case returns libHTTP.ErrContextAccessDenied", func(t *testing.T) {
		t.Parallel()

		v := NewTenantOwnershipVerifier(nil)
		err := v(context.Background(), validTenantID, validContextID)

		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextAccessDenied)
	})

	t.Run("query returns sql.ErrNoRows returns ErrContextNotFound", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			validTenantID,
			value_objects.ContextStatusActive,
			sql.ErrNoRows,
		)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextNotFound)
	})

	t.Run("query returns generic error returns infrastructure error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			validTenantID,
			value_objects.ContextStatusActive,
			errMockQuery,
		)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		require.Error(t, err)
		require.ErrorIs(t, err, errMockQuery)
		assert.Contains(t, err.Error(), "failed to verify context ownership")
	})

	t.Run("query returns nil context returns ErrContextNotFound", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		ctxRepo.EXPECT().FindByID(gomock.Any(), validContextID).Return(nil, nil)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextNotFound)
	})

	t.Run("context found but tenant mismatch returns libHTTP.ErrContextNotOwned", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			differentTenantID,
			value_objects.ContextStatusActive,
			nil,
		)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextNotOwned)
	})

	t.Run("paused context is still accessible for configuration", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			validTenantID,
			value_objects.ContextStatusPaused,
			nil,
		)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		assert.NoError(t, err)
	})

	t.Run("success case returns nil error", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			validTenantID,
			value_objects.ContextStatusActive,
			nil,
		)
		v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
		err := v(context.Background(), validTenantID, validContextID)
		assert.NoError(t, err)
	})
}

func TestNewTenantOwnershipVerifier(t *testing.T) {
	t.Parallel()

	t.Run("creates verifier with nil query use case", func(t *testing.T) {
		t.Parallel()

		verifier := NewTenantOwnershipVerifier(nil)
		require.NotNil(t, verifier)

		err := verifier(context.Background(), uuid.New(), uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextAccessDenied)
	})

	t.Run("creates verifier with valid query use case", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)

		defer ctrl.Finish()

		validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")

		validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
		setupMockContext(
			ctxRepo,
			validContextID,
			validTenantID,
			value_objects.ContextStatusActive,
			nil,
		)
		uc, err := query.NewUseCase(ctxRepo, srcRepo, fmRepo, mrRepo)
		require.NoError(t, err)

		verifier := NewTenantOwnershipVerifier(uc)
		require.NotNil(t, verifier)
		err = verifier(context.Background(), validTenantID, validContextID)
		assert.NoError(t, err)
	})
}

func TestTenantOwnershipVerifier_PassesCorrectParameters(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	expectedTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	expectedContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
	ctxRepo.EXPECT().
		FindByID(gomock.Any(), expectedContextID).
		DoAndReturn(func(ctx context.Context, contextID uuid.UUID) (*entities.ReconciliationContext, error) {
			assert.Equal(t, expectedContextID, contextID)

			return &entities.ReconciliationContext{
				ID:       expectedContextID,
				TenantID: expectedTenantID,
				Name:     "test-context",
				Type:     value_objects.ContextTypeOneToOne,
				Status:   value_objects.ContextStatusActive,
			}, nil
		})
	v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
	err := v(context.Background(), expectedTenantID, expectedContextID)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Security audit tests (Taura Security, 2026-03)
//
// These tests prove that the configuration verifier does NOT block operations
// on paused contexts, ensuring PAUSED contexts remain recoverable via the
// UpdateContext endpoint (PATCH with status=ACTIVE).
// ---------------------------------------------------------------------------

// TestPausedContextRemainsAccessibleForConfiguration verifies that ALL
// non-archived context statuses pass the configuration verifier without error.
// This is the primary regression test for the Taura Security finding that
// paused contexts could become irrecoverable if the configuration verifier
// enforced an active-status check.
func TestPausedContextRemainsAccessibleForConfiguration(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name   string
		status value_objects.ContextStatus
	}{
		{
			name:   "DRAFT context accessible via configuration verifier",
			status: value_objects.ContextStatusDraft,
		},
		{
			name:   "ACTIVE context accessible via configuration verifier",
			status: value_objects.ContextStatusActive,
		},
		{
			name:   "PAUSED context accessible via configuration verifier (recovery path)",
			status: value_objects.ContextStatusPaused,
		},
		{
			name:   "ARCHIVED context accessible via configuration verifier (read/delete only at domain level)",
			status: value_objects.ContextStatusArchived,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			defer ctrl.Finish()

			ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
			setupMockContext(ctxRepo, validContextID, validTenantID, tt.status, nil)

			v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
			err := v(context.Background(), validTenantID, validContextID)

			assert.NoError(t, err, "configuration verifier must NOT block %s contexts", tt.status)
		})
	}
}

// TestPausedContextCanBeReactivatedViaDomainStateMachine proves the full
// recovery path: a PAUSED context can transition back to ACTIVE via the
// domain entity's Activate() method, which is the code path used by the
// UpdateContext handler when status=ACTIVE is sent in the PATCH payload.
func TestPausedContextCanBeReactivatedViaDomainStateMachine(t *testing.T) {
	t.Parallel()

	pausedCtx := &entities.ReconciliationContext{
		ID:       uuid.MustParse("22222222-2222-2222-2222-222222222222"),
		TenantID: uuid.MustParse("11111111-1111-1111-1111-111111111111"),
		Name:     "paused-context",
		Type:     value_objects.ContextTypeOneToOne,
		Status:   value_objects.ContextStatusPaused,
	}

	// Step 1: Verify the context is paused.
	assert.Equal(t, value_objects.ContextStatusPaused, pausedCtx.Status)

	// Step 2: Apply an update with status=ACTIVE (simulates the PATCH handler).
	activeStatus := value_objects.ContextStatusActive
	err := pausedCtx.Update(context.Background(), entities.UpdateReconciliationContextInput{
		Status: &activeStatus,
	})
	require.NoError(t, err, "PAUSED -> ACTIVE transition must succeed")

	// Step 3: Verify the context is now active.
	assert.Equal(t, value_objects.ContextStatusActive, pausedCtx.Status)
}

// TestPausedContextCanBeReadViaConfigurationVerifier proves that reading a
// paused context through the configuration verifier succeeds. This simulates
// the GetContext handler path: ParseAndVerifyTenantScopedID calls the verifier,
// which returns nil (success), allowing the handler to proceed with the read.
func TestPausedContextCanBeReadViaConfigurationVerifier(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
	setupMockContext(ctxRepo, validContextID, validTenantID, value_objects.ContextStatusPaused, nil)

	v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
	err := v(context.Background(), validTenantID, validContextID)

	assert.NoError(t, err, "reading a PAUSED context via configuration verifier must succeed")
}

// TestPausedContextCanBeDeletedViaConfigurationVerifier proves that deleting
// a paused context through the configuration verifier succeeds. The verifier
// does not block any status; deletion constraints (e.g., child entities) are
// enforced at the domain/service layer, not the verifier.
func TestPausedContextCanBeDeletedViaConfigurationVerifier(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
	setupMockContext(ctxRepo, validContextID, validTenantID, value_objects.ContextStatusPaused, nil)

	v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
	err := v(context.Background(), validTenantID, validContextID)

	assert.NoError(t, err, "deleting a PAUSED context via configuration verifier must succeed")
}

// TestConfigurationVerifierNeverReturnsErrContextNotActive is a negative test
// that proves the configuration verifier cannot produce ErrContextNotActive
// for ANY context status. This is a guard against future regressions -- if
// someone accidentally adds an active-status check to the configuration
// verifier, this test will fail.
func TestConfigurationVerifierNeverReturnsErrContextNotActive(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	allStatuses := []value_objects.ContextStatus{
		value_objects.ContextStatusDraft,
		value_objects.ContextStatusActive,
		value_objects.ContextStatusPaused,
		value_objects.ContextStatusArchived,
	}

	for _, status := range allStatuses {
		t.Run("no ErrContextNotActive for "+status.String(), func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)

			defer ctrl.Finish()

			ctxRepo, srcRepo, fmRepo, mrRepo := createMockRepositories(ctrl)
			setupMockContext(ctxRepo, validContextID, validTenantID, status, nil)

			v := createVerifierWithMocks(t, ctxRepo, srcRepo, fmRepo, mrRepo)
			err := v(context.Background(), validTenantID, validContextID)
			if err != nil {
				assert.False(t,
					errors.Is(err, libHTTP.ErrContextNotActive),
					"configuration verifier must NEVER return ErrContextNotActive for %s status", status,
				)
			}
		})
	}
}
