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

	libHTTP "github.com/LerianStudio/lib-uncommons/v2/uncommons/net/http"
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
