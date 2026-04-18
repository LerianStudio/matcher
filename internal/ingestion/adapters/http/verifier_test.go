//go:build unit

package http

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
)

var errMockContextProvider = errors.New("mock context provider error")

type mockContextProvider struct {
	findByIDFunc func(ctx context.Context, contextID uuid.UUID) (*ReconciliationContextInfo, error)
}

func (m *mockContextProvider) FindByID(
	ctx context.Context,
	contextID uuid.UUID,
) (*ReconciliationContextInfo, error) {
	if m.findByIDFunc != nil {
		return m.findByIDFunc(ctx, contextID)
	}

	return nil, nil
}

func TestTenantOwnershipVerifier_VerifyOwnership(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	differentContextID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	tests := []struct {
		name          string
		verifier      libHTTP.TenantOwnershipVerifier
		tenantID      uuid.UUID
		contextID     uuid.UUID
		expectedErr   error
		expectedErrIn string
	}{
		{
			name:        "nil context provider returns libHTTP.ErrContextAccessDenied",
			verifier:    NewTenantOwnershipVerifier(nil),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: libHTTP.ErrContextAccessDenied,
		},
		{
			name:        "provider returns error returns infrastructure error (not access denied)",
			verifier:    createMockVerifierWithError(errMockContextProvider),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: errMockContextProvider,
		},
		{
			name:        "provider returns nil context returns ErrContextNotFound",
			verifier:    createMockVerifierWithNilContext(),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: libHTTP.ErrContextNotFound,
		},
		{
			name:        "context found but ID mismatch returns libHTTP.ErrContextNotOwned",
			verifier:    createMockVerifierWithContext(differentContextID, true),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: libHTTP.ErrContextNotOwned,
		},
		{
			name:          "context found but inactive returns libHTTP.ErrContextNotActive",
			verifier:      createMockVerifierWithContext(validContextID, false),
			tenantID:      validTenantID,
			contextID:     validContextID,
			expectedErr:   libHTTP.ErrContextNotActive,
			expectedErrIn: "context is paused",
		},
		{
			name:        "success case returns nil error",
			verifier:    createMockVerifierWithContext(validContextID, true),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.verifier(context.Background(), tt.tenantID, tt.contextID)

			verifyOwnershipTestResult(t, err, tt.expectedErr, tt.expectedErrIn)
		})
	}
}

func createMockVerifierWithError(err error) libHTTP.TenantOwnershipVerifier {
	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, contextID uuid.UUID) (*ReconciliationContextInfo, error) {
			return nil, err
		},
	}

	return NewTenantOwnershipVerifier(mock)
}

func createMockVerifierWithNilContext() libHTTP.TenantOwnershipVerifier {
	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, contextID uuid.UUID) (*ReconciliationContextInfo, error) {
			return nil, nil
		},
	}

	return NewTenantOwnershipVerifier(mock)
}

func createMockVerifierWithContext(contextID uuid.UUID, active bool) libHTTP.TenantOwnershipVerifier {
	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, cID uuid.UUID) (*ReconciliationContextInfo, error) {
			return &ReconciliationContextInfo{
				ID:     contextID,
				Active: active,
			}, nil
		},
	}

	return NewTenantOwnershipVerifier(mock)
}

func verifyOwnershipTestResult(t *testing.T, err, expectedErr error, expectedErrIn string) {
	t.Helper()

	if expectedErr != nil {
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)

		if expectedErrIn != "" {
			assert.Contains(t, err.Error(), expectedErrIn)
		}
	} else {
		assert.NoError(t, err)
	}
}

func TestNewTenantOwnershipVerifier(t *testing.T) {
	t.Parallel()

	t.Run("creates verifier with nil context provider", func(t *testing.T) {
		t.Parallel()

		verifier := NewTenantOwnershipVerifier(nil)
		require.NotNil(t, verifier)

		err := verifier(context.Background(), uuid.New(), uuid.New())
		require.Error(t, err)
		require.ErrorIs(t, err, libHTTP.ErrContextAccessDenied)
	})

	t.Run("creates verifier with valid context provider", func(t *testing.T) {
		t.Parallel()

		validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
		mock := &mockContextProvider{
			findByIDFunc: func(ctx context.Context, contextID uuid.UUID) (*ReconciliationContextInfo, error) {
				return &ReconciliationContextInfo{
					ID:     validContextID,
					Active: true,
				}, nil
			},
		}

		verifier := NewTenantOwnershipVerifier(mock)
		require.NotNil(t, verifier)

		err := verifier(context.Background(), uuid.New(), validContextID)
		assert.NoError(t, err)
	})
}

func TestTenantOwnershipVerifier_PassesCorrectParameters(t *testing.T) {
	t.Parallel()

	expectedContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	var capturedContextID uuid.UUID

	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, contextID uuid.UUID) (*ReconciliationContextInfo, error) {
			capturedContextID = contextID

			return &ReconciliationContextInfo{
				ID:     expectedContextID,
				Active: true,
			}, nil
		},
	}

	verifier := NewTenantOwnershipVerifier(mock)
	err := verifier(context.Background(), uuid.MustParse("11111111-1111-1111-1111-111111111111"), expectedContextID)

	require.NoError(t, err)
	assert.Equal(t, expectedContextID, capturedContextID)
}
