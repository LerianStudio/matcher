//go:build unit

package http

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libHTTP "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	"github.com/LerianStudio/matcher/internal/matching/ports"
)

var errMockContextProvider = errors.New("mock context provider error")

type mockContextProvider struct {
	findByIDFunc func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error)
}

func (m *mockContextProvider) FindByID(
	ctx context.Context,
	tenantID, contextID uuid.UUID,
) (*ports.ReconciliationContextInfo, error) {
	if m.findByIDFunc != nil {
		return m.findByIDFunc(ctx, tenantID, contextID)
	}

	return nil, nil
}

func newMockContextProviderWithError() *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return nil, errMockContextProvider
		},
	}
}

func newMockContextProviderNotFound() *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return nil, fmt.Errorf("find by id: %w", libHTTP.ErrContextNotFound)
		},
	}
}

func newMockContextProviderReturnsNil() *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return nil, nil
		},
	}
}

func newMockContextProviderWithIDMismatch(differentID uuid.UUID) *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return &ports.ReconciliationContextInfo{
				ID:     differentID,
				Active: true,
			}, nil
		},
	}
}

func newMockContextProviderInactive() *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: false,
			}, nil
		},
	}
}

func newMockContextProviderSuccess() *mockContextProvider {
	return &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			return &ports.ReconciliationContextInfo{
				ID:     contextID,
				Active: true,
			}, nil
		},
	}
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
			verifier:    NewTenantOwnershipVerifier(newMockContextProviderWithError()),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: errMockContextProvider,
		},
		{
			name:          "provider returns nil context returns ErrContextNotFound",
			verifier:      NewTenantOwnershipVerifier(newMockContextProviderReturnsNil()),
			tenantID:      validTenantID,
			contextID:     validContextID,
			expectedErr:   libHTTP.ErrContextNotFound,
			expectedErrIn: "context not found",
		},
		{
			name:        "context found but ID mismatch returns libHTTP.ErrContextNotOwned",
			verifier:    NewTenantOwnershipVerifier(newMockContextProviderWithIDMismatch(differentContextID)),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: libHTTP.ErrContextNotOwned,
		},
		{
			name:          "context found but inactive returns libHTTP.ErrContextNotActive",
			verifier:      NewTenantOwnershipVerifier(newMockContextProviderInactive()),
			tenantID:      validTenantID,
			contextID:     validContextID,
			expectedErr:   libHTTP.ErrContextNotActive,
			expectedErrIn: "context is paused or disabled",
		},
		{
			name: "draft context (not active) returns libHTTP.ErrContextNotActive",
			verifier: NewTenantOwnershipVerifier(&mockContextProvider{
				findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
					return &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: false, // DRAFT contexts are not active
					}, nil
				},
			}),
			tenantID:      validTenantID,
			contextID:     validContextID,
			expectedErr:   libHTTP.ErrContextNotActive,
			expectedErrIn: "context is paused or disabled",
		},
		{
			name: "archived context (not active) returns libHTTP.ErrContextNotActive",
			verifier: NewTenantOwnershipVerifier(&mockContextProvider{
				findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
					return &ports.ReconciliationContextInfo{
						ID:     contextID,
						Active: false, // ARCHIVED contexts are not active
					}, nil
				},
			}),
			tenantID:      validTenantID,
			contextID:     validContextID,
			expectedErr:   libHTTP.ErrContextNotActive,
			expectedErrIn: "context is paused or disabled",
		},
		{
			name:        "success case returns nil error",
			verifier:    NewTenantOwnershipVerifier(newMockContextProviderSuccess()),
			tenantID:    validTenantID,
			contextID:   validContextID,
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.verifier(context.Background(), tt.tenantID, tt.contextID)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedErr)

				if tt.expectedErrIn != "" {
					assert.Contains(t, err.Error(), tt.expectedErrIn)
				}
			} else {
				assert.NoError(t, err)
			}
		})
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
			findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
				return &ports.ReconciliationContextInfo{
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

func TestTenantOwnershipVerifier_ImplementsInterface(t *testing.T) {
	t.Parallel()

	// Verify that mockContextProvider implements ports.ContextProvider.
	var _ ports.ContextProvider = (*mockContextProvider)(nil)
}

func TestTenantOwnershipVerifier_PassesCorrectParameters(t *testing.T) {
	t.Parallel()

	expectedTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	expectedContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	var capturedTenantID, capturedContextID uuid.UUID

	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			capturedTenantID = tenantID
			capturedContextID = contextID

			return &ports.ReconciliationContextInfo{
				ID:     expectedContextID,
				Active: true,
			}, nil
		},
	}

	verifier := NewTenantOwnershipVerifier(mock)
	err := verifier(context.Background(), expectedTenantID, expectedContextID)

	require.NoError(t, err)
	assert.Equal(t, expectedTenantID, capturedTenantID)
	assert.Equal(t, expectedContextID, capturedContextID)
}

// mockTenantExtractor creates a tenant extractor function that returns a fixed tenant ID string.
func mockTenantExtractor(tenantID string) func(context.Context) string {
	return func(_ context.Context) string { return tenantID }
}

func TestNewResourceContextVerifier(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	differentContextID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	tests := []struct {
		name            string
		contextProvider ports.ContextProvider
		tenantExtractor func(context.Context) string
		contextID       uuid.UUID
		expectedErr     error
		expectedErrIn   string
	}{
		{
			name:            "nil context provider returns ErrContextAccessDenied",
			contextProvider: nil,
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextAccessDenied,
			expectedErrIn:   "not initialized",
		},
		{
			name:            "nil tenant extractor returns ErrContextAccessDenied",
			contextProvider: newMockContextProviderSuccess(),
			tenantExtractor: nil,
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextAccessDenied,
			expectedErrIn:   "tenant extractor not initialized",
		},
		{
			name:            "empty tenant ID from extractor returns ErrContextAccessDenied",
			contextProvider: newMockContextProviderSuccess(),
			tenantExtractor: mockTenantExtractor(""),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextAccessDenied,
			expectedErrIn:   "invalid tenant id",
		},
		{
			name:            "invalid tenant ID format returns ErrContextAccessDenied",
			contextProvider: newMockContextProviderSuccess(),
			tenantExtractor: mockTenantExtractor("not-a-uuid"),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextAccessDenied,
			expectedErrIn:   "invalid tenant id",
		},
		{
			name:            "context not found from repository error returns ErrContextNotFound",
			contextProvider: newMockContextProviderNotFound(),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotFound,
		},
		{
			name:            "context nil result returns ErrContextNotFound",
			contextProvider: newMockContextProviderReturnsNil(),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotFound,
			expectedErrIn:   "context not found",
		},
		{
			name:            "context ID mismatch returns ErrContextNotOwned",
			contextProvider: newMockContextProviderWithIDMismatch(differentContextID),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotOwned,
		},
		{
			name:            "context inactive returns ErrContextNotActive",
			contextProvider: newMockContextProviderInactive(),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotActive,
			expectedErrIn:   "context is paused or disabled",
		},
		{
			name:            "success case returns nil error",
			contextProvider: newMockContextProviderSuccess(),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     nil,
		},
		{
			name:            "infrastructure error propagation wraps underlying error",
			contextProvider: newMockContextProviderWithError(),
			tenantExtractor: mockTenantExtractor(validTenantID.String()),
			contextID:       validContextID,
			expectedErr:     errMockContextProvider,
			expectedErrIn:   "verify context ownership",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verifier := NewResourceContextVerifier(tt.contextProvider, tt.tenantExtractor)
			err := verifier(context.Background(), tt.contextID)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedErr)

				if tt.expectedErrIn != "" {
					assert.Contains(t, err.Error(), tt.expectedErrIn)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestNewResourceContextVerifier_PassesCorrectParameters(t *testing.T) {
	t.Parallel()

	expectedTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	expectedContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	var capturedTenantID, capturedContextID uuid.UUID

	mock := &mockContextProvider{
		findByIDFunc: func(ctx context.Context, tenantID, contextID uuid.UUID) (*ports.ReconciliationContextInfo, error) {
			capturedTenantID = tenantID
			capturedContextID = contextID

			return &ports.ReconciliationContextInfo{
				ID:     expectedContextID,
				Active: true,
			}, nil
		},
	}

	verifier := NewResourceContextVerifier(mock, mockTenantExtractor(expectedTenantID.String()))
	err := verifier(context.Background(), expectedContextID)

	require.NoError(t, err)
	assert.Equal(t, expectedTenantID, capturedTenantID)
	assert.Equal(t, expectedContextID, capturedContextID)
}

func TestVerifyContextOwnership(t *testing.T) {
	t.Parallel()

	validTenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	validContextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	differentContextID := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	tests := []struct {
		name            string
		contextProvider ports.ContextProvider
		tenantID        uuid.UUID
		contextID       uuid.UUID
		expectedErr     error
		expectedErrIn   string
	}{
		{
			name:            "repository returns not found error",
			contextProvider: newMockContextProviderNotFound(),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotFound,
		},
		{
			name:            "repository returns infrastructure error",
			contextProvider: newMockContextProviderWithError(),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     errMockContextProvider,
			expectedErrIn:   "verify context ownership",
		},
		{
			name:            "nil context result",
			contextProvider: newMockContextProviderReturnsNil(),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotFound,
			expectedErrIn:   "context not found",
		},
		{
			name:            "context ID mismatch",
			contextProvider: newMockContextProviderWithIDMismatch(differentContextID),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotOwned,
		},
		{
			name:            "context not active",
			contextProvider: newMockContextProviderInactive(),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     libHTTP.ErrContextNotActive,
			expectedErrIn:   "context is paused or disabled",
		},
		{
			name:            "success",
			contextProvider: newMockContextProviderSuccess(),
			tenantID:        validTenantID,
			contextID:       validContextID,
			expectedErr:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := verifyContextOwnership(context.Background(), tt.contextProvider, tt.tenantID, tt.contextID)

			if tt.expectedErr != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedErr)

				if tt.expectedErrIn != "" {
					assert.Contains(t, err.Error(), tt.expectedErrIn)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
