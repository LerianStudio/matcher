//go:build unit

package http

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedhttp "github.com/LerianStudio/lib-commons/v4/commons/net/http"
)

var errDatabaseFailure = errors.New("database error")

type verifierMockContextProvider struct {
	findByIDFunc func(ctx context.Context, tenantID, contextID uuid.UUID) (*ReconciliationContextInfo, error)
}

func (m *verifierMockContextProvider) FindByID(
	ctx context.Context,
	tenantID, contextID uuid.UUID,
) (*ReconciliationContextInfo, error) {
	if m.findByIDFunc != nil {
		return m.findByIDFunc(ctx, tenantID, contextID)
	}

	return nil, nil
}

func TestNewTenantOwnershipVerifier(t *testing.T) {
	t.Parallel()

	provider := &verifierMockContextProvider{}
	verifier := NewTenantOwnershipVerifier(provider)

	assert.NotNil(t, verifier)
}

func TestTenantOwnershipVerifier_VerifyOwnership(t *testing.T) {
	t.Parallel()

	tenantID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	contextID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ctx := context.Background()

	tests := []struct {
		name          string
		verifier      sharedhttp.TenantOwnershipVerifier
		tenantID      uuid.UUID
		contextID     uuid.UUID
		expectedError error
	}{
		{
			name:          "nil provider",
			verifier:      NewTenantOwnershipVerifier(nil),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextAccessDenied,
		},
		{
			name: "context not owned error",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, _ uuid.UUID) (*ReconciliationContextInfo, error) {
					return nil, sharedhttp.ErrContextNotOwned
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextNotOwned,
		},
		{
			name: "context not found error",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, _ uuid.UUID) (*ReconciliationContextInfo, error) {
					return nil, sharedhttp.ErrContextNotFound
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextNotFound,
		},
		{
			name: "lookup failed error",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, _ uuid.UUID) (*ReconciliationContextInfo, error) {
					return nil, errDatabaseFailure
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextLookupFailed,
		},
		{
			name: "nil info returned",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, _ uuid.UUID) (*ReconciliationContextInfo, error) {
					return nil, nil
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextNotFound,
		},
		{
			name: "context ID mismatch",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, _ uuid.UUID) (*ReconciliationContextInfo, error) {
					return &ReconciliationContextInfo{
						ID:     uuid.MustParse("33333333-3333-3333-3333-333333333333"),
						Active: true,
					}, nil
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextNotOwned,
		},
		{
			name: "context not active",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, ctxID uuid.UUID) (*ReconciliationContextInfo, error) {
					return &ReconciliationContextInfo{
						ID:     ctxID,
						Active: false,
					}, nil
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: sharedhttp.ErrContextNotActive,
		},
		{
			name: "successful verification",
			verifier: NewTenantOwnershipVerifier(&verifierMockContextProvider{
				findByIDFunc: func(_ context.Context, _, ctxID uuid.UUID) (*ReconciliationContextInfo, error) {
					return &ReconciliationContextInfo{
						ID:     ctxID,
						Active: true,
					}, nil
				},
			}),
			tenantID:      tenantID,
			contextID:     contextID,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.verifier(ctx, tt.tenantID, tt.contextID)

			if tt.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedError)
			}
		})
	}
}
