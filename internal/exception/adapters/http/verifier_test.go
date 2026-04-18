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

var errDatabaseFailure = errors.New("database error")

type mockExceptionProvider struct {
	existsForTenantFunc func(ctx context.Context, id uuid.UUID) (bool, error)
}

func (m *mockExceptionProvider) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if m.existsForTenantFunc != nil {
		return m.existsForTenantFunc(ctx, id)
	}

	return false, nil
}

type mockDisputeProvider struct {
	existsForTenantFunc func(ctx context.Context, id uuid.UUID) (bool, error)
}

func (m *mockDisputeProvider) ExistsForTenant(ctx context.Context, id uuid.UUID) (bool, error) {
	if m.existsForTenantFunc != nil {
		return m.existsForTenantFunc(ctx, id)
	}

	return false, nil
}

func TestNewExceptionOwnershipVerifier(t *testing.T) {
	t.Parallel()

	provider := &mockExceptionProvider{}
	verifier := NewExceptionOwnershipVerifier(provider)

	assert.NotNil(t, verifier)
}

func TestExceptionOwnershipVerifier_VerifyOwnership(t *testing.T) {
	t.Parallel()

	exceptionID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	ctx := context.Background()

	tests := []struct {
		name          string
		verifier      libHTTP.ResourceOwnershipVerifier
		expectedError error
	}{
		{
			name:          "nil provider returns ErrExceptionAccessDenied",
			verifier:      NewExceptionOwnershipVerifier(nil),
			expectedError: ErrExceptionAccessDenied,
		},
		{
			name: "exists true returns nil",
			verifier: NewExceptionOwnershipVerifier(&mockExceptionProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return true, nil
				},
			}),
			expectedError: nil,
		},
		{
			name: "exists false returns ErrExceptionNotFound",
			verifier: NewExceptionOwnershipVerifier(&mockExceptionProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return false, nil
				},
			}),
			expectedError: ErrExceptionNotFound,
		},
		{
			name: "provider error returns infrastructure error (not access denied)",
			verifier: NewExceptionOwnershipVerifier(&mockExceptionProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return false, errDatabaseFailure
				},
			}),
			expectedError: errDatabaseFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.verifier(ctx, exceptionID)

			if tt.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedError)

				// Infrastructure errors must NOT be wrapped as access-denied;
				// they should surface the original error so the classifier
				// maps them to ErrLookupFailed (500) rather than forbidden (403).
				if tt.name == "provider error returns infrastructure error (not access denied)" {
					require.False(t, errors.Is(err, ErrExceptionAccessDenied),
						"infrastructure errors must not be wrapped as access denied")
				}
			}
		})
	}
}

func TestNewDisputeOwnershipVerifier(t *testing.T) {
	t.Parallel()

	provider := &mockDisputeProvider{}
	verifier := NewDisputeOwnershipVerifier(provider)

	assert.NotNil(t, verifier)
}

func TestDisputeOwnershipVerifier_VerifyOwnership(t *testing.T) {
	t.Parallel()

	disputeID := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	ctx := context.Background()

	tests := []struct {
		name          string
		verifier      libHTTP.ResourceOwnershipVerifier
		expectedError error
	}{
		{
			name:          "nil provider returns ErrDisputeAccessDenied",
			verifier:      NewDisputeOwnershipVerifier(nil),
			expectedError: ErrDisputeAccessDenied,
		},
		{
			name: "exists true returns nil",
			verifier: NewDisputeOwnershipVerifier(&mockDisputeProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return true, nil
				},
			}),
			expectedError: nil,
		},
		{
			name: "exists false returns ErrDisputeNotFound",
			verifier: NewDisputeOwnershipVerifier(&mockDisputeProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return false, nil
				},
			}),
			expectedError: ErrDisputeNotFound,
		},
		{
			name: "provider error returns infrastructure error (not access denied)",
			verifier: NewDisputeOwnershipVerifier(&mockDisputeProvider{
				existsForTenantFunc: func(_ context.Context, _ uuid.UUID) (bool, error) {
					return false, errDatabaseFailure
				},
			}),
			expectedError: errDatabaseFailure,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.verifier(ctx, disputeID)

			if tt.expectedError == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.expectedError)

				// Infrastructure errors must NOT be wrapped as access-denied;
				// they should surface the original error so the classifier
				// maps them to ErrLookupFailed (500) rather than forbidden (403).
				if tt.name == "provider error returns infrastructure error (not access denied)" {
					require.False(t, errors.Is(err, ErrDisputeAccessDenied),
						"infrastructure errors must not be wrapped as access denied")
				}
			}
		})
	}
}
