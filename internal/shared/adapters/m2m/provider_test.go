//go:build unit

package m2m_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/matcher/internal/shared/adapters/m2m"
	"github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockSecretsClient is a test double for the AWS Secrets Manager client.
type mockSecretsClient struct {
	callCount atomic.Int64
	creds     *ports.M2MCredentials
	err       error
}

func (m *mockSecretsClient) GetM2MCredentials(
	_ context.Context,
	_, _, _, _ string,
) (*ports.M2MCredentials, error) {
	m.callCount.Add(1)

	if m.err != nil {
		return nil, m.err
	}

	return m.creds, nil
}

func TestM2MCredentialProvider_GetCredentials_Success(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
		},
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	creds, err := provider.GetCredentials(ctx, "tenant-1")
	require.NoError(t, err)
	assert.Equal(t, "test-client-id", creds.ClientID)
	assert.Equal(t, "test-client-secret", creds.ClientSecret)
	assert.Equal(t, int64(1), mock.callCount.Load())
}

func TestM2MCredentialProvider_L1CacheHit(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "cached-id",
			ClientSecret: "cached-secret",
		},
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	// First call: miss -> fetch from source
	creds1, err1 := provider.GetCredentials(ctx, "tenant-1")
	require.NoError(t, err1)
	assert.Equal(t, "cached-id", creds1.ClientID)

	// Second call: L1 cache hit -> no additional fetch
	creds2, err2 := provider.GetCredentials(ctx, "tenant-1")
	require.NoError(t, err2)
	assert.Equal(t, "cached-id", creds2.ClientID)

	// Only one AWS call should have been made
	assert.Equal(t, int64(1), mock.callCount.Load())
}

func TestM2MCredentialProvider_DifferentTenants(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "shared-id",
			ClientSecret: "shared-secret",
		},
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	_, err1 := provider.GetCredentials(ctx, "tenant-A")
	require.NoError(t, err1)

	_, err2 := provider.GetCredentials(ctx, "tenant-B")
	require.NoError(t, err2)

	// Two different tenants = two separate fetches
	assert.Equal(t, int64(2), mock.callCount.Load())
}

func TestM2MCredentialProvider_InvalidateCredentials(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "original-id",
			ClientSecret: "original-secret",
		},
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	// Populate cache
	_, err := provider.GetCredentials(ctx, "tenant-1")
	require.NoError(t, err)
	assert.Equal(t, int64(1), mock.callCount.Load())

	// Invalidate
	err = provider.InvalidateCredentials(ctx, "tenant-1")
	require.NoError(t, err)

	// Next call must re-fetch
	_, err = provider.GetCredentials(ctx, "tenant-1")
	require.NoError(t, err)
	assert.Equal(t, int64(2), mock.callCount.Load())
}

func TestM2MCredentialProvider_ErrorPropagation(t *testing.T) {
	t.Parallel()

	errNotFound := errors.New("credentials not found")

	mock := &mockSecretsClient{
		err: errNotFound,
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	creds, err := provider.GetCredentials(ctx, "tenant-1")
	assert.Nil(t, creds)
	assert.Error(t, err)
	assert.ErrorIs(t, err, errNotFound)
}

func TestM2MCredentialProvider_NilClient(t *testing.T) {
	t.Parallel()

	_, err := m2m.NewM2MCredentialProvider(
		nil, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MSecretsClientNil)
}

func TestM2MCredentialProvider_EmptyTenantOrgID(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "id",
			ClientSecret: "secret",
		},
	}

	provider, providerErr := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.NoError(t, providerErr)

	ctx := context.Background()

	creds, err := provider.GetCredentials(ctx, "")
	assert.Nil(t, creds)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tenant org ID is required")
}

func TestNewM2MCredentialProvider_EmptyEnv(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{}

	_, err := m2m.NewM2MCredentialProvider(
		mock, "", "matcher", "fetcher",
		5*time.Minute, nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MEnvRequired)
}

func TestNewM2MCredentialProvider_EmptyAppName(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{}

	_, err := m2m.NewM2MCredentialProvider(
		mock, "staging", "", "fetcher",
		5*time.Minute, nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MAppNameRequired)
}

func TestNewM2MCredentialProvider_EmptyTargetService(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{}

	_, err := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "",
		5*time.Minute, nil,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, m2m.ErrM2MTargetSvcRequired)
}

func TestNewM2MCredentialProvider_DefaultTTL(t *testing.T) {
	t.Parallel()

	mock := &mockSecretsClient{
		creds: &ports.M2MCredentials{
			ClientID:     "id",
			ClientSecret: "secret",
		},
	}

	// Zero TTL should not panic — constructor applies default
	provider, err := m2m.NewM2MCredentialProvider(
		mock, "staging", "matcher", "fetcher",
		0, nil,
	)
	require.NoError(t, err)
	assert.NotNil(t, provider)
}
