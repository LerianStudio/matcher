//go:build unit

package command

import (
	"context"
	"database/sql"
	"testing"

	libPostgres "github.com/LerianStudio/lib-commons/v4/commons/postgres"
	libRedis "github.com/LerianStudio/lib-commons/v4/commons/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockNilConnProvider returns nil connection with no error.
type mockNilConnProvider struct{}

var _ sharedPorts.InfrastructureProvider = (*mockNilConnProvider)(nil)

func (m *mockNilConnProvider) GetPostgresConnection(_ context.Context) (*libPostgres.Client, error) {
	return nil, nil
}

func (m *mockNilConnProvider) GetRedisConnection(_ context.Context) (*libRedis.Client, error) {
	return nil, nil
}

func (m *mockNilConnProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (m *mockNilConnProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

// mockErrProvider returns a configurable error from GetPostgresConnection.
type mockErrProvider struct {
	err error
}

var _ sharedPorts.InfrastructureProvider = (*mockErrProvider)(nil)

func (m *mockErrProvider) GetPostgresConnection(_ context.Context) (*libPostgres.Client, error) {
	return nil, m.err
}

func (m *mockErrProvider) GetRedisConnection(_ context.Context) (*libRedis.Client, error) {
	return nil, nil
}

func (m *mockErrProvider) BeginTx(_ context.Context) (*sql.Tx, error) {
	return nil, nil
}

func (m *mockErrProvider) GetReplicaDB(_ context.Context) (*sql.DB, error) {
	return nil, nil
}

func TestBeginTenantTx_NilProvider(t *testing.T) {
	t.Parallel()

	tx, cancel, err := beginTenantTx(context.Background(), nil)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	require.ErrorIs(t, err, ErrInlineCreateRequiresInfrastructure)

	// Calling cancel should not panic.
	cancel()
}

func TestBeginTenantTx_ProviderReturnsNilConnection(t *testing.T) {
	t.Parallel()

	provider := &mockNilConnProvider{}

	tx, cancel, err := beginTenantTx(context.Background(), provider)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	require.ErrorIs(t, err, ErrInlineCreateRequiresInfrastructure)

	cancel()
}

func TestBeginTenantTx_ProviderReturnsError(t *testing.T) {
	t.Parallel()

	provider := &mockErrProvider{err: assert.AnError}

	tx, cancel, err := beginTenantTx(context.Background(), provider)
	assert.Nil(t, tx)
	assert.NotNil(t, cancel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get postgres connection")

	cancel()
}

func TestErrNoPrimaryDatabase(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "no primary database configured for tenant transaction", errNoPrimaryDatabase.Error())
}

func TestDefaultBeginTxTimeout(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 30.0, defaultBeginTxTimeout.Seconds())
}
