//go:build unit

package command

import (
	"context"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// mockNilConnProvider returns nil connection with no error.
type mockNilConnProvider struct{}

var _ sharedPorts.InfrastructureProvider = (*mockNilConnProvider)(nil)

func (m *mockNilConnProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, nil
}

func (m *mockNilConnProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockNilConnProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, nil
}

func (m *mockNilConnProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
	return nil, nil
}

// mockErrProvider returns a configurable error from GetPostgresConnection.
type mockErrProvider struct {
	err error
}

var _ sharedPorts.InfrastructureProvider = (*mockErrProvider)(nil)

func (m *mockErrProvider) GetPostgresConnection(_ context.Context) (*sharedPorts.PostgresConnectionLease, error) {
	return nil, m.err
}

func (m *mockErrProvider) GetRedisConnection(_ context.Context) (*sharedPorts.RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockErrProvider) BeginTx(_ context.Context) (*sharedPorts.TxLease, error) {
	return nil, m.err
}

func (m *mockErrProvider) GetReplicaDB(_ context.Context) (*sharedPorts.ReplicaDBLease, error) {
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
	assert.Contains(t, err.Error(), "begin tenant transaction")

	cancel()
}

func TestLockSourceContextForShare_Success(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	sourceID := uuid.New()

	mock.ExpectExec(`SELECT 1 FROM reconciliation_contexts WHERE id = \$1 FOR SHARE`).
		WithArgs(sourceID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = lockSourceContextForShare(context.Background(), tx, sourceID)
	require.NoError(t, err)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestLockSourceContextForShare_Error(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	defer db.Close()

	mock.ExpectBegin()

	tx, err := db.Begin()
	require.NoError(t, err)

	sourceID := uuid.New()

	mock.ExpectExec(`SELECT 1 FROM reconciliation_contexts WHERE id = \$1 FOR SHARE`).
		WithArgs(sourceID).
		WillReturnError(assert.AnError)

	err = lockSourceContextForShare(context.Background(), tx, sourceID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lock source context for share")

	require.NoError(t, mock.ExpectationsWereMet())
}
