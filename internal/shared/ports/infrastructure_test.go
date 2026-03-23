//go:build unit

// Package ports defines shared interfaces (ports) for infrastructure abstraction.
package ports

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ InfrastructureProvider = (*mockInfrastructureProvider)(nil)

type mockInfrastructureProvider struct{}

func (m *mockInfrastructureProvider) GetPostgresConnection(
	_ context.Context,
) (*PostgresConnectionLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*TxLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*ReplicaDBLease, error) {
	return nil, nil
}

func TestInfrastructureProviderInterface(t *testing.T) {
	t.Parallel()

	var provider InfrastructureProvider = &mockInfrastructureProvider{}

	pgConn, err := provider.GetPostgresConnection(context.Background())
	require.NoError(t, err)
	assert.Nil(t, pgConn)

	redisConn, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	assert.Nil(t, redisConn)

	tx, err := provider.BeginTx(context.Background())
	require.NoError(t, err)
	assert.Nil(t, tx)

	replicaDB, err := provider.GetReplicaDB(context.Background())
	require.NoError(t, err)
	assert.Nil(t, replicaDB)
}

// Section: PostgresConnectionLease edge cases.

func TestNewPostgresConnectionLease_NilConn_ReturnsNil(t *testing.T) {
	t.Parallel()

	lease := NewPostgresConnectionLease(nil, func() {})
	assert.Nil(t, lease, "NewPostgresConnectionLease with nil conn must return nil")
}

func TestPostgresConnectionLease_Release_CalledTwice_IsIdempotent(t *testing.T) {
	t.Parallel()

	callCount := 0
	release := func() { callCount++ }

	// Use a non-nil conn via the internal struct (we're in package ports)
	lease := &PostgresConnectionLease{
		conn:    nil,
		release: release,
	}

	lease.releaseOnce.Do(lease.release)
	// Second call via Release() — sync.Once should prevent a second invocation
	lease.Release()

	assert.Equal(t, 1, callCount, "release function must be called exactly once (sync.Once)")
}

// Section: TxLease edge cases.

func TestNewTxLease_NilTx_ReturnsNil(t *testing.T) {
	t.Parallel()

	lease := NewTxLease(nil, func() {})
	assert.Nil(t, lease, "NewTxLease with nil tx must return nil")
}

func TestTxLease_CommitThenRollback_ReleasesOnce(t *testing.T) {
	t.Parallel()

	releaseCount := 0
	release := func() { releaseCount++ }

	// We can't easily create a real *sql.Tx in a unit test without a DB,
	// but we can test the release-once behavior by exercising the nil-tx path.
	// With nil tx, Commit and Rollback both call finish() which calls releaseOnce.Do.
	// The constructor returns nil for nil tx, so we construct manually.
	lease := &TxLease{
		tx:      nil,
		release: release,
	}

	// Commit on nil tx calls finish() -> releaseOnce.Do(release)
	err := lease.Commit()
	require.NoError(t, err)
	assert.Equal(t, 1, releaseCount, "Commit must trigger release")

	// Rollback after Commit — release must NOT fire again
	err = lease.Rollback()
	require.NoError(t, err)
	assert.Equal(t, 1, releaseCount, "Rollback after Commit must not call release again (sync.Once)")
}
