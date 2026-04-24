// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

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

func (m *mockInfrastructureProvider) GetRedisConnection(
	_ context.Context,
) (*RedisConnectionLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) BeginTx(_ context.Context) (*TxLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetReplicaDB(_ context.Context) (*DBLease, error) {
	return nil, nil
}

func (m *mockInfrastructureProvider) GetPrimaryDB(_ context.Context) (*DBLease, error) {
	return nil, nil
}

func TestInfrastructureProviderInterface(t *testing.T) {
	t.Parallel()

	var provider InfrastructureProvider = &mockInfrastructureProvider{}

	redisConn, err := provider.GetRedisConnection(context.Background())
	require.NoError(t, err)
	assert.Nil(t, redisConn)

	tx, err := provider.BeginTx(context.Background())
	require.NoError(t, err)
	assert.Nil(t, tx)

	replicaDB, err := provider.GetReplicaDB(context.Background())
	require.NoError(t, err)
	assert.Nil(t, replicaDB)

	primaryDB, err := provider.GetPrimaryDB(context.Background())
	require.NoError(t, err)
	assert.Nil(t, primaryDB)
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
