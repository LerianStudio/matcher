//go:build unit

// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
)

// Compile-time interface satisfaction check.
var _ governanceWorker.PartitionManager = (*dynamicPartitionManager)(nil)

func TestDynamicPartitionManager_ImplementsPartitionManager(t *testing.T) {
	t.Parallel()

	var pm governanceWorker.PartitionManager = &dynamicPartitionManager{}
	assert.NotNil(t, pm)
}

func TestDynamicPartitionManager_EnsurePartitionsExist_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	err := manager.EnsurePartitionsExist(context.Background(), 3)

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_EnsurePartitionsExist_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	err := manager.EnsurePartitionsExist(context.Background(), 3)

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_ListPartitions_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	partitions, err := manager.ListPartitions(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
	assert.Nil(t, partitions)
}

func TestDynamicPartitionManager_ListPartitions_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	partitions, err := manager.ListPartitions(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
	assert.Nil(t, partitions)
}

// Tests for DetachPartition / DropPartition (non-WithTx) were removed when
// those methods were deleted as Ring-2 orphans on the dynamicPartitionManager
// wrapper. Their WithTx counterparts route through the same `current()`
// resolver, so the nil-manager / nil-provider invariants are exercised both
// indirectly (via the Current tests below) and directly (via the four
// DetachPartitionWithTx / DropPartitionWithTx tests immediately below) so a
// future refactor that bypasses `current()` cannot silently regress the
// guard.

func TestDynamicPartitionManager_DetachPartitionWithTx_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	err := manager.DetachPartitionWithTx(context.Background(), nil, "audit_log_p2026_05")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DetachPartitionWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	err := manager.DetachPartitionWithTx(context.Background(), nil, "audit_log_p2026_05")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DropPartitionWithTx_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	err := manager.DropPartitionWithTx(context.Background(), nil, "audit_log_p2026_05")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DropPartitionWithTx_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	err := manager.DropPartitionWithTx(context.Background(), nil, "audit_log_p2026_05")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_Current_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	delegate, release, err := manager.current(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
	assert.Nil(t, delegate)
	assert.Nil(t, release)
}

func TestDynamicPartitionManager_Current_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	delegate, release, err := manager.current(context.Background())

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
	assert.Nil(t, delegate)
	assert.Nil(t, release)
}

func TestNewDynamicPartitionManager_NilDeps_ReturnsError(t *testing.T) {
	t.Parallel()

	// newDynamicPartitionManager must fail fast when dependencies are nil.
	pm, err := newDynamicPartitionManager(nil, nil, nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerNilDependency)
	assert.Nil(t, pm)
}

func TestErrPartitionManagerProviderUnavailable_Message(t *testing.T) {
	t.Parallel()

	assert.Contains(t, errPartitionManagerProviderUnavailable.Error(), "partition manager")
	assert.Contains(t, errPartitionManagerProviderUnavailable.Error(), "infrastructure provider not available")
}
