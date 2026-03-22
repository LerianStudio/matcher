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

func TestDynamicPartitionManager_DetachPartition_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	err := manager.DetachPartition(context.Background(), "audit_logs_2024_01")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DetachPartition_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	err := manager.DetachPartition(context.Background(), "audit_logs_2024_01")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DropPartition_NilManager(t *testing.T) {
	t.Parallel()

	var manager *dynamicPartitionManager

	err := manager.DropPartition(context.Background(), "audit_logs_2024_01")

	require.Error(t, err)
	assert.ErrorIs(t, err, errPartitionManagerProviderUnavailable)
}

func TestDynamicPartitionManager_DropPartition_NilProvider(t *testing.T) {
	t.Parallel()

	manager := &dynamicPartitionManager{provider: nil}

	err := manager.DropPartition(context.Background(), "audit_logs_2024_01")

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
