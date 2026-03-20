// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	governanceCommand "github.com/LerianStudio/matcher/internal/governance/services/command"
	governanceWorker "github.com/LerianStudio/matcher/internal/governance/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type dynamicPartitionManager struct {
	provider sharedPorts.InfrastructureProvider
	logger   libLog.Logger
	tracer   trace.Tracer
}

var _ governanceWorker.PartitionManager = (*dynamicPartitionManager)(nil)

func newDynamicPartitionManager(
	provider sharedPorts.InfrastructureProvider,
	logger libLog.Logger,
	tracer trace.Tracer,
) governanceWorker.PartitionManager {
	return &dynamicPartitionManager{provider: provider, logger: logger, tracer: tracer}
}

// EnsurePartitionsExist delegates partition provisioning to a runtime-backed manager.
func (manager *dynamicPartitionManager) EnsurePartitionsExist(ctx context.Context, lookaheadMonths int) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return fmt.Errorf("resolve partition manager for ensure partitions: %w", err)
	}
	defer release()

	if err := delegate.EnsurePartitionsExist(ctx, lookaheadMonths); err != nil {
		return fmt.Errorf("ensure partitions exist: %w", err)
	}

	return nil
}

// ListPartitions delegates partition listing to a runtime-backed manager.
func (manager *dynamicPartitionManager) ListPartitions(ctx context.Context) ([]governanceCommand.PartitionInfo, error) {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve partition manager for list partitions: %w", err)
	}
	defer release()

	partitions, err := delegate.ListPartitions(ctx)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}

	return partitions, nil
}

// DetachPartition delegates partition detachment to a runtime-backed manager.
func (manager *dynamicPartitionManager) DetachPartition(ctx context.Context, name string) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return fmt.Errorf("resolve partition manager for detach partition: %w", err)
	}
	defer release()

	if err := delegate.DetachPartition(ctx, name); err != nil {
		return fmt.Errorf("detach partition %q: %w", name, err)
	}

	return nil
}

// DropPartition delegates partition deletion to a runtime-backed manager.
func (manager *dynamicPartitionManager) DropPartition(ctx context.Context, name string) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return fmt.Errorf("resolve partition manager for drop partition: %w", err)
	}
	defer release()

	if err := delegate.DropPartition(ctx, name); err != nil {
		return fmt.Errorf("drop partition %q: %w", name, err)
	}

	return nil
}

func (manager *dynamicPartitionManager) current(ctx context.Context) (*governanceCommand.PartitionManager, func(), error) {
	lease, err := manager.provider.GetPostgresConnection(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get postgres connection for partition manager: %w", err)
	}

	client := lease.Connection()
	if client == nil {
		lease.Release()
		return nil, nil, governanceCommand.ErrNilDB
	}

	resolver, err := client.Resolver(ctx)
	if err != nil {
		lease.Release()
		return nil, nil, fmt.Errorf("resolve postgres connection for partition manager: %w", err)
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		lease.Release()
		return nil, nil, governanceCommand.ErrNilDB
	}

	delegate, err := governanceCommand.NewPartitionManager(primaryDBs[0], manager.logger, manager.tracer)
	if err != nil {
		lease.Release()
		return nil, nil, fmt.Errorf("create partition manager: %w", err)
	}

	return delegate, lease.Release, nil
}
