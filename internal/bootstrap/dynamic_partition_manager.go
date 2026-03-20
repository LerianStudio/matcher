// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"

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

func (manager *dynamicPartitionManager) EnsurePartitionsExist(ctx context.Context, lookaheadMonths int) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return err
	}
	defer release()

	return delegate.EnsurePartitionsExist(ctx, lookaheadMonths)
}

func (manager *dynamicPartitionManager) ListPartitions(ctx context.Context) ([]governanceCommand.PartitionInfo, error) {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	return delegate.ListPartitions(ctx)
}

func (manager *dynamicPartitionManager) DetachPartition(ctx context.Context, name string) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return err
	}
	defer release()

	return delegate.DetachPartition(ctx, name)
}

func (manager *dynamicPartitionManager) DropPartition(ctx context.Context, name string) error {
	delegate, release, err := manager.current(ctx)
	if err != nil {
		return err
	}
	defer release()

	return delegate.DropPartition(ctx, name)
}

func (manager *dynamicPartitionManager) current(ctx context.Context) (*governanceCommand.PartitionManager, func(), error) {
	lease, err := manager.provider.GetPostgresConnection(ctx)
	if err != nil {
		return nil, nil, err
	}

	client := lease.Connection()
	if client == nil {
		lease.Release()
		return nil, nil, governanceCommand.ErrNilDB
	}

	resolver, err := client.Resolver(ctx)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}

	primaryDBs := resolver.PrimaryDBs()
	if len(primaryDBs) == 0 || primaryDBs[0] == nil {
		lease.Release()
		return nil, nil, governanceCommand.ErrNilDB
	}

	delegate, err := governanceCommand.NewPartitionManager(primaryDBs[0], manager.logger, manager.tracer)
	if err != nil {
		lease.Release()
		return nil, nil, err
	}

	return delegate, lease.Release, nil
}
