// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Bridge worker wiring for the Fetcher extraction-to-ingestion pipeline.
// Split from init_fetcher_bridge.go so the worker construction, its
// orchestrator configuration, and the Redis-backed heartbeat wiring live
// in one place. The bundle consumed here is produced by
// init_fetcher_adapters.go; the HTTP client and custody retention worker
// live in sibling files.

package bootstrap

import (
	"context"
	"errors"
	"fmt"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	"github.com/LerianStudio/matcher/internal/discovery/adapters/fetcher"
	discoveryExtractionRepo "github.com/LerianStudio/matcher/internal/discovery/adapters/postgres/extraction"
	discoveryRedis "github.com/LerianStudio/matcher/internal/discovery/adapters/redis"
	discoveryCommand "github.com/LerianStudio/matcher/internal/discovery/services/command"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	crossAdapters "github.com/LerianStudio/matcher/internal/shared/adapters/cross"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// errRedisConnectionLeaseNil indicates the infrastructure provider returned
// a nil Redis connection lease without an accompanying error. Package-private
// sentinel — only resolveBridgeHeartbeat can produce this state.
var errRedisConnectionLeaseNil = errors.New("redis connection lease is nil")

// initFetcherBridgeWorker constructs the T-003 bridge worker when all
// preconditions are satisfied. Returns (nil, nil) only when Fetcher is
// disabled (cfg.Fetcher.Enabled=false). When Fetcher is enabled but any
// upstream dependency (bundle, provider, extraction repo) is nil, this
// function returns ErrFetcherBridgeNotOperational so operators see the
// integration bug at startup instead of silently running without a bridge.
//
// T-003 P4/P5 hardening: Fetcher-enabled deployments MUST fail loudly
// when wiring is incomplete. The caller propagates the error as a
// bootstrap failure.
func initFetcherBridgeWorker(
	ctx context.Context,
	cfg *Config,
	configGetter func() *Config,
	provider sharedPorts.InfrastructureProvider,
	extractionRepo *discoveryExtractionRepo.Repository,
	tenantLister sharedPorts.TenantLister,
	bundle *FetcherBridgeAdapters,
	logger libLog.Logger,
) (*discoveryWorker.BridgeWorker, error) {
	if cfg == nil || !cfg.Fetcher.Enabled {
		return nil, nil
	}

	if bundle == nil {
		return nil, fmt.Errorf("%w: bridge adapter bundle is nil", ErrFetcherBridgeNotOperational)
	}

	if err := EnsureBridgeOperational(bundle); err != nil {
		// Fetcher is enabled but bundle is incomplete — this is a hard
		// failure so operators see the misconfiguration at startup.
		return nil, err
	}

	if provider == nil {
		return nil, fmt.Errorf("%w: infrastructure provider is nil", ErrFetcherBridgeNotOperational)
	}

	if extractionRepo == nil {
		return nil, fmt.Errorf("%w: extraction repository is nil", ErrFetcherBridgeNotOperational)
	}

	sourceResolver, err := crossAdapters.NewBridgeSourceResolverAdapter(provider)
	if err != nil {
		return nil, fmt.Errorf("create bridge source resolver: %w", err)
	}

	orchestratorCfg := discoveryCommand.BridgeOrchestratorConfig{
		FetcherBaseURLGetter: func() string {
			if configGetter == nil {
				return cfg.Fetcher.URL
			}

			if currentCfg := configGetter(); currentCfg != nil {
				return currentCfg.Fetcher.URL
			}

			return cfg.Fetcher.URL
		},
		MaxExtractionBytes: cfg.FetcherMaxExtractionBytes(),
		Flatten:            fetcher.FlattenFetcherJSON,
	}

	orchestrator, err := discoveryCommand.NewBridgeExtractionOrchestrator(
		extractionRepo,
		bundle.VerifiedArtifactOrchestrator,
		bundle.ArtifactCustody,
		bundle.Intake,
		bundle.LinkWrite,
		sourceResolver,
		orchestratorCfg,
	)
	if err != nil {
		return nil, fmt.Errorf("create bridge orchestrator: %w", err)
	}

	worker, err := discoveryWorker.NewBridgeWorker(
		orchestrator,
		extractionRepo,
		tenantLister,
		provider,
		discoveryWorker.BridgeWorkerConfig{
			Interval:          cfg.FetcherBridgeInterval(),
			BatchSize:         cfg.FetcherBridgeBatchSize(),
			TenantConcurrency: cfg.FetcherBridgeTenantConcurrency(),
			Retry: discoveryWorker.BridgeRetryBackoff{
				MaxAttempts: cfg.FetcherBridgeRetryMaxAttempts(),
			},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("create bridge worker: %w", err)
	}

	wireBridgeHeartbeatWriter(ctx, provider, worker, logger)

	logger.Log(ctx, libLog.LevelInfo,
		fmt.Sprintf("fetcher bridge worker wired (interval=%s batch=%d tenant_concurrency=%d)",
			cfg.FetcherBridgeInterval(), cfg.FetcherBridgeBatchSize(), cfg.FetcherBridgeTenantConcurrency()))

	return worker, nil
}

// wireBridgeHeartbeatWriter resolves the Redis client from the shared
// infrastructure provider and plumbs a BridgeHeartbeatWriter into the
// bridge worker. Non-fatal on failure — the bridge must still run when
// Redis is momentarily unavailable at boot; it will simply not emit
// heartbeats until the operator addresses the underlying issue. C15.
func wireBridgeHeartbeatWriter(
	ctx context.Context,
	provider sharedPorts.InfrastructureProvider,
	worker *discoveryWorker.BridgeWorker,
	logger libLog.Logger,
) {
	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if worker == nil || provider == nil {
		return
	}

	writer, err := resolveBridgeHeartbeat(ctx, provider)
	if err != nil {
		logger.Log(ctx, libLog.LevelWarn,
			fmt.Sprintf("bridge heartbeat writer not wired: %v", err))

		return
	}

	worker.WithHeartbeatWriter(writer)

	logger.Log(ctx, libLog.LevelInfo, "bridge heartbeat writer wired")
}

// resolveBridgeHeartbeat constructs the Redis-backed heartbeat adapter.
// Exported at package level so the query-side wiring (init_discovery.go)
// can reuse the exact same construction site and key contract.
//
// Returns the concrete *discoveryRedis.BridgeHeartbeat which satisfies
// both BridgeHeartbeatWriter and BridgeHeartbeatReader — callers pick
// whichever port their dependency needs. Keeping one construction site
// guarantees the two sides agree on the Redis key and TTL format.
func resolveBridgeHeartbeat(
	ctx context.Context,
	provider sharedPorts.InfrastructureProvider,
) (*discoveryRedis.BridgeHeartbeat, error) {
	lease, leaseErr := provider.GetRedisConnection(ctx)
	if leaseErr != nil {
		return nil, fmt.Errorf("get redis connection: %w", leaseErr)
	}

	if lease == nil {
		return nil, fmt.Errorf("resolve bridge heartbeat: %w", errRedisConnectionLeaseNil)
	}

	defer lease.Release()

	client, clientErr := lease.GetClient(ctx)
	if clientErr != nil {
		return nil, fmt.Errorf("get redis client: %w", clientErr)
	}

	hb, hbErr := discoveryRedis.NewBridgeHeartbeat(client)
	if hbErr != nil {
		return nil, fmt.Errorf("construct bridge heartbeat adapter: %w", hbErr)
	}

	return hb, nil
}
