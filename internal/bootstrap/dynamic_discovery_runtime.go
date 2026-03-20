// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"

	discoveryRedis "github.com/LerianStudio/matcher/internal/discovery/adapters/redis"
	discoveryRepos "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	discoveryPorts "github.com/LerianStudio/matcher/internal/discovery/ports"
	discoveryWorker "github.com/LerianStudio/matcher/internal/discovery/services/worker"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

type providerBackedSchemaCache struct {
	provider          sharedPorts.InfrastructureProvider
	allowTenantPrefix bool
}

func newProviderBackedSchemaCache(provider sharedPorts.InfrastructureProvider, allowTenantPrefix bool) discoveryPorts.SchemaCache {
	if provider == nil {
		return nil
	}

	return &providerBackedSchemaCache{provider: provider, allowTenantPrefix: allowTenantPrefix}
}

// GetSchema retrieves cached schema data using the current provider-backed Redis client.
func (cache *providerBackedSchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	delegate, release, err := cache.currentCache(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve provider-backed schema cache: %w", err)
	}
	defer release()

	schema, err := delegate.GetSchema(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("get schema from provider-backed cache: %w", err)
	}

	return schema, nil
}

// SetSchema stores schema data using the current provider-backed Redis client.
func (cache *providerBackedSchemaCache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	delegate, release, err := cache.currentCache(ctx)
	if err != nil {
		return fmt.Errorf("resolve provider-backed schema cache: %w", err)
	}
	defer release()

	if err := delegate.SetSchema(ctx, connectionID, schema, ttl); err != nil {
		return fmt.Errorf("set schema in provider-backed cache: %w", err)
	}

	return nil
}

// InvalidateSchema removes schema data using the current provider-backed Redis client.
func (cache *providerBackedSchemaCache) InvalidateSchema(ctx context.Context, connectionID string) error {
	delegate, release, err := cache.currentCache(ctx)
	if err != nil {
		return fmt.Errorf("resolve provider-backed schema cache: %w", err)
	}
	defer release()

	if err := delegate.InvalidateSchema(ctx, connectionID); err != nil {
		return fmt.Errorf("invalidate schema in provider-backed cache: %w", err)
	}

	return nil
}

func (cache *providerBackedSchemaCache) currentCache(ctx context.Context) (discoveryPorts.SchemaCache, func(), error) {
	lease, err := cache.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("get redis connection for schema cache: %w", err)
	}

	redisConn := lease.Connection()
	if redisConn == nil {
		lease.Release()
		return nil, nil, discoveryWorker.ErrRedisClientNil
	}

	redisClient, err := redisConn.GetClient(ctx)
	if err != nil {
		lease.Release()
		return nil, nil, fmt.Errorf("get redis client for schema cache: %w", err)
	}

	schemaCache, err := discoveryRedis.NewSchemaCache(redisClient, cache.allowTenantPrefix)
	if err != nil {
		lease.Release()
		return nil, nil, fmt.Errorf("create schema cache: %w", err)
	}

	return schemaCache, lease.Release, nil
}

type dynamicSchemaCache struct {
	inner     discoveryPorts.SchemaCache
	ttlGetter func() time.Duration
}

func newDynamicSchemaCache(inner discoveryPorts.SchemaCache, ttlGetter func() time.Duration) discoveryPorts.SchemaCache {
	if inner == nil || ttlGetter == nil {
		return inner
	}

	return &dynamicSchemaCache{inner: inner, ttlGetter: ttlGetter}
}

// GetSchema retrieves cached schema data using the current dynamic TTL configuration.
func (cache *dynamicSchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	schema, err := cache.inner.GetSchema(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("get schema from dynamic cache: %w", err)
	}

	return schema, nil
}

// SetSchema stores schema data using the current dynamic TTL configuration.
func (cache *dynamicSchemaCache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	if cache.ttlGetter != nil {
		if currentTTL := cache.ttlGetter(); currentTTL > 0 {
			ttl = currentTTL
		}
	}

	if err := cache.inner.SetSchema(ctx, connectionID, schema, ttl); err != nil {
		return fmt.Errorf("set schema in dynamic cache: %w", err)
	}

	return nil
}

// InvalidateSchema removes schema data using the current dynamic TTL configuration.
func (cache *dynamicSchemaCache) InvalidateSchema(ctx context.Context, connectionID string) error {
	if err := cache.inner.InvalidateSchema(ctx, connectionID); err != nil {
		return fmt.Errorf("invalidate schema in dynamic cache: %w", err)
	}

	return nil
}

type dynamicExtractionPoller struct {
	fetcherClient  sharedPorts.FetcherClient
	extractionRepo discoveryRepos.ExtractionRepository
	configGetter   func() discoveryWorker.ExtractionPollerConfig
	logger         libLog.Logger
}

func newDynamicExtractionPoller(
	fetcherClient sharedPorts.FetcherClient,
	extractionRepo discoveryRepos.ExtractionRepository,
	configGetter func() discoveryWorker.ExtractionPollerConfig,
	logger libLog.Logger,
) discoveryPorts.ExtractionJobPoller {
	if configGetter == nil {
		return nil
	}

	return &dynamicExtractionPoller{
		fetcherClient:  fetcherClient,
		extractionRepo: extractionRepo,
		configGetter:   configGetter,
		logger:         logger,
	}
}

// PollUntilComplete delegates polling to a freshly built extraction poller.
func (poller *dynamicExtractionPoller) PollUntilComplete(
	ctx context.Context,
	extractionID uuid.UUID,
	onComplete func(ctx context.Context, resultPath string) error,
	onFailed func(ctx context.Context, errMsg string),
) {
	if poller == nil {
		if onFailed != nil {
			onFailed(ctx, "extraction poller unavailable")
		}

		return
	}

	delegate, err := discoveryWorker.NewExtractionPoller(
		poller.fetcherClient,
		poller.extractionRepo,
		poller.configGetter(),
		poller.logger,
	)
	if err != nil {
		if onFailed != nil {
			onFailed(ctx, err.Error())
		}

		return
	}

	delegate.PollUntilComplete(ctx, extractionID, onComplete, onFailed)
}
