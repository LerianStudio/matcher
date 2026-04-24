// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package redis provides Redis-based adapters for the discovery bounded context.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v5/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

const (
	// schemaKeyPrefix includes a version segment so that cache entries written
	// with an earlier FetcherTableSchema JSON layout (different field tags) become
	// invisible after deploy. Old entries expire naturally via TTL.
	schemaKeyPrefix   = "matcher:discovery:schema:v2:"
	singleTenantScope = "single-tenant"
)

// safeKeyPattern matches only alphanumeric characters, hyphens, and underscores.
// This prevents Redis key injection via special characters (spaces, newlines, glob
// patterns like *, ?, etc.) when connection IDs originate from external systems.
var safeKeyPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// ErrUnsafeConnectionID indicates the connection ID contains characters that are
// not safe for use as a Redis key component.
var ErrUnsafeConnectionID = errors.New("connection ID contains unsafe characters for cache key")

// ErrUnsafeTenantID indicates the tenant ID contains characters that are not
// safe for use as a Redis key component.
var ErrUnsafeTenantID = errors.New("tenant ID contains unsafe characters for cache key")

// ErrTenantContextRequired indicates tenant-aware cache access was attempted without tenant context.
var ErrTenantContextRequired = errors.New("tenant context is required for schema cache access")

// SchemaCache provides Redis-backed caching for Fetcher discovery data.
type SchemaCache struct {
	client                    goredis.UniversalClient
	allowSingleTenantFallback bool
}

// schemaKey constructs a validated Redis key for a connection's schema.
func (cache *SchemaCache) tenantScopeFromContext(ctx context.Context) (string, error) {
	tenantID, hasTenantValue := ctx.Value(auth.TenantIDKey).(string)

	tenantID = strings.TrimSpace(tenantID)
	if cache != nil && !cache.allowSingleTenantFallback {
		if !hasTenantValue || tenantID == "" {
			return "", ErrTenantContextRequired
		}
	} else if tenantID == "" {
		tenantID = strings.TrimSpace(auth.GetTenantID(ctx))
	}

	if tenantID == "" {
		return singleTenantScope, nil
	}

	if !safeKeyPattern.MatchString(tenantID) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeTenantID, tenantID)
	}

	return tenantID, nil
}

func (cache *SchemaCache) schemaKey(ctx context.Context, connectionID string) (string, error) {
	if connectionID == "" {
		return "", ErrEmptyConnectionID
	}

	if !safeKeyPattern.MatchString(connectionID) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeConnectionID, connectionID)
	}

	tenantScope, err := cache.tenantScopeFromContext(ctx)
	if err != nil {
		return "", err
	}

	return schemaKeyPrefix + tenantScope + ":" + connectionID, nil
}

// Sentinel errors for the schema cache.
var (
	// ErrCacheMiss indicates the requested data is not in the cache.
	ErrCacheMiss = errors.New("cache miss")
	// ErrCacheNotInitialized indicates the cache is not properly initialized.
	ErrCacheNotInitialized = errors.New("schema cache not initialized")
	// ErrRedisClientRequired indicates a nil Redis client was provided.
	ErrRedisClientRequired = errors.New("redis client is required")
	// ErrEmptyConnectionID indicates an empty connection ID was provided for cache key.
	ErrEmptyConnectionID = errors.New("connection ID is required for schema cache key")
	// ErrSchemaRequired indicates a nil schema was provided for cache storage.
	ErrSchemaRequired = errors.New("schema is required for cache storage")
)

// tracerFromContext extracts the OpenTelemetry tracer from context.
func tracerFromContext(ctx context.Context) trace.Tracer {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // utility wrapper

	return tracer
}

// NewSchemaCache creates a new Redis schema cache.
func NewSchemaCache(client goredis.UniversalClient, allowSingleTenantFallback bool) (*SchemaCache, error) {
	if client == nil {
		return nil, ErrRedisClientRequired
	}

	return &SchemaCache{client: client, allowSingleTenantFallback: allowSingleTenantFallback}, nil
}

// GetSchema retrieves a cached schema for a specific connection.
func (cache *SchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if cache == nil || cache.client == nil {
		return nil, ErrCacheNotInitialized
	}

	tracer := tracerFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.get_schema")
	defer span.End()

	key, err := cache.schemaKey(ctx, connectionID)
	if err != nil {
		wrappedErr := fmt.Errorf("construct schema key: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to construct schema key", wrappedErr)

		return nil, wrappedErr
	}

	data, err := cache.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		wrappedErr := fmt.Errorf("get schema from cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get schema from cache", wrappedErr)

		return nil, wrappedErr
	}

	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		return nil, ErrCacheMiss
	}

	var schema sharedPorts.FetcherSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		wrappedErr := fmt.Errorf("unmarshal cached schema: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to unmarshal cached schema", wrappedErr)

		return nil, wrappedErr
	}

	return &schema, nil
}

// SetSchema stores a connection schema in the cache with a TTL.
func (cache *SchemaCache) SetSchema(ctx context.Context, connID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	if cache == nil || cache.client == nil {
		return ErrCacheNotInitialized
	}

	if schema == nil {
		return ErrSchemaRequired
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.set_schema")
	defer span.End()

	data, err := json.Marshal(schema)
	if err != nil {
		wrappedErr := fmt.Errorf("marshal schema for cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to marshal schema", wrappedErr)

		return wrappedErr
	}

	key, err := cache.schemaKey(ctx, connID)
	if err != nil {
		wrappedErr := fmt.Errorf("construct schema key: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to construct schema key", wrappedErr)

		return wrappedErr
	}

	if err := cache.client.Set(ctx, key, data, ttl).Err(); err != nil {
		wrappedErr := fmt.Errorf("set schema in cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to set schema in cache", wrappedErr)
		logger.With(libLog.Err(wrappedErr)).Log(ctx, libLog.LevelError, "failed to set schema in cache")

		return wrappedErr
	}

	return nil
}

// InvalidateSchema removes a cached schema for a specific connection.
func (cache *SchemaCache) InvalidateSchema(ctx context.Context, connectionID string) error {
	if cache == nil || cache.client == nil {
		return ErrCacheNotInitialized
	}

	tracer := tracerFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.invalidate_schema")
	defer span.End()

	key, err := cache.schemaKey(ctx, connectionID)
	if err != nil {
		wrappedErr := fmt.Errorf("construct schema key: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to construct schema key", wrappedErr)

		return wrappedErr
	}

	if err := cache.client.Del(ctx, key).Err(); err != nil {
		wrappedErr := fmt.Errorf("invalidate schema in cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to invalidate schema in cache", wrappedErr)

		return wrappedErr
	}

	return nil
}
