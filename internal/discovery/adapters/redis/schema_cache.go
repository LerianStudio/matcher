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

	libCommons "github.com/LerianStudio/lib-commons/v4/commons"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libOpentelemetry "github.com/LerianStudio/lib-commons/v4/commons/opentelemetry"

	"github.com/LerianStudio/matcher/internal/auth"
	"github.com/LerianStudio/matcher/internal/discovery/ports"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Compile-time interface compliance check.
var _ ports.SchemaCache = (*SchemaCache)(nil)

const (
	connectionsKeyPrefix = "matcher:discovery:connections:"
	schemaKeyPrefix      = "matcher:discovery:schema:"
	singleTenantScope    = "single-tenant"
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

// schemaKey constructs a validated Redis key for a connection's schema.
func tenantScopeFromContext(ctx context.Context) (string, error) {
	tenantID := strings.TrimSpace(auth.GetTenantID(ctx))
	if tenantID == "" {
		return singleTenantScope, nil
	}

	if !safeKeyPattern.MatchString(tenantID) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeTenantID, tenantID)
	}

	return tenantID, nil
}

func connectionsKeyForContext(ctx context.Context) (string, error) {
	tenantScope, err := tenantScopeFromContext(ctx)
	if err != nil {
		return "", err
	}

	return connectionsKeyPrefix + tenantScope, nil
}

func schemaKey(ctx context.Context, connectionID string) (string, error) {
	if connectionID == "" {
		return "", ErrEmptyConnectionID
	}

	if !safeKeyPattern.MatchString(connectionID) {
		return "", fmt.Errorf("%w: %q", ErrUnsafeConnectionID, connectionID)
	}

	tenantScope, err := tenantScopeFromContext(ctx)
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
)

const defaultScanCount = 100

// tracerFromContext extracts the OpenTelemetry tracer from context.
func tracerFromContext(ctx context.Context) trace.Tracer {
	_, tracer, _, _ := libCommons.NewTrackingFromContext(ctx) //nolint:dogsled // utility wrapper

	return tracer
}

// SchemaCache provides Redis-backed caching for Fetcher discovery data.
type SchemaCache struct {
	client goredis.UniversalClient
}

// NewSchemaCache creates a new Redis schema cache.
func NewSchemaCache(client goredis.UniversalClient) (*SchemaCache, error) {
	if client == nil {
		return nil, ErrRedisClientRequired
	}

	return &SchemaCache{client: client}, nil
}

// GetConnections retrieves cached Fetcher connections.
func (cache *SchemaCache) GetConnections(ctx context.Context) ([]*sharedPorts.FetcherConnection, error) {
	if cache == nil || cache.client == nil {
		return nil, ErrCacheNotInitialized
	}

	tracer := tracerFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.get_connections")
	defer span.End()

	key, keyErr := connectionsKeyForContext(ctx)
	if keyErr != nil {
		wrappedErr := fmt.Errorf("construct connections key: %w", keyErr)
		libOpentelemetry.HandleSpanError(span, "failed to construct connections key", wrappedErr)

		return nil, wrappedErr
	}

	data, err := cache.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrCacheMiss
		}

		wrappedErr := fmt.Errorf("get connections from cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to get connections from cache", wrappedErr)

		return nil, wrappedErr
	}

	var conns []*sharedPorts.FetcherConnection
	if err := json.Unmarshal(data, &conns); err != nil {
		wrappedErr := fmt.Errorf("unmarshal cached connections: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to unmarshal cached connections", wrappedErr)

		return nil, wrappedErr
	}

	return conns, nil
}

// SetConnections stores Fetcher connections in the cache with a TTL.
func (cache *SchemaCache) SetConnections(ctx context.Context, conns []*sharedPorts.FetcherConnection, ttl time.Duration) error {
	if cache == nil || cache.client == nil {
		return ErrCacheNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.set_connections")
	defer span.End()

	key, keyErr := connectionsKeyForContext(ctx)
	if keyErr != nil {
		wrappedErr := fmt.Errorf("construct connections key: %w", keyErr)
		libOpentelemetry.HandleSpanError(span, "failed to construct connections key", wrappedErr)

		return wrappedErr
	}

	data, err := json.Marshal(conns)
	if err != nil {
		wrappedErr := fmt.Errorf("marshal connections for cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to marshal connections", wrappedErr)

		return wrappedErr
	}

	if err := cache.client.Set(ctx, key, data, ttl).Err(); err != nil {
		wrappedErr := fmt.Errorf("set connections in cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to set connections in cache", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to set connections in cache")

		return wrappedErr
	}

	return nil
}

// GetSchema retrieves a cached schema for a specific connection.
func (cache *SchemaCache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	if cache == nil || cache.client == nil {
		return nil, ErrCacheNotInitialized
	}

	tracer := tracerFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.get_schema")
	defer span.End()

	key, err := schemaKey(ctx, connectionID)
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

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.set_schema")
	defer span.End()

	data, err := json.Marshal(schema)
	if err != nil {
		wrappedErr := fmt.Errorf("marshal schema for cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to marshal schema", wrappedErr)

		return wrappedErr
	}

	key, err := schemaKey(ctx, connID)
	if err != nil {
		wrappedErr := fmt.Errorf("construct schema key: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to construct schema key", wrappedErr)

		return wrappedErr
	}

	if err := cache.client.Set(ctx, key, data, ttl).Err(); err != nil {
		wrappedErr := fmt.Errorf("set schema in cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to set schema in cache", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to set schema in cache")

		return wrappedErr
	}

	return nil
}

// InvalidateAll removes all cached discovery data (connections and schemas).
func (cache *SchemaCache) InvalidateAll(ctx context.Context) error {
	if cache == nil || cache.client == nil {
		return ErrCacheNotInitialized
	}

	logger, tracer, _, _ := libCommons.NewTrackingFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.invalidate_all")
	defer span.End()

	connectionsKey, keyErr := connectionsKeyForContext(ctx)
	if keyErr != nil {
		wrappedErr := fmt.Errorf("construct connections key: %w", keyErr)
		libOpentelemetry.HandleSpanError(span, "failed to construct connections key", wrappedErr)

		return wrappedErr
	}

	tenantScope, tenantErr := tenantScopeFromContext(ctx)
	if tenantErr != nil {
		wrappedErr := fmt.Errorf("construct tenant scope: %w", tenantErr)
		libOpentelemetry.HandleSpanError(span, "failed to construct tenant scope", wrappedErr)

		return wrappedErr
	}

	pipe := cache.client.Pipeline()
	pipe.Del(ctx, connectionsKey)

	// Scan and delete schema keys for this tenant only.
	iter := cache.client.Scan(ctx, 0, schemaKeyPrefix+tenantScope+":*", defaultScanCount).Iterator()
	for iter.Next(ctx) {
		pipe.Del(ctx, iter.Val())
	}

	if err := iter.Err(); err != nil {
		wrappedErr := fmt.Errorf("scan schema keys: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to scan schema keys", wrappedErr)

		return wrappedErr
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		wrappedErr := fmt.Errorf("invalidate cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to invalidate cache", wrappedErr)
		logger.With(libLog.Any("error", wrappedErr.Error())).Log(ctx, libLog.LevelError, "failed to invalidate cache")

		return wrappedErr
	}

	return nil
}

// InvalidateSchema removes a specific connection's cached schema.
func (cache *SchemaCache) InvalidateSchema(ctx context.Context, connectionID string) error {
	if cache == nil || cache.client == nil {
		return ErrCacheNotInitialized
	}

	tracer := tracerFromContext(ctx)

	ctx, span := tracer.Start(ctx, "redis.discovery.invalidate_schema")
	defer span.End()

	key, err := schemaKey(ctx, connectionID)
	if err != nil {
		wrappedErr := fmt.Errorf("construct schema key: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to construct schema key", wrappedErr)

		return wrappedErr
	}

	if err := cache.client.Del(ctx, key).Err(); err != nil {
		wrappedErr := fmt.Errorf("invalidate schema cache: %w", err)
		libOpentelemetry.HandleSpanError(span, "failed to invalidate schema cache", wrappedErr)

		return wrappedErr
	}

	return nil
}
