// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package schemacache provides a hot-reloadable schema cache wrapper
// around the discovery Redis adapter. The concrete Cache holds the
// current TTL behind an atomic.Pointer so bootstrap can swap it on
// config change without coordinated restarts, while the per-call
// Redis client resolution continues to flow through the infrastructure
// provider (which issues tenant-scoped leases).
//
// Design note: prior to T-015 this was modelled as two port
// implementations (providerBackedSchemaCache + dynamicSchemaCache)
// chained through the discoveryPorts.SchemaCache interface. The
// interface had no real substitutability — both implementations were
// lifecycle wrappers for the same underlying redis adapter. T-015
// collapses them into one concrete type. The TTL is the only thing
// that actually changes at runtime; atomic.Pointer on the TTL
// expresses that honestly.
package schemacache

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	discoveryRedis "github.com/LerianStudio/matcher/internal/discovery/adapters/redis"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// ErrProviderNil indicates the cache was constructed without an
// infrastructure provider. Per-call methods surface this via a wrapped
// error so callers can distinguish "no cache configured" from "redis
// returned an error".
var ErrProviderNil = errors.New("schema cache: infrastructure provider not available")

// ErrRedisClientNil indicates the infrastructure provider returned a
// nil Redis client (e.g. during bootstrap before Redis is configured
// or when the lease's underlying connection has been torn down).
var ErrRedisClientNil = errors.New("schema cache: redis client is nil")

// TTLResolver returns the current cache TTL. The Cache calls this
// before every SetSchema so runtime config changes apply on the next
// write without needing an explicit Reload. Pass nil to use the
// caller-supplied TTL verbatim.
type TTLResolver func() time.Duration

// state is what Cache.state swaps atomically. Only the TTL is
// genuinely mutable at runtime — the provider and allowTenantPrefix
// are set once at construction time and never change. Holding them in
// the same state keeps every field readable with a single atomic load.
type state struct {
	ttl time.Duration
}

// Cache is the concrete, hot-reloadable schema cache. It resolves the
// current Redis client per call through the InfrastructureProvider
// (which handles tenant-scoped leases) and applies the current TTL to
// writes.
type Cache struct {
	provider          sharedPorts.InfrastructureProvider
	allowTenantPrefix bool
	state             atomic.Pointer[state]
	ttlResolver       TTLResolver
}

// NewCache constructs a schema cache backed by the provider. The
// allowTenantPrefix flag is forwarded to the underlying Redis adapter
// (it controls whether tenant-less contexts are permitted). The
// ttlResolver lets bootstrap swap the TTL at runtime; pass nil to use
// per-call caller TTLs instead.
//
// Returns nil when provider is nil — callers should check and fall
// back to a no-op path.
func NewCache(
	provider sharedPorts.InfrastructureProvider,
	allowTenantPrefix bool,
	initialTTL time.Duration,
	ttlResolver TTLResolver,
) *Cache {
	cache := &Cache{
		provider:          provider,
		allowTenantPrefix: allowTenantPrefix,
		ttlResolver:       ttlResolver,
	}
	cache.state.Store(&state{ttl: initialTTL})

	return cache
}

// Reload atomically installs a new TTL. Safe to call from any
// goroutine. Use this path when bootstrap drives reloads via explicit
// OnChange callbacks instead of the ttlResolver pull model.
func (cache *Cache) Reload(ttl time.Duration) {
	if cache == nil {
		return
	}

	for {
		current := cache.state.Load()

		next := &state{ttl: ttl}

		if cache.state.CompareAndSwap(current, next) {
			return
		}
	}
}

// currentTTL returns the live TTL. It resolves via ttlResolver (if
// wired), CAS-swapping the state on change so later reads see the new
// value directly.
func (cache *Cache) currentTTL(fallback time.Duration) time.Duration {
	if cache == nil {
		return fallback
	}

	if resolved, ok := cache.resolveTTLFromGetter(); ok {
		return resolved
	}

	snapshot := cache.state.Load()
	if snapshot == nil || snapshot.ttl <= 0 {
		return fallback
	}

	return snapshot.ttl
}

// resolveTTLFromGetter consults the ttlResolver. When it returns a
// positive TTL, that value wins: the state is CAS-updated if needed so
// subsequent reads see the new TTL without another resolver call. The
// bool result distinguishes "resolver produced a value" from "fall
// through to stored/fallback TTL".
func (cache *Cache) resolveTTLFromGetter() (time.Duration, bool) {
	if cache.ttlResolver == nil {
		return 0, false
	}

	resolved := cache.ttlResolver()
	if resolved <= 0 {
		return 0, false
	}

	cache.swapTTLIfChanged(resolved)

	return resolved, true
}

// swapTTLIfChanged CAS-updates the stored TTL when it differs from
// resolved. Retries on contention; abandons if another goroutine
// already installed the same TTL.
func (cache *Cache) swapTTLIfChanged(resolved time.Duration) {
	for {
		current := cache.state.Load()

		if current != nil && current.ttl == resolved {
			return
		}

		next := &state{ttl: resolved}
		if cache.state.CompareAndSwap(current, next) {
			return
		}
	}
}

// resolveBackend builds a fresh *discoveryRedis.SchemaCache backed by
// the provider's current Redis lease. The lease must be released by
// the caller via the returned release function.
//
// This is the per-call path preserved from providerBackedSchemaCache:
// we can't cache the redis client because the lease is tenant-scoped
// and may change between invocations.
func (cache *Cache) resolveBackend(ctx context.Context) (*discoveryRedis.SchemaCache, func(), error) {
	if cache == nil || cache.provider == nil {
		return nil, func() {}, ErrProviderNil
	}

	lease, err := cache.provider.GetRedisConnection(ctx)
	if err != nil {
		return nil, func() {}, fmt.Errorf("get redis connection for schema cache: %w", err)
	}

	redisConn := lease.Connection()
	if redisConn == nil {
		lease.Release()
		return nil, func() {}, ErrRedisClientNil
	}

	redisClient, err := redisConn.GetClient(ctx)
	if err != nil {
		lease.Release()
		return nil, func() {}, fmt.Errorf("get redis client for schema cache: %w", err)
	}

	backend, err := discoveryRedis.NewSchemaCache(redisClient, cache.allowTenantPrefix)
	if err != nil {
		lease.Release()
		return nil, func() {}, fmt.Errorf("create schema cache: %w", err)
	}

	return backend, lease.Release, nil
}

// GetSchema retrieves a cached schema via the per-call Redis lease.
func (cache *Cache) GetSchema(ctx context.Context, connectionID string) (*sharedPorts.FetcherSchema, error) {
	backend, release, err := cache.resolveBackend(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve schema cache backend: %w", err)
	}
	defer release()

	schema, err := backend.GetSchema(ctx, connectionID)
	if err != nil {
		return nil, fmt.Errorf("get schema from cache: %w", err)
	}

	return schema, nil
}

// SetSchema stores a schema using the currently configured TTL (from
// the resolver if wired, falling back to ttl).
func (cache *Cache) SetSchema(ctx context.Context, connectionID string, schema *sharedPorts.FetcherSchema, ttl time.Duration) error {
	backend, release, err := cache.resolveBackend(ctx)
	if err != nil {
		return fmt.Errorf("resolve schema cache backend: %w", err)
	}
	defer release()

	effective := cache.currentTTL(ttl)

	if err := backend.SetSchema(ctx, connectionID, schema, effective); err != nil {
		return fmt.Errorf("set schema in cache: %w", err)
	}

	return nil
}

// InvalidateSchema removes a cached schema.
func (cache *Cache) InvalidateSchema(ctx context.Context, connectionID string) error {
	backend, release, err := cache.resolveBackend(ctx)
	if err != nil {
		return fmt.Errorf("resolve schema cache backend: %w", err)
	}
	defer release()

	if err := backend.InvalidateSchema(ctx, connectionID); err != nil {
		return fmt.Errorf("invalidate schema in cache: %w", err)
	}

	return nil
}
