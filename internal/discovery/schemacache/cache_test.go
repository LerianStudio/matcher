// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package schemacache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

func TestCache_NilProvider_GetSchemaReturnsError(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, time.Minute, nil)

	_, err := cache.GetSchema(context.Background(), "conn-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProviderNil)
}

func TestCache_NilProvider_SetSchemaReturnsError(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, time.Minute, nil)

	err := cache.SetSchema(context.Background(), "conn-1", &sharedPorts.FetcherSchema{ID: "conn-1"}, time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProviderNil)
}

func TestCache_NilProvider_InvalidateSchemaReturnsError(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, time.Minute, nil)

	err := cache.InvalidateSchema(context.Background(), "conn-1")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrProviderNil)
}

func TestCache_Reload_InstallsNewTTL(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, time.Minute, nil)

	// Exercise currentTTL directly to verify the swap.
	got := cache.currentTTL(0)
	assert.Equal(t, time.Minute, got)

	cache.Reload(5 * time.Minute)

	got = cache.currentTTL(0)
	assert.Equal(t, 5*time.Minute, got)
}

func TestCache_CurrentTTL_UsesResolverWhenPositive(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		ttl = 5 * time.Minute
	)

	resolver := func() time.Duration {
		mu.Lock()
		defer mu.Unlock()

		return ttl
	}

	cache := NewCache(nil, false, time.Minute, resolver)

	got := cache.currentTTL(30 * time.Second)
	assert.Equal(t, 5*time.Minute, got, "resolver result wins when > 0")

	mu.Lock()
	ttl = 10 * time.Minute
	mu.Unlock()

	got = cache.currentTTL(30 * time.Second)
	assert.Equal(t, 10*time.Minute, got, "resolver update propagates immediately")
}

func TestCache_CurrentTTL_FallsBackWhenResolverReturnsZero(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, 0, func() time.Duration { return 0 })

	got := cache.currentTTL(7 * time.Minute)
	assert.Equal(t, 7*time.Minute, got, "resolver zero + initial zero must fall back to caller TTL")
}

func TestCache_CurrentTTL_UsesInitialWhenNoResolverAndNoCallerTTL(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, 3*time.Minute, nil)

	got := cache.currentTTL(0)
	assert.Equal(t, 3*time.Minute, got)
}

// TestCache_ConcurrentReloadAndCurrentTTL is the atomic-swap race test.
// Run with `go test -race` — concurrent Reload + currentTTL must never
// produce a torn read. The assertion verifies every observed TTL is
// one of the values Reload was called with.
func TestCache_ConcurrentReloadAndCurrentTTL(t *testing.T) {
	t.Parallel()

	cache := NewCache(nil, false, 1*time.Minute, nil)

	validTTLs := map[time.Duration]struct{}{
		1 * time.Minute: {},
		2 * time.Minute: {},
		3 * time.Minute: {},
		4 * time.Minute: {},
	}

	const (
		readers         = 16
		readsPerReader  = 500
	)

	var (
		wg         sync.WaitGroup
		unknownTTL atomic.Int64
	)

	wg.Add(readers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < readsPerReader; j++ {
				got := cache.currentTTL(0)
				if _, ok := validTTLs[got]; !ok {
					unknownTTL.Add(1)
				}
			}
		}()
	}

	// Reloader: cycle through the valid TTLs.
	wg.Add(1)
	go func() {
		defer wg.Done()

		candidates := []time.Duration{2 * time.Minute, 3 * time.Minute, 4 * time.Minute}
		for i := 0; i < 200; i++ {
			cache.Reload(candidates[i%len(candidates)])
		}
	}()

	wg.Wait()

	assert.Zero(t, unknownTTL.Load(), "every currentTTL must observe a known value")
}

// TestCache_ConcurrentResolverTTLMutation races concurrent currentTTL
// calls against a mutating resolver. The CAS path inside currentTTL
// must not drop updates.
func TestCache_ConcurrentResolverTTLMutation(t *testing.T) {
	t.Parallel()

	var ttl atomic.Int64

	ttl.Store(int64(time.Minute))

	resolver := func() time.Duration {
		return time.Duration(ttl.Load())
	}

	cache := NewCache(nil, false, time.Minute, resolver)

	validTTLs := map[time.Duration]struct{}{
		1 * time.Minute: {},
		2 * time.Minute: {},
		3 * time.Minute: {},
	}

	const readers = 16

	var (
		wg         sync.WaitGroup
		unknownTTL atomic.Int64
	)

	wg.Add(readers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < 500; j++ {
				got := cache.currentTTL(0)
				if _, ok := validTTLs[got]; !ok {
					unknownTTL.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		candidates := []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute}
		for i := 0; i < 200; i++ {
			ttl.Store(int64(candidates[i%len(candidates)]))
		}
	}()

	wg.Wait()

	assert.Zero(t, unknownTTL.Load(), "every currentTTL must observe a known value")
}
