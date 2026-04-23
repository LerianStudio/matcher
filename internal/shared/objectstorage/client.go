// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

// Package objectstorage provides a hot-reloadable object storage client
// used across governance (archival), reporting (exports), and custody
// (artifact write-once). The concrete Client wraps an active backend
// (typically *storage.S3Client) behind an atomic.Pointer so bootstrap
// can swap the backend on config change without coordinated restarts.
//
// Design note: prior to T-015 this was modelled as an interface (shared
// kernel port) plus a dynamic wrapper that re-resolved the backend on
// every call under a mutex. The interface had exactly one production
// implementation — hot-reload was being expressed via the
// interface-substitution metaphor instead of what it actually is: a
// pointer swap. This package collapses the port + wrapper into a
// concrete type whose backend is atomically swappable.
//
// The Reload method lets bootstrap install a new backend on config
// change. Callers with a Resolver (configGetter-style) can wire it at
// construction time and get per-call cache-key rebuilds "for free".
package objectstorage

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// Backend is the narrow dependency the Client wraps. storage.S3Client
// already satisfies this interface; tests can substitute lightweight
// stubs without touching the shared-kernel sentinel errors.
//
// The method surface is IDENTICAL to the previous sharedPorts.ObjectStorageClient
// port so downstream callers remain unchanged after substituting *Client
// for the interface value they used to consume.
type Backend interface {
	Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)
	UploadIfAbsent(ctx context.Context, key string, reader io.Reader, contentType string) (string, error)
	UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...sharedPorts.UploadOption) (string, error)
	Download(ctx context.Context, key string) (io.ReadCloser, error)
	Delete(ctx context.Context, key string) error
	GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)
	Exists(ctx context.Context, key string) (bool, error)
}

// Resolver rebuilds the backend from current config. It returns the
// backend, a stable cache key derived from the config that produced it,
// and any error. The Client compares cache keys to decide whether to
// reuse the current backend or swap to the new one. Returning a nil
// backend is OK — the Client keeps the last known good active (or
// falls through to fallback when no runtime backend has ever been
// observed).
type Resolver func(ctx context.Context) (backend Backend, cacheKey string, err error)

// state is what Client.state swaps atomically. active is the backend
// installed by the most recent successful Resolve; activeKey is the
// cache key that produced it; fallback is a long-lived backup used
// until the first runtime backend is observed; runtimeObserved records
// whether a real backend has ever been installed — once true, a later
// nil active fails fast instead of silently reverting to fallback.
type state struct {
	active          Backend
	activeKey       string
	fallback        Backend
	runtimeObserved bool
}

// Client is the concrete, hot-reloadable object storage client. It
// holds the active + fallback backends behind an atomic.Pointer so
// Reload, Resolver-driven rebuilds, and method calls never race.
//
// The pointer discipline is straightforward:
//   - Load the state at the top of each method (single atomic read).
//   - Call the resolver (if wired), CAS in a fresh state on cache-key change.
//   - Select the backend (runtimeObserved gate + active/fallback choice).
//   - Delegate.
//
// In-flight operations keep using the backend they loaded; subsequent
// calls pick up the new backend after the CAS succeeds.
type Client struct {
	state    atomic.Pointer[state]
	resolver Resolver
}

// NewClient constructs a Client with fallback as the initial backend.
// fallback may be nil if the service boots without storage configured;
// in that case every method returns sharedPorts.ErrObjectStorageUnavailable
// until Reload or the resolver installs a real backend.
func NewClient(fallback Backend) *Client {
	client := &Client{}
	client.state.Store(&state{fallback: fallback})

	return client
}

// NewClientWithResolver constructs a Client that lazily rebuilds its
// backend via the resolver when the resolver reports a new cache key.
// Pass nil for resolver to disable lazy rebuild (callers then drive
// reloads explicitly via Reload).
func NewClientWithResolver(fallback Backend, resolver Resolver) *Client {
	client := NewClient(fallback)
	client.resolver = resolver

	return client
}

// Reload atomically installs newBackend as the active backend with an
// empty cache key. Passing nil clears the active backend (next call
// returns ErrObjectStorageUnavailable if runtimeObserved is already
// set). Reload is safe to call concurrently with method calls; the
// pointer swap is the synchronisation point.
func (client *Client) Reload(newBackend Backend) {
	client.reloadInternal(newBackend, "")
}

// reloadInternal is the shared CAS loop used by Reload and the
// resolver-driven path. The loop retries on CAS failure so concurrent
// reloads don't lose a backend.
func (client *Client) reloadInternal(newBackend Backend, cacheKey string) {
	if client == nil {
		return
	}

	for {
		current := client.state.Load()

		var fallback Backend
		if current != nil {
			fallback = current.fallback
		}

		next := &state{
			active:          newBackend,
			activeKey:       cacheKey,
			fallback:        fallback,
			runtimeObserved: current != nil && current.runtimeObserved,
		}

		if !isNilBackend(newBackend) {
			next.runtimeObserved = true
		}

		if client.state.CompareAndSwap(current, next) {
			return
		}
	}
}

// SetFallback installs a new fallback backend. Bootstrap occasionally
// swaps the bootstrap default (e.g. when configuration changes the
// fallback's bucket name). Rarely needed outside tests.
func (client *Client) SetFallback(fallback Backend) {
	if client == nil {
		return
	}

	for {
		current := client.state.Load()

		var next *state
		if current == nil {
			next = &state{fallback: fallback}
		} else {
			next = &state{
				active:          current.active,
				activeKey:       current.activeKey,
				fallback:        fallback,
				runtimeObserved: current.runtimeObserved,
			}
		}

		if client.state.CompareAndSwap(current, next) {
			return
		}
	}
}

// current resolves the backend to dispatch to. It first honours the
// resolver (if wired) to pick up config changes, then selects active
// vs fallback per the runtimeObserved gate.
func (client *Client) current(ctx context.Context) (Backend, error) {
	if client == nil {
		return nil, sharedPorts.ErrObjectStorageUnavailable
	}

	if client.resolver != nil {
		client.resolveAndMaybeSwap(ctx)
	}

	snapshot := client.state.Load()
	if snapshot == nil {
		return nil, sharedPorts.ErrObjectStorageUnavailable
	}

	if !isNilBackend(snapshot.active) {
		return snapshot.active, nil
	}

	if snapshot.runtimeObserved {
		// We used to have a real backend and don't any more. Fail fast
		// instead of silently serving from the bootstrap default.
		return nil, sharedPorts.ErrObjectStorageUnavailable
	}

	if !isNilBackend(snapshot.fallback) {
		return snapshot.fallback, nil
	}

	return nil, sharedPorts.ErrObjectStorageUnavailable
}

// resolveAndMaybeSwap consults the resolver; if it produces a backend
// with a different cache key than the currently installed one, CAS in
// the new state. Resolver errors are silently tolerated — the caller
// still gets the last known good backend via the subsequent state
// load. Matches the previous dynamic wrapper's behaviour of treating
// resolver failures as "keep the old backend".
func (client *Client) resolveAndMaybeSwap(ctx context.Context) {
	backend, cacheKey, err := client.resolver(ctx)
	if err != nil || isNilBackend(backend) {
		return
	}

	current := client.state.Load()
	if current != nil && current.activeKey == cacheKey && !isNilBackend(current.active) {
		return
	}

	client.reloadInternal(backend, cacheKey)
}

// isNilBackend reports whether a Backend value is nil or an interface
// holding a typed-nil pointer. sharedPorts.IsNilValue centralises the
// reflect dance so the hot-reload wrappers in this codebase share the
// same semantics.
func isNilBackend(b Backend) bool {
	return sharedPorts.IsNilValue(b)
}

// Upload delegates to the current backend.
func (client *Client) Upload(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for upload: %w", err)
	}

	result, err := backend.Upload(ctx, key, reader, contentType)
	if err != nil {
		return "", fmt.Errorf("upload object: %w", err)
	}

	return result, nil
}

// UploadIfAbsent delegates to the current backend. Crucially, the
// sharedPorts.ErrObjectAlreadyExists sentinel is passed through without
// wrapping so callers using errors.Is continue to recognise the replay
// signal — this is the custody write-once contract.
func (client *Client) UploadIfAbsent(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for conditional upload: %w", err)
	}

	// Do NOT wrap the delegate error — the adapter contract says
	// ErrObjectAlreadyExists must survive errors.Is at the caller.
	return backend.UploadIfAbsent(ctx, key, reader, contentType) //nolint:wrapcheck // intentional passthrough to preserve ErrObjectAlreadyExists sentinel
}

// UploadWithOptions delegates to the current backend, passing options
// through verbatim.
func (client *Client) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, opts ...sharedPorts.UploadOption) (string, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for upload with options: %w", err)
	}

	result, err := backend.UploadWithOptions(ctx, key, reader, contentType, opts...)
	if err != nil {
		return "", fmt.Errorf("upload object with options: %w", err)
	}

	return result, nil
}

// Download delegates to the current backend.
func (client *Client) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve object storage client for download: %w", err)
	}

	reader, err := backend.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download object: %w", err)
	}

	return reader, nil
}

// Delete delegates to the current backend.
func (client *Client) Delete(ctx context.Context, key string) error {
	backend, err := client.current(ctx)
	if err != nil {
		return fmt.Errorf("resolve object storage client for delete: %w", err)
	}

	if err := backend.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	return nil
}

// GeneratePresignedURL delegates to the current backend.
func (client *Client) GeneratePresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return "", fmt.Errorf("resolve object storage client for presigned url: %w", err)
	}

	url, err := backend.GeneratePresignedURL(ctx, key, expiry)
	if err != nil {
		return "", fmt.Errorf("generate presigned url: %w", err)
	}

	return url, nil
}

// Exists delegates to the current backend.
func (client *Client) Exists(ctx context.Context, key string) (bool, error) {
	backend, err := client.current(ctx)
	if err != nil {
		return false, fmt.Errorf("resolve object storage client for existence check: %w", err)
	}

	exists, err := backend.Exists(ctx, key)
	if err != nil {
		return false, fmt.Errorf("check object existence: %w", err)
	}

	return exists, nil
}
