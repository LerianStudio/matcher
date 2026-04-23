// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package objectstorage

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// stubBackend is the minimal Backend test double. Every method prefixes
// the key with a tag so tests can identify which backend served a call.
type stubBackend struct {
	prefix string
	calls  atomic.Int64
}

func (b *stubBackend) Upload(_ context.Context, key string, _ io.Reader, _ string) (string, error) {
	b.calls.Add(1)
	return b.prefix + key, nil
}

func (b *stubBackend) UploadIfAbsent(ctx context.Context, key string, reader io.Reader, contentType string) (string, error) {
	return b.Upload(ctx, key, reader, contentType)
}

func (b *stubBackend) UploadWithOptions(ctx context.Context, key string, reader io.Reader, contentType string, _ ...sharedPorts.UploadOption) (string, error) {
	return b.Upload(ctx, key, reader, contentType)
}

func (b *stubBackend) Download(_ context.Context, key string) (io.ReadCloser, error) {
	b.calls.Add(1)
	return io.NopCloser(strings.NewReader(b.prefix + key)), nil
}

func (b *stubBackend) Delete(context.Context, string) error {
	b.calls.Add(1)
	return nil
}

func (b *stubBackend) GeneratePresignedURL(_ context.Context, key string, _ time.Duration) (string, error) {
	b.calls.Add(1)
	return b.prefix + key, nil
}

func (b *stubBackend) Exists(_ context.Context, key string) (bool, error) {
	b.calls.Add(1)
	return strings.HasPrefix(b.prefix+key, b.prefix), nil
}

func TestClient_UsesFallbackUntilReload(t *testing.T) {
	t.Parallel()

	fallback := &stubBackend{prefix: "fallback:"}
	client := NewClient(fallback)

	url, err := client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "fallback:file", url)

	client.Reload(&stubBackend{prefix: "runtime:"})

	url, err = client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "runtime:file", url)
}

func TestClient_FailsAfterRuntimeBackendCleared(t *testing.T) {
	t.Parallel()

	fallback := &stubBackend{prefix: "fallback:"}
	client := NewClient(fallback)
	client.Reload(&stubBackend{prefix: "runtime:"})

	_, err := client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.NoError(t, err)

	// Once a runtime backend was installed, a later nil-reload should
	// fail fast instead of silently reverting to fallback. This is the
	// "I had real storage; don't quietly downgrade me" contract.
	client.Reload(nil)
	_, err = client.GeneratePresignedURL(context.Background(), "file", time.Minute)
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}

func TestClient_NilReceiver_FailsClosed(t *testing.T) {
	t.Parallel()

	var client *Client

	_, err := client.Upload(context.Background(), "file", strings.NewReader(""), "text/plain")
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}

func TestClient_NilFallback_FailsClosed(t *testing.T) {
	t.Parallel()

	client := NewClient(nil)

	_, err := client.Upload(context.Background(), "file", strings.NewReader(""), "text/plain")
	require.Error(t, err)
	assert.ErrorIs(t, err, sharedPorts.ErrObjectStorageUnavailable)
}

func TestClient_UploadIfAbsent_PreservesSentinel(t *testing.T) {
	t.Parallel()

	client := NewClient(&sentinelBackend{})

	_, err := client.UploadIfAbsent(context.Background(), "file", strings.NewReader("x"), "text/plain")
	require.Error(t, err)
	// Critical: the ErrObjectAlreadyExists sentinel must survive the
	// wrapper so custody's errors.Is path keeps working.
	assert.ErrorIs(t, err, sharedPorts.ErrObjectAlreadyExists)
}

func TestClient_Resolver_SwapsOnCacheKeyChange(t *testing.T) {
	t.Parallel()

	v1 := &stubBackend{prefix: "v1:"}
	v2 := &stubBackend{prefix: "v2:"}

	var (
		mu      sync.Mutex
		current = v1
		key     = "k1"
	)

	resolver := func(_ context.Context) (Backend, string, error) {
		mu.Lock()
		defer mu.Unlock()

		return current, key, nil
	}

	client := NewClientWithResolver(nil, resolver)

	got, err := client.Upload(context.Background(), "x", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "v1:x", got)

	mu.Lock()
	current = v2
	key = "k2"
	mu.Unlock()

	got, err = client.Upload(context.Background(), "x", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "v2:x", got)
}

func TestClient_Resolver_KeepsBackendOnUnchangedKey(t *testing.T) {
	t.Parallel()

	b := &stubBackend{prefix: "stable:"}

	var resolves atomic.Int64

	resolver := func(_ context.Context) (Backend, string, error) {
		resolves.Add(1)
		return b, "stable-key", nil
	}

	client := NewClientWithResolver(nil, resolver)

	for i := 0; i < 5; i++ {
		_, err := client.Upload(context.Background(), "k", strings.NewReader(""), "text/plain")
		require.NoError(t, err)
	}

	// Resolver is called every time to pick up potential drift, but the
	// backend is not swapped when the cache key is unchanged.
	assert.EqualValues(t, 5, resolves.Load())
	assert.EqualValues(t, 5, b.calls.Load())
}

func TestClient_Resolver_KeepsLastGoodOnResolverError(t *testing.T) {
	t.Parallel()

	b := &stubBackend{prefix: "good:"}

	var errMode atomic.Bool

	resolver := func(_ context.Context) (Backend, string, error) {
		if errMode.Load() {
			return nil, "", errors.New("resolver boom")
		}

		return b, "k", nil
	}

	client := NewClientWithResolver(nil, resolver)

	got, err := client.Upload(context.Background(), "x", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "good:x", got)

	errMode.Store(true)

	got, err = client.Upload(context.Background(), "x", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "good:x", got, "resolver error must leave the last good backend in place")
}

// TestClient_ConcurrentReloadAndCalls is the atomic-swap race test.
// Run with `go test -race` — many goroutines hammer Upload while a
// reloader goroutine cycles through a bank of backends. The race
// detector catches any unsynchronised access to the state pointer.
// The assertions verify that every Upload call observed a prefix
// from one of the installed backends (no torn reads) and that at
// least two backends served traffic (proves the swap is observable
// by concurrent callers — otherwise the test lost its signal).
func TestClient_ConcurrentReloadAndCalls(t *testing.T) {
	t.Parallel()

	backends := []*stubBackend{
		{prefix: "a:"},
		{prefix: "b:"},
		{prefix: "c:"},
		{prefix: "d:"},
	}

	validPrefixes := make(map[string]struct{}, len(backends))
	for _, b := range backends {
		validPrefixes[b.prefix] = struct{}{}
	}

	client := NewClient(backends[0])

	const (
		callers        = 16
		callsPerCaller = 2000
	)

	var (
		wg         sync.WaitGroup
		unknownTag atomic.Int64
		stopReload atomic.Bool
	)

	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < callsPerCaller; j++ {
				got, err := client.Upload(context.Background(), "key", strings.NewReader(""), "text/plain")
				if err != nil {
					t.Errorf("upload: %v", err)
					return
				}

				tag := got[:2]
				if _, ok := validPrefixes[tag]; !ok {
					unknownTag.Add(1)
				}
			}
		}()
	}

	// Reloader: cycle through backends until the callers are done.
	reloaderDone := make(chan struct{})

	go func() {
		defer close(reloaderDone)

		counter := 0
		for !stopReload.Load() {
			client.Reload(backends[counter%len(backends)])
			counter++
		}
	}()

	wg.Wait()
	stopReload.Store(true)
	<-reloaderDone

	assert.Zero(t, unknownTag.Load(), "every Upload call must have dispatched to an installed backend")

	var (
		total        int64
		backendsUsed int
	)

	for _, b := range backends {
		count := b.calls.Load()
		total += count

		if count > 0 {
			backendsUsed++
		}
	}

	assert.EqualValues(t, callers*callsPerCaller, total, "every call must have reached some backend")
	assert.GreaterOrEqual(t, backendsUsed, 2, "at least two backends must have served traffic; otherwise the reload swap is invisible to callers")
}

// TestClient_ConcurrentReloadReload ensures two concurrent Reload calls
// don't lose a backend. The CompareAndSwap loop inside Reload is what
// makes this safe; the race detector plus the prefix assertion catches
// any regression.
func TestClient_ConcurrentReloadReload(t *testing.T) {
	t.Parallel()

	client := NewClient(&stubBackend{prefix: "initial:"})

	const reloads = 64

	var wg sync.WaitGroup

	wg.Add(reloads)

	for i := 0; i < reloads; i++ {
		go func(idx int) {
			defer wg.Done()

			client.Reload(&stubBackend{prefix: "r:"})
		}(i)
	}

	wg.Wait()

	got, err := client.Upload(context.Background(), "k", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "r:"), "last reload must have installed a reloaded backend, got %q", got)
}

// TestClient_ConcurrentResolverAndCalls races a resolver-driven swap
// against concurrent method calls. The resolver's cache key flips
// between two values so the CAS path is hit frequently. Race detector
// plus prefix check catches any torn state.
func TestClient_ConcurrentResolverAndCalls(t *testing.T) {
	t.Parallel()

	v1 := &stubBackend{prefix: "1:"}
	v2 := &stubBackend{prefix: "2:"}

	var flip atomic.Bool

	resolver := func(_ context.Context) (Backend, string, error) {
		if flip.Load() {
			return v2, "k2", nil
		}

		return v1, "k1", nil
	}

	client := NewClientWithResolver(nil, resolver)

	const (
		callers        = 16
		callsPerCaller = 200
	)

	var (
		wg         sync.WaitGroup
		unknownTag atomic.Int64
	)

	wg.Add(callers)

	for i := 0; i < callers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < callsPerCaller; j++ {
				got, err := client.Upload(context.Background(), "x", strings.NewReader(""), "text/plain")
				if err != nil {
					t.Errorf("upload: %v", err)
					return
				}

				if !strings.HasPrefix(got, "1:") && !strings.HasPrefix(got, "2:") {
					unknownTag.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 100; i++ {
			flip.Store(i%2 == 0)
			time.Sleep(time.Microsecond)
		}
	}()

	wg.Wait()

	assert.Zero(t, unknownTag.Load(), "every call must have observed a known backend")
	assert.Positive(t, v1.calls.Load(), "v1 must have served some calls")
	assert.Positive(t, v2.calls.Load(), "v2 must have served some calls")
}

func TestClient_SetFallback(t *testing.T) {
	t.Parallel()

	client := NewClient(&stubBackend{prefix: "old:"})

	// SetFallback before any reload: next call should use the new fallback.
	client.SetFallback(&stubBackend{prefix: "new:"})

	got, err := client.Upload(context.Background(), "k", strings.NewReader(""), "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "new:k", got)
}

// sentinelBackend returns ErrObjectAlreadyExists on UploadIfAbsent and
// panics otherwise. Used to verify the sentinel passthrough contract.
type sentinelBackend struct {
	stubBackend
}

func (b *sentinelBackend) UploadIfAbsent(_ context.Context, _ string, _ io.Reader, _ string) (string, error) {
	return "", sharedPorts.ErrObjectAlreadyExists
}
