// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package extractionpoller

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"

	discoveryRepos "github.com/LerianStudio/matcher/internal/discovery/domain/repositories"
	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

// fakeFetcherClient is a minimal FetcherClient for NewPoller
// construction. Its methods are never exercised in these tests
// because Runner substitutes the real worker.ExtractionPoller.
type fakeFetcherClient struct {
	sharedPorts.FetcherClient
}

// fakeExtractionRepo mirrors fakeFetcherClient: supplied only so
// NewPoller's constructor signature is satisfied.
type fakeExtractionRepo struct {
	discoveryRepos.ExtractionRepository
}

// stubRunner is the test Runner: tracks how many times
// PollUntilComplete was invoked and does no real work.
type stubRunner struct {
	id    string
	calls atomic.Int64
}

func (r *stubRunner) PollUntilComplete(
	_ context.Context,
	_ uuid.UUID,
	_ func(context.Context, string) error,
	_ func(context.Context, string),
) {
	r.calls.Add(1)
}

func TestPoller_NilReceiver_InvokesOnFailed(t *testing.T) {
	t.Parallel()

	var p *Poller

	called := atomic.Bool{}

	p.PollUntilComplete(context.Background(), uuid.New(),
		func(_ context.Context, _ string) error { return nil },
		func(_ context.Context, msg string) {
			called.Store(true)
			assert.Contains(t, msg, "extraction poller")
		})

	assert.True(t, called.Load())
}

func TestPoller_NoRunnerNoResolver_InvokesOnFailed(t *testing.T) {
	t.Parallel()

	p := NewPoller(&fakeFetcherClient{}, &fakeExtractionRepo{}, nil, nil, &libLog.NopLogger{})

	called := atomic.Bool{}

	p.PollUntilComplete(context.Background(), uuid.New(),
		func(_ context.Context, _ string) error { return nil },
		func(_ context.Context, msg string) {
			called.Store(true)
			assert.Contains(t, msg, "extraction poller")
		})

	assert.True(t, called.Load())
}

func TestPoller_Reload_InstallsRunner(t *testing.T) {
	t.Parallel()

	p := NewPoller(&fakeFetcherClient{}, &fakeExtractionRepo{}, nil, nil, &libLog.NopLogger{})

	runner := &stubRunner{id: "r1"}
	p.Reload(runner, RunnerConfig{PollInterval: time.Hour, Timeout: time.Hour})

	got, err := p.current()
	require.NoError(t, err)
	assert.Same(t, Runner(runner), got, "Reload must install the provided runner")
}

func TestPoller_Resolver_RebuildsOnConfigChange(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		cfg = RunnerConfig{PollInterval: time.Hour, Timeout: time.Hour}
	)

	resolver := func() RunnerConfig {
		mu.Lock()
		defer mu.Unlock()

		return cfg
	}

	var factoryCalls atomic.Int64

	factory := func(
		_ sharedPorts.FetcherClient,
		_ discoveryRepos.ExtractionRepository,
		fcfg RunnerConfig,
		_ libLog.Logger,
	) (Runner, error) {
		factoryCalls.Add(1)
		return &stubRunner{id: fcfg.PollInterval.String()}, nil
	}

	p := NewPoller(&fakeFetcherClient{}, &fakeExtractionRepo{}, resolver, factory, &libLog.NopLogger{})

	first, err := p.current()
	require.NoError(t, err)

	// Same config: resolver must not rebuild.
	second, err := p.current()
	require.NoError(t, err)
	assert.Same(t, first, second, "unchanged config must reuse runner")
	assert.EqualValues(t, 1, factoryCalls.Load())

	// Change config: expect a fresh runner pointer.
	mu.Lock()
	cfg = RunnerConfig{PollInterval: 2 * time.Hour, Timeout: 2 * time.Hour}
	mu.Unlock()

	third, err := p.current()
	require.NoError(t, err)
	assert.NotSame(t, first, third, "config change must produce a fresh runner")
	assert.EqualValues(t, 2, factoryCalls.Load())
}

// TestPoller_ConcurrentReloadAndCurrent is the atomic-swap race test.
// Run with `go test -race` — many goroutines call current() while a
// reload goroutine cycles through runners.
func TestPoller_ConcurrentReloadAndCurrent(t *testing.T) {
	t.Parallel()

	p := NewPoller(&fakeFetcherClient{}, &fakeExtractionRepo{}, nil, nil, &libLog.NopLogger{})
	p.Reload(&stubRunner{id: "initial"}, RunnerConfig{PollInterval: time.Hour, Timeout: time.Hour})

	const (
		readers        = 16
		readsPerReader = 500
	)

	var (
		wg         sync.WaitGroup
		nilSeen    atomic.Int64
		stopReload atomic.Bool
	)

	wg.Add(readers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < readsPerReader; j++ {
				got, currErr := p.current()
				if currErr != nil {
					t.Errorf("current: %v", currErr)
					return
				}

				if got == nil {
					nilSeen.Add(1)
				}
			}
		}()
	}

	// Reloader: swap the runner many times while readers are active.
	reloaderDone := make(chan struct{})

	go func() {
		defer close(reloaderDone)

		counter := 0
		for !stopReload.Load() {
			p.Reload(&stubRunner{id: "r"}, RunnerConfig{PollInterval: time.Hour, Timeout: time.Hour})
			counter++
		}
	}()

	wg.Wait()
	stopReload.Store(true)
	<-reloaderDone

	assert.Zero(t, nilSeen.Load(), "every current() must observe a non-nil runner once Reload installed one")
}

// TestPoller_ConcurrentResolverReads races concurrent current() calls
// against a mutating resolver config. The CAS loop inside
// resolveAndMaybeSwap must not drop updates or serve nil runners.
func TestPoller_ConcurrentResolverReads(t *testing.T) {
	t.Parallel()

	var (
		mu  sync.Mutex
		cfg = RunnerConfig{PollInterval: time.Hour, Timeout: time.Hour}
	)

	resolver := func() RunnerConfig {
		mu.Lock()
		defer mu.Unlock()

		return cfg
	}

	factory := func(
		_ sharedPorts.FetcherClient,
		_ discoveryRepos.ExtractionRepository,
		fcfg RunnerConfig,
		_ libLog.Logger,
	) (Runner, error) {
		return &stubRunner{id: fcfg.PollInterval.String()}, nil
	}

	p := NewPoller(&fakeFetcherClient{}, &fakeExtractionRepo{}, resolver, factory, &libLog.NopLogger{})

	const (
		readers        = 16
		readsPerReader = 300
	)

	var (
		wg      sync.WaitGroup
		nilSeen atomic.Int64
	)

	wg.Add(readers)

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()

			for j := 0; j < readsPerReader; j++ {
				got, err := p.current()
				if err != nil {
					t.Errorf("current: %v", err)
					return
				}

				if got == nil {
					nilSeen.Add(1)
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 50; i++ {
			mu.Lock()
			cfg = RunnerConfig{
				PollInterval: time.Duration(i+1) * time.Second,
				Timeout:      time.Duration(i+1) * time.Minute,
			}
			mu.Unlock()
		}
	}()

	wg.Wait()

	assert.Zero(t, nilSeen.Load(), "every current() must observe a non-nil runner")
}
