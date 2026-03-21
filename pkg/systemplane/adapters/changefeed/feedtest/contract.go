// Copyright 2025 Lerian Studio.

// Package feedtest provides contract test functions for ports.ChangeFeed implementations.
// Integration tests import these functions and run them against real backends
// (PostgreSQL LISTEN/NOTIFY, MongoDB change streams, etc.).
//
// Each test function accepts a FeedFactory that creates fresh, isolated
// Store + ChangeFeed pairs. The Store is required because signal emission
// is coupled to Store.Put() -- the change feed fires when data is written.
package feedtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

const (
	subscribeSignalWaitTimeout   = 10 * time.Second
	subscribeShutdownWaitTimeout = 5 * time.Second
	multipleSignalsCollectWait   = 3 * time.Second
	expectedFinalRevision        = domain.Revision(3)
	readyProbeRetryInterval      = 100 * time.Millisecond
	eventuallyPollInterval       = 50 * time.Millisecond
)

// FeedFactory creates a fresh Store + ChangeFeed pair for each test.
// The Store is needed because signal emission is coupled to Store.Put().
// The cleanup function tears down resources (connections, containers, etc.).
type FeedFactory func(t *testing.T) (ports.Store, ports.ChangeFeed, func())

// ---------------------------------------------------------------------------.
// ChangeFeed contract tests
// ---------------------------------------------------------------------------.

// TestSubscribeReceivesSignal verifies that a Put() to the Store causes
// the ChangeFeed handler to receive a ChangeSignal with the correct target
// and a revision greater than RevisionZero.
func TestSubscribeReceivesSignal(t *testing.T, factory FeedFactory) {
	store, feed, cleanup := factory(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	target := globalTarget(t)
	actor := testActor()

	// Channel to receive the first signal from the handler.
	signalCh := make(chan ports.ChangeSignal, 1)
	readyCh := make(chan struct{}, 1)

	// Start Subscribe in a background goroutine. It blocks until
	// the context is cancelled or an unrecoverable error occurs.
	subscribeDone := make(chan error, 1)

	runtime.SafeGo(nil, "feedtest.subscribe.receive_signal", runtime.KeepRunning, func() {
		subscribeDone <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
			select { //nolint:wsl // compact signal forwarding in closure
			case readyCh <- struct{}{}:
			default:
			}

			select {
			case signalCh <- sig:
			default:
				// Already received one signal; drop extras.
			}
		})
	})

	readyRevision := waitForSubscriptionReady(ctx, t, store, target, actor, readyCh)

	select {
	case <-signalCh:
	default:
	}

	// Write an entry to trigger a change signal.
	_, err := store.Put(ctx, target, []ports.WriteOp{
		{Key: "test.key", Value: "hello"},
	}, readyRevision, actor, "integration-test")
	require.NoError(t, err)

	// Wait for the signal with a generous timeout for slow containers.
	select {
	case sig := <-signalCh:
		assert.Equal(t, target, sig.Target,
			"signal target must match the written target")
		assert.Greater(t, sig.Revision.Uint64(), domain.RevisionZero.Uint64(),
			"signal revision must be greater than RevisionZero")
	case <-time.After(subscribeSignalWaitTimeout):
		t.Fatal("timed out waiting for change signal after Put()")
	}

	// Shut down the subscriber.
	cancel()

	select {
	case <-subscribeDone:
		// Subscribe returned after cancellation -- good.
	case <-time.After(subscribeShutdownWaitTimeout):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}

// TestContextCancellationStops verifies that cancelling the context
// causes Subscribe to return promptly without error (or with
// context.Canceled / context.DeadlineExceeded).
func TestContextCancellationStops(t *testing.T, factory FeedFactory) {
	_, feed, cleanup := factory(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	subscribeDone := make(chan error, 1)

	runtime.SafeGo(nil, "feedtest.subscribe.cancel", runtime.KeepRunning, func() {
		subscribeDone <- feed.Subscribe(ctx, func(_ ports.ChangeSignal) {
			// No-op handler; we only care about shutdown behavior.
		})
	})

	cancel()

	// Subscribe must return within a reasonable window.
	select {
	case err := <-subscribeDone:
		// Acceptable outcomes: nil, context.Canceled, or context.DeadlineExceeded.
		if err != nil {
			require.ErrorIs(t, err, context.Canceled,
				"Subscribe should return context.Canceled (or nil) on cancellation, got: %v", err)
		}
	case <-time.After(subscribeShutdownWaitTimeout):
		t.Fatal("Subscribe did not return within 5s after context cancellation")
	}
}

// TestMultipleSignals verifies that multiple Put() calls each produce
// a signal. Signals may be coalesced (debounce, MongoDB poll cycles),
// so we assert that at least one signal arrives and that its revision
// reflects the latest write.
func TestMultipleSignals(t *testing.T, factory FeedFactory) {
	store, feed, cleanup := factory(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	target := globalTarget(t)
	actor := testActor()

	// Collect all received signals in a thread-safe slice.
	var (
		mu      sync.Mutex
		signals []ports.ChangeSignal
	)

	readyCh := make(chan struct{}, 1)

	subscribeDone := make(chan error, 1)

	runtime.SafeGo(nil, "feedtest.subscribe.multiple_signals", runtime.KeepRunning, func() {
		subscribeDone <- feed.Subscribe(ctx, func(sig ports.ChangeSignal) {
			select {
			case readyCh <- struct{}{}:
			default:
			}

			mu.Lock()

			signals = append(signals, sig)
			mu.Unlock()
		})
	})

	prevRev := waitForSubscriptionReady(ctx, t, store, target, actor, readyCh)

	mu.Lock()
	signals = nil
	mu.Unlock()

	// Perform 3 sequential Puts: rev 0->1, 1->2, 2->3.
	for i := 1; i <= 3; i++ {
		rev, err := store.Put(ctx, target, []ports.WriteOp{
			{Key: "counter", Value: i},
		}, prevRev, actor, "integration-test")
		require.NoError(t, err, "Put #%d failed", i)

		prevRev = rev
	}

	// Wait for the latest revision to arrive. Coalescing is acceptable.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		if len(signals) == 0 {
			return false
		}

		return signals[len(signals)-1].Revision.Uint64() >= expectedFinalRevision.Uint64()
	}, multipleSignalsCollectWait, eventuallyPollInterval, "expected final revision signal after 3 Puts")

	mu.Lock()
	received := make([]ports.ChangeSignal, len(signals))
	copy(received, signals)
	mu.Unlock()

	latest := received[len(received)-1]
	assert.GreaterOrEqual(t, latest.Revision.Uint64(), expectedFinalRevision.Uint64(),
		"latest signal revision should be >= 3 (the final write revision)")

	cancel()

	select {
	case err := <-subscribeDone:
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
		}
	case <-time.After(subscribeShutdownWaitTimeout):
		t.Fatal("Subscribe did not return after context cancellation")
	}
}

// ---------------------------------------------------------------------------.
// RunAll convenience
// ---------------------------------------------------------------------------.

// RunAll runs all feed contract tests as sub-tests. This is the recommended
// entry point for integration tests that validate a ChangeFeed implementation.
func RunAll(t *testing.T, factory FeedFactory) {
	t.Helper()

	t.Run("SubscribeReceivesSignal", func(t *testing.T) {
		TestSubscribeReceivesSignal(t, factory)
	})
	t.Run("ContextCancellationStops", func(t *testing.T) {
		TestContextCancellationStops(t, factory)
	})
	t.Run("MultipleSignals", func(t *testing.T) {
		TestMultipleSignals(t, factory)
	})
}
