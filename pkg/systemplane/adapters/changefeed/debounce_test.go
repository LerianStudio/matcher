// Copyright 2025 Lerian Studio.

//go:build unit

package changefeed

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/pkg/systemplane/domain"
	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// fakeFeed is a test double for ports.ChangeFeed that captures the handler
// and blocks until the context is cancelled.
type fakeFeed struct {
	mu      sync.Mutex
	handler func(ports.ChangeSignal)
}

// Subscribe stores the handler and blocks until ctx is cancelled.
func (f *fakeFeed) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
	f.mu.Lock()
	f.handler = handler
	f.mu.Unlock()

	<-ctx.Done()

	return ctx.Err()
}

// emit sends a signal to the stored handler. Safe to call from any goroutine.
func (f *fakeFeed) emit(signal ports.ChangeSignal) {
	f.mu.Lock()
	h := f.handler
	f.mu.Unlock()

	if h != nil {
		h(signal)
	}
}

// makeTarget creates a valid domain.Target for testing. Panics on error
// because test helpers must not fail silently.
func makeTarget(kind domain.Kind, scope domain.Scope, subject string) domain.Target {
	t, err := domain.NewTarget(kind, scope, subject)
	if err != nil {
		panic(err)
	}

	return t
}

// ---------------------------------------------------------------------------
// Subscribe behavior
// ---------------------------------------------------------------------------

func TestSingleSignalPassesThrough(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(50*time.Millisecond),
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []ports.ChangeSignal
	var mu sync.Mutex

	// Start subscribe in background.
	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		})
	}()

	// Wait for handler registration.
	time.Sleep(20 * time.Millisecond)

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	inner.emit(ports.ChangeSignal{
		Target:   target,
		Revision: domain.Revision(1),
	})

	// Wait for debounce window to elapse.
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	require.Len(t, received, 1)
	assert.Equal(t, domain.Revision(1), received[0].Revision)
	assert.Equal(t, target, received[0].Target)
	mu.Unlock()

	cancel()
	<-done
}

func TestRapidSignalsCoalesced(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(80*time.Millisecond),
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []ports.ChangeSignal
	var mu sync.Mutex

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		})
	}()

	time.Sleep(20 * time.Millisecond)

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")

	// Emit 5 signals rapidly for the same target — each within the window.
	for i := range 5 {
		inner.emit(ports.ChangeSignal{
			Target:   target,
			Revision: domain.Revision(uint64(i + 1)),
		})
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for debounce window after last signal.
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	require.Len(t, received, 1, "rapid signals should be coalesced into one")
	assert.Equal(t, domain.Revision(5), received[0].Revision, "should deliver the latest revision")
	mu.Unlock()

	cancel()
	<-done
}

func TestRapidSignalsOutOfOrder_EmitsHighestRevision(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(80*time.Millisecond),
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []ports.ChangeSignal
	var mu sync.Mutex

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		})
	}()

	time.Sleep(20 * time.Millisecond)

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")

	inner.emit(ports.ChangeSignal{Target: target, Revision: domain.Revision(5)})
	time.Sleep(10 * time.Millisecond)
	inner.emit(ports.ChangeSignal{Target: target, Revision: domain.Revision(3)})
	time.Sleep(10 * time.Millisecond)
	inner.emit(ports.ChangeSignal{Target: target, Revision: domain.Revision(4)})

	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	require.Len(t, received, 1)
	assert.Equal(t, domain.Revision(5), received[0].Revision)
	mu.Unlock()

	cancel()
	<-done
}

func TestDifferentTargetsIndependent(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(50*time.Millisecond),
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received []ports.ChangeSignal
	var mu sync.Mutex

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(signal ports.ChangeSignal) {
			mu.Lock()
			received = append(received, signal)
			mu.Unlock()
		})
	}()

	time.Sleep(20 * time.Millisecond)

	targetA := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	targetB := makeTarget(domain.KindSetting, domain.ScopeTenant, "tenant-42")

	inner.emit(ports.ChangeSignal{Target: targetA, Revision: domain.Revision(1)})
	inner.emit(ports.ChangeSignal{Target: targetB, Revision: domain.Revision(10)})

	// Wait for both timers to fire.
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	require.Len(t, received, 2, "signals for different targets should both fire")

	targets := make(map[string]domain.Revision)
	for _, s := range received {
		targets[s.Target.String()] = s.Revision
	}

	assert.Equal(t, domain.Revision(1), targets[targetA.String()])
	assert.Equal(t, domain.Revision(10), targets[targetB.String()])
	mu.Unlock()

	cancel()
	<-done
}

func TestContextCancellation(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(_ ports.ChangeSignal) {})
	}()

	// Give subscribe time to start.
	time.Sleep(20 * time.Millisecond)

	cancel()

	err := <-done
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestHandlerSerialExecution(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(30*time.Millisecond),
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(_ ports.ChangeSignal) {
			cur := concurrent.Add(1)

			// Track the maximum observed concurrency.
			for {
				old := maxConcurrent.Load()
				if cur <= old || maxConcurrent.CompareAndSwap(old, cur) {
					break
				}
			}

			// Simulate work to widen the window for detecting concurrency.
			time.Sleep(20 * time.Millisecond)

			concurrent.Add(-1)
		})
	}()

	time.Sleep(20 * time.Millisecond)

	// Emit signals for two different targets simultaneously.
	targetA := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	targetB := makeTarget(domain.KindSetting, domain.ScopeTenant, "tenant-1")

	inner.emit(ports.ChangeSignal{Target: targetA, Revision: domain.Revision(1)})
	inner.emit(ports.ChangeSignal{Target: targetB, Revision: domain.Revision(1)})

	// Wait for both timers to fire and handlers to complete.
	time.Sleep(200 * time.Millisecond)

	assert.LessOrEqual(t, maxConcurrent.Load(), int32(1),
		"handler calls should be serialized — max concurrency must be 1")

	cancel()
	<-done
}

func TestPendingTimersCancelledOnExit(t *testing.T) {
	t.Parallel()

	inner := &fakeFeed{}
	df := NewDebouncedFeed(inner,
		WithWindow(500*time.Millisecond), // Long window — timer should NOT fire.
		WithJitter(0),
	)

	ctx, cancel := context.WithCancel(context.Background())

	var callCount atomic.Int32

	done := make(chan error, 1)

	go func() {
		done <- df.Subscribe(ctx, func(_ ports.ChangeSignal) {
			callCount.Add(1)
		})
	}()

	time.Sleep(20 * time.Millisecond)

	target := makeTarget(domain.KindConfig, domain.ScopeGlobal, "")
	inner.emit(ports.ChangeSignal{Target: target, Revision: domain.Revision(1)})

	// Cancel immediately — the 500ms timer should be stopped.
	time.Sleep(10 * time.Millisecond)
	cancel()
	<-done

	// Wait past the would-be fire time to confirm no handler invocation.
	time.Sleep(600 * time.Millisecond)

	assert.Equal(t, int32(0), callCount.Load(),
		"handler should not fire after context cancellation")
}
