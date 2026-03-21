// Copyright 2025 Lerian Studio.

// Package changefeed provides adapters and decorators for configuration change feeds.
package changefeed

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LerianStudio/lib-commons/v4/commons/runtime"

	"github.com/LerianStudio/matcher/pkg/systemplane/ports"
)

// DebouncedFeed wraps a ChangeFeed to coalesce rapid signals into fewer
// handler calls. It uses trailing-edge debounce: for each target the handler
// fires only after a quiet window has elapsed since the last signal. If
// multiple signals arrive for the same target within the window, only the
// latest signal is delivered.
//
// Signals for different targets are tracked independently, so a burst of
// updates to target A does not delay delivery of a signal for target B.
//
// Handler invocations are serialized: at most one call to handler() runs
// at any time across all targets. This prevents the downstream consumer
// from needing its own synchronization.
type DebouncedFeed struct {
	inner  ports.ChangeFeed
	window time.Duration
	jitter time.Duration
}

var (
	// ErrNilDebouncedFeed is returned when Subscribe is called on a nil receiver.
	ErrNilDebouncedFeed = errors.New("changefeed debounce: feed is nil")
	// ErrNilInnerFeed is returned when the wrapped feed is nil.
	ErrNilInnerFeed = errors.New("changefeed debounce: inner feed is nil")
	// ErrNilHandler is returned when Subscribe receives a nil handler.
	ErrNilHandler = errors.New("changefeed debounce: handler is nil")
)

// Compile-time interface check.
var _ ports.ChangeFeed = (*DebouncedFeed)(nil)

// DebounceOption configures a DebouncedFeed.
type DebounceOption func(*DebouncedFeed)

// WithWindow sets the debounce window duration. Values <= 0 are ignored.
func WithWindow(d time.Duration) DebounceOption {
	return func(df *DebouncedFeed) {
		if d > 0 {
			df.window = d
		}
	}
}

// WithJitter sets the random jitter added to the debounce window.
// A value of 0 disables jitter. Negative values are ignored.
func WithJitter(d time.Duration) DebounceOption {
	return func(df *DebouncedFeed) {
		if d >= 0 {
			df.jitter = d
		}
	}
}

const (
	defaultWindow = 100 * time.Millisecond
	defaultJitter = 50 * time.Millisecond
)

// NewDebouncedFeed wraps an inner ChangeFeed with trailing-edge debounce.
// Default window is 100ms with 50ms jitter. Use WithWindow and WithJitter
// to override.
func NewDebouncedFeed(inner ports.ChangeFeed, opts ...DebounceOption) *DebouncedFeed {
	df := &DebouncedFeed{
		inner:  inner,
		window: defaultWindow,
		jitter: defaultJitter,
	}

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt(df)
	}

	return df
}

// Subscribe registers a handler that is called with debounced signals.
// Internally it subscribes to the wrapped ChangeFeed and coalesces rapid
// signals per target. The method blocks until ctx is cancelled or the inner
// feed returns an error.
func (df *DebouncedFeed) Subscribe(ctx context.Context, handler func(ports.ChangeSignal)) error {
	if df == nil {
		return ErrNilDebouncedFeed
	}

	if isNilChangeFeed(df.inner) {
		return ErrNilInnerFeed
	}

	if handler == nil {
		return ErrNilHandler
	}

	inputCh, errCh := df.launchInnerSubscribe(ctx)

	ds := &debounceState{
		pending: make(map[string]*debounceEntry),
	}

	for {
		ds.resetTimer()

		select {
		case <-ctx.Done():
			ds.stopTimer()
			return fmt.Errorf("changefeed debounce subscribe: context done: %w", ctx.Err())
		case signal := <-inputCh:
			df.handleSignal(ds, signal)
		case <-ds.timerCh:
			if err := df.fireReady(ds, handler); err != nil {
				return err
			}
		case err := <-errCh:
			return df.handleInnerExit(ctx, ds, handler, err)
		}
	}
}

// launchInnerSubscribe starts the inner feed subscription in a background
// goroutine and returns channels for signals and the terminal error.
func (df *DebouncedFeed) launchInnerSubscribe(ctx context.Context) (<-chan ports.ChangeSignal, <-chan error) {
	inputCh := make(chan ports.ChangeSignal)
	errCh := make(chan error, 1)

	runtime.SafeGo(nil, "changefeed.debounce.subscribe", runtime.KeepRunning, func() {
		errCh <- df.inner.Subscribe(ctx, func(signal ports.ChangeSignal) {
			select {
			case inputCh <- signal:
			case <-ctx.Done():
			}
		})
	})

	return inputCh, errCh
}

// handleSignal upserts a signal into the pending map, keeping the highest
// revision and strongest ApplyBehavior seen, then extending the debounce
// deadline.
func (df *DebouncedFeed) handleSignal(ds *debounceState, signal ports.ChangeSignal) {
	key := signal.Target.String()

	entry, exists := ds.pending[key]
	if !exists {
		ds.pending[key] = &debounceEntry{signal: signal, dueAt: time.Now().Add(df.debounceDuration())}
		return
	}

	// Keep the highest revision seen.
	if signal.Revision >= entry.signal.Revision {
		entry.signal.Revision = signal.Revision
		entry.signal.Target = signal.Target
	}

	// Escalate to the strongest apply behavior seen in this window.
	if signal.ApplyBehavior.Strength() > entry.signal.ApplyBehavior.Strength() {
		entry.signal.ApplyBehavior = signal.ApplyBehavior
	}

	entry.dueAt = time.Now().Add(df.debounceDuration())
}

// fireReady delivers all signals whose debounce window has elapsed.
func (df *DebouncedFeed) fireReady(ds *debounceState, handler func(ports.ChangeSignal)) error {
	for _, signal := range ds.collectSignals(time.Now(), false) {
		if err := SafeInvokeHandler(handler, signal); err != nil {
			ds.stopTimer()
			return fmt.Errorf("changefeed debounce subscribe: %w", err)
		}
	}

	return nil
}

// handleInnerExit processes the terminal error from the inner feed. If the
// inner feed failed (non-cancelled), it flushes all pending signals before
// returning the error.
func (df *DebouncedFeed) handleInnerExit(
	ctx context.Context,
	ds *debounceState,
	handler func(ports.ChangeSignal),
	err error,
) error {
	ds.stopTimer()

	if err != nil && !errors.Is(err, context.Canceled) && ctx.Err() == nil {
		for _, signal := range ds.collectSignals(time.Now(), true) {
			if handlerErr := SafeInvokeHandler(handler, signal); handlerErr != nil {
				return fmt.Errorf("changefeed debounce subscribe: %w", handlerErr)
			}
		}

		return fmt.Errorf("changefeed debounce subscribe: %w", err)
	}

	if ctx.Err() != nil {
		return fmt.Errorf("changefeed debounce subscribe: context done: %w", ctx.Err())
	}

	return nil
}
