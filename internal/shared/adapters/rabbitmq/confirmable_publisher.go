// Package rabbitmq provides shared RabbitMQ configuration and utilities.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/LerianStudio/lib-commons/v4/commons/backoff"
	libLog "github.com/LerianStudio/lib-commons/v4/commons/log"
	libRabbitmq "github.com/LerianStudio/lib-commons/v4/commons/rabbitmq"
	"github.com/LerianStudio/lib-commons/v4/commons/runtime"
)

// recoveryAttemptResult indicates the outcome of a single recovery attempt.
type recoveryAttemptResult int

const (
	recoveryAttemptRetry   recoveryAttemptResult = iota // retry next attempt
	recoveryAttemptSuccess                              // recovery succeeded
	recoveryAttemptAborted                              // recovery aborted externally
)

// MIGRATION(lib-uncommons): ConfirmablePublisher Migration Plan
//
// This type is a candidate for promotion to lib-uncommons/v2/commons/rabbitmq so
// that all Lerian services (Matcher, Midaz, and future services) can share a
// single, battle-tested publisher-confirms implementation.
//
// # Prerequisites (all must be met before migration)
//
//   - API stability: The ConfirmablePublisher API must be stable for at least
//     two release cycles (no breaking option or method signature changes).
//   - Test coverage: Unit test coverage must be >= 90%, including recovery edge
//     cases (partial failures, jitter determinism, concurrent recovery+publish).
//   - Feature completeness: Automatic channel recovery (exponential backoff,
//     health callbacks, configurable max attempts) must be implemented and
//     validated in production within the Matcher service first.
//   - Documentation: Godoc examples for all public types and option functions.
//
// # Features to add during migration
//
//   - Batch confirms: A PublishBatch method that publishes N messages and waits
//     for all confirmations in a single round-trip, reducing latency for
//     high-throughput scenarios (e.g., outbox dispatcher draining 100+ events).
//   - Metrics integration: Expose confirm latency histograms, nack counters,
//     and recovery attempt counters via OpenTelemetry metrics (not just logs).
//   - Connection-level recovery: In addition to channel recovery, support
//     full connection re-establishment when the AMQP connection itself drops.
//   - Builder API: Consider a fluent builder pattern instead of functional
//     options if the option count grows beyond 8-10 (readability trade-off).
//
// # Breaking changes to consider
//
//   - The ConfirmableChannel interface may need additional methods (e.g.,
//     IsClosed() bool) for health probing. Adding methods is a breaking change
//     for external implementors.
//   - The ChannelProvider function type signature may evolve if connection-level
//     recovery is added (it may need to accept a context or return a connection).
//   - Package path changes: lib-uncommons uses commons/rabbitmq as the base
//     package. Sentinel errors would move and importers must update.
//
// # Migration trigger
//
//   When 2+ Lerian services need reliable publisher confirms (Matcher already
//   does; if Midaz or another service adopts outbox+RabbitMQ), begin the
//   migration. File an issue in lib-uncommons with a link to this comment.

// Publisher confirm errors.
var (
	ErrConnectionRequired     = errors.New("rabbitmq connection is required")
	ErrChannelRequired        = errors.New("rabbitmq channel is required")
	ErrPublisherNotReady      = errors.New("confirmable publisher not initialized")
	ErrConfirmModeUnavailable = errors.New("channel does not support confirm mode")
	ErrPublishNacked          = errors.New("message was nacked by broker")
	ErrConfirmTimeout         = errors.New("confirmation timed out")
	ErrPublisherClosed        = errors.New("publisher is closed")
	ErrReconnectWhileOpen     = errors.New("cannot reconnect: publisher is still open, call Close first")
	ErrRecoveryExhausted      = errors.New("automatic recovery exhausted all attempts")
	ErrRecoveryDisabled       = errors.New("automatic recovery is not configured")
)

const (
	// DefaultConfirmTimeout is the default timeout for waiting on broker confirmation.
	DefaultConfirmTimeout = 5 * time.Second

	// confirmChannelBuffer is the buffer size for the confirmation channel.
	// Should be >= max unconfirmed messages to avoid blocking.
	confirmChannelBuffer = 256

	// DefaultMaxRecoveryAttempts is the default number of recovery attempts before giving up.
	DefaultMaxRecoveryAttempts = 10

	// DefaultRecoveryBackoffInitial is the starting backoff duration for recovery retries.
	DefaultRecoveryBackoffInitial = 1 * time.Second

	// DefaultRecoveryBackoffMax is the maximum backoff duration between recovery retries.
	DefaultRecoveryBackoffMax = 30 * time.Second
)

// HealthState represents the current connection health of a ConfirmablePublisher.
type HealthState int

const (
	// HealthStateConnected indicates the publisher has a healthy AMQP channel
	// and is ready to publish messages.
	HealthStateConnected HealthState = iota

	// HealthStateReconnecting indicates the publisher detected a channel closure
	// and is actively attempting to recover by obtaining a new channel.
	HealthStateReconnecting

	// HealthStateDisconnected indicates the publisher has exhausted all recovery
	// attempts and is no longer able to publish. Manual intervention is required.
	HealthStateDisconnected
)

// String returns a human-readable representation of the health state.
func (h HealthState) String() string {
	switch h {
	case HealthStateConnected:
		return "connected"
	case HealthStateReconnecting:
		return "reconnecting"
	case HealthStateDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

// ChannelProvider is a function that returns a new AMQP channel for recovery.
// It is called by the auto-recovery goroutine when the current channel closes.
// The returned channel must be a fresh, dedicated channel (not shared with
// other publishers). The provider should handle its own connection management
// internally -- for example, calling conn.Connection.Channel() on the
// underlying *amqp.Connection.
//
// If the provider returns an error, the recovery loop will retry with
// exponential backoff up to the configured maximum attempts.
type ChannelProvider func() (ConfirmableChannel, error)

// HealthCallback is called when the publisher's connection health changes.
// Implementations must be safe for concurrent use and should return quickly
// (avoid blocking operations). The callback runs in the recovery goroutine,
// so slow callbacks delay recovery attempts.
type HealthCallback func(HealthState)

// recoveryConfig holds the auto-recovery configuration.
// It is kept separate from the publisher to make the opt-in nature explicit:
// a nil recoveryConfig means auto-recovery is disabled.
type recoveryConfig struct {
	provider       ChannelProvider
	healthCallback HealthCallback
	maxAttempts    int
	backoffInitial time.Duration
	backoffMax     time.Duration
}

// ConfirmableChannel defines the interface for AMQP channel operations with confirms.
type ConfirmableChannel interface {
	Confirm(noWait bool) error
	NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	PublishWithContext(
		ctx context.Context,
		exchange, key string,
		mandatory, immediate bool,
		msg amqp.Publishing,
	) error
	Close() error
}

// ConfirmablePublisher wraps an AMQP channel with publisher confirms enabled.
// It ensures that each published message is acknowledged by the broker before
// returning success, providing at-least-once delivery semantics.
//
// Publish is serialized via publishMu so that each goroutine waits for exactly
// its own broker confirmation, preventing confirm cross-talk.
//
// # Channel Isolation
//
// Each ConfirmablePublisher MUST own a dedicated AMQP channel. AMQP publisher
// confirms are channel-scoped: calling Confirm(false) resets the delivery tag
// counter and confirmation state. If two publishers share the same channel, the
// second Confirm call will invalidate the first publisher's confirmation tracking,
// leading to silent message loss. Always open a new channel (e.g., via
// RabbitMQConnection.GetNewConnect()) for each publisher instance.
//
// # Reconnection
//
// ConfirmablePublisher supports two reconnection modes:
//
// Manual reconnection: When auto-recovery is not configured, channel closure
// causes all Publish calls to return ErrPublisherClosed. Callers must detect
// this error and call Close() followed by Reconnect(newChannel) manually.
//
// Automatic recovery: When configured via WithAutoRecovery, the publisher
// detects channel closure and automatically attempts to obtain a new channel
// from the provided ChannelProvider. Recovery uses exponential backoff with
// jitter and emits health state changes via the optional HealthCallback.
// During recovery, Publish calls return ErrPublisherClosed until a new channel
// is successfully established.
type ConfirmablePublisher struct {
	ch             ConfirmableChannel
	confirms       chan amqp.Confirmation
	closedCh       chan struct{}
	closeOnce      *sync.Once
	done           chan struct{}
	logger         libLog.Logger
	confirmTimeout time.Duration
	recovery       *recoveryConfig
	mu             sync.RWMutex
	publishMu      sync.Mutex
	closed         bool
}

// ConfirmablePublisherOption configures a ConfirmablePublisher.
type ConfirmablePublisherOption func(*ConfirmablePublisher)

// WithLogger sets a structured logger for the publisher.
func WithLogger(logger libLog.Logger) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		pub.logger = logger
	}
}

// WithConfirmTimeout sets the timeout for waiting on broker confirmation.
// Invalid values (<= 0) are logged and ignored; the default is kept.
func WithConfirmTimeout(timeout time.Duration) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		if timeout > 0 {
			pub.confirmTimeout = timeout
			return
		}

		logIfConfigured(pub.logger, libLog.LevelWarn,
			fmt.Sprintf("rabbitmq: ignoring invalid confirm timeout %v, using default", timeout))
	}
}

// WithAutoRecovery enables automatic channel recovery using the provided
// ChannelProvider. When the AMQP channel closes unexpectedly, the publisher
// will automatically attempt to obtain a new channel and resume operation.
//
// The provider function is called each time a recovery attempt is made. It
// should return a fresh, dedicated *amqp.Channel (typically by calling
// conn.Connection.Channel() on the underlying AMQP connection).
//
// Auto-recovery uses exponential backoff with jitter between attempts.
// Default backoff parameters are 1s initial / 30s max with 10 max attempts.
// Use WithRecoveryBackoff and WithMaxRecoveryAttempts to customize.
//
// Example:
//
//	publisher, err := NewConfirmablePublisherFromChannel(ch,
//	    WithAutoRecovery(func() (ConfirmableChannel, error) {
//	        return conn.Connection.Channel()
//	    }),
//	    WithHealthCallback(func(state HealthState) {
//	        log.Printf("publisher health: %s", state)
//	    }),
//	)
func WithAutoRecovery(provider ChannelProvider) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		if provider == nil {
			return
		}

		if pub.recovery == nil {
			pub.recovery = &recoveryConfig{
				maxAttempts:    DefaultMaxRecoveryAttempts,
				backoffInitial: DefaultRecoveryBackoffInitial,
				backoffMax:     DefaultRecoveryBackoffMax,
			}
		}

		pub.recovery.provider = provider
	}
}

// WithMaxRecoveryAttempts sets the maximum number of consecutive recovery
// attempts before the publisher gives up and transitions to disconnected state.
// Values <= 0 are ignored. Default is 10.
func WithMaxRecoveryAttempts(maxAttempts int) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		if maxAttempts <= 0 {
			return
		}

		if pub.recovery == nil {
			pub.recovery = &recoveryConfig{
				maxAttempts:    DefaultMaxRecoveryAttempts,
				backoffInitial: DefaultRecoveryBackoffInitial,
				backoffMax:     DefaultRecoveryBackoffMax,
			}
		}

		pub.recovery.maxAttempts = maxAttempts
	}
}

// WithRecoveryBackoff sets the initial and maximum backoff durations for
// recovery retries. The actual delay between attempts uses exponential backoff
// with full jitter: random(0, min(max, initial * 2^attempt)).
// Invalid values (<= 0) are ignored. Default is 1s initial / 30s max.
func WithRecoveryBackoff(initial, maxBackoff time.Duration) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		if initial <= 0 || maxBackoff <= 0 {
			return
		}

		if pub.recovery == nil {
			pub.recovery = &recoveryConfig{
				maxAttempts:    DefaultMaxRecoveryAttempts,
				backoffInitial: DefaultRecoveryBackoffInitial,
				backoffMax:     DefaultRecoveryBackoffMax,
			}
		}

		pub.recovery.backoffInitial = initial
		pub.recovery.backoffMax = maxBackoff
	}
}

// WithHealthCallback registers a function that is called whenever the
// publisher's connection health state changes. The callback receives the new
// HealthState (Connected, Reconnecting, or Disconnected).
//
// The callback runs in the recovery goroutine and must not block. If no
// callback is set, health transitions are only logged (when a logger is
// configured).
func WithHealthCallback(fn HealthCallback) ConfirmablePublisherOption {
	return func(pub *ConfirmablePublisher) {
		if fn == nil {
			return
		}

		if pub.recovery == nil {
			pub.recovery = &recoveryConfig{
				maxAttempts:    DefaultMaxRecoveryAttempts,
				backoffInitial: DefaultRecoveryBackoffInitial,
				backoffMax:     DefaultRecoveryBackoffMax,
			}
		}

		pub.recovery.healthCallback = fn
	}
}

// NewConfirmablePublisher creates a publisher with confirms enabled on the channel.
// The channel is put into confirm mode and a confirmation listener is started.
//
// WARNING: This uses conn.Channel directly, which is the shared connection-level
// channel. If multiple publishers are created from the same connection, they will
// share the channel and corrupt each other's confirm state. Use
// conn.GetNewConnect() to obtain a dedicated channel for each publisher, then
// call NewConfirmablePublisherFromChannel instead.
func NewConfirmablePublisher(
	conn *libRabbitmq.RabbitMQConnection,
	opts ...ConfirmablePublisherOption,
) (*ConfirmablePublisher, error) {
	if conn == nil {
		return nil, ErrConnectionRequired
	}

	if conn.Channel == nil {
		return nil, ErrChannelRequired
	}

	return NewConfirmablePublisherFromChannel(conn.Channel, opts...)
}

// NewConfirmablePublisherFromChannel creates a publisher from an existing channel.
// Useful for testing with mock channels.
func NewConfirmablePublisherFromChannel(
	ch ConfirmableChannel,
	opts ...ConfirmablePublisherOption,
) (*ConfirmablePublisher, error) {
	if isNilInterface(ch) {
		return nil, ErrChannelRequired
	}

	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrConfirmModeUnavailable, err)
	}

	confirms := make(chan amqp.Confirmation, confirmChannelBuffer)
	ch.NotifyPublish(confirms)

	closeNotify := ch.NotifyClose(make(chan *amqp.Error, 1))

	publisher := &ConfirmablePublisher{
		ch:             ch,
		confirms:       confirms,
		closedCh:       make(chan struct{}),
		closeOnce:      &sync.Once{},
		done:           make(chan struct{}),
		confirmTimeout: DefaultConfirmTimeout,
	}

	for _, opt := range opts {
		opt(publisher)
	}

	publisher.startCloseMonitor(closeNotify)

	return publisher, nil
}

// startCloseMonitor launches a goroutine that watches for AMQP channel close
// events and either triggers auto-recovery or broadcasts closure to waiters.
func (pub *ConfirmablePublisher) startCloseMonitor(closeNotify chan *amqp.Error) {
	// Capture local references so the goroutine does not race with Reconnect()
	// overwriting struct fields after Close() signals this goroutine to exit.
	monitorDone := pub.done
	monitorCloseOnce := pub.closeOnce
	monitorClosedCh := pub.closedCh

	runtime.SafeGo(nil, "confirmable-publisher-close-monitor", runtime.KeepRunning, func() {
		select {
		case amqpErr := <-closeNotify:
			// Broadcast closure so any in-flight Publish calls unblock.
			monitorCloseOnce.Do(func() { close(monitorClosedCh) })

			// If auto-recovery is configured, attempt to recover.
			pub.mu.RLock()
			hasRecovery := pub.recovery != nil && pub.recovery.provider != nil
			pub.mu.RUnlock()

			if hasRecovery {
				pub.attemptAutoRecovery(amqpErr)
			}
		case <-monitorDone:
			return
		}
	})
}

// attemptAutoRecovery runs the recovery loop with exponential backoff.
// It is called by the close-monitor goroutine when the channel closes
// and auto-recovery is configured.
func (pub *ConfirmablePublisher) attemptAutoRecovery(amqpErr *amqp.Error) {
	pub.mu.RLock()
	recovery := pub.recovery
	logger := pub.logger
	pub.mu.RUnlock()

	if recovery == nil || recovery.provider == nil {
		return
	}

	pub.emitHealthState(HealthStateReconnecting)
	pub.logChannelClosed(logger, amqpErr, recovery.maxAttempts)

	// Prepare the publisher for reconnection without calling the full Close()
	// method, which would close pub.done and make the recovery loop think it
	// should abort. Instead, we directly set the closed state and drain
	// pending confirmations.
	pub.prepareForRecovery()

	// Create a stop channel for the recovery loop. If an external Close()
	// is called while we're recovering, it will close pub.done -- which we
	// capture here. After prepareForRecovery, pub.done is a fresh channel
	// (not yet closed), so the recovery loop can safely watch it.
	pub.mu.RLock()
	recoveryStop := pub.done
	pub.mu.RUnlock()

	for attempt := range recovery.maxAttempts {
		result := pub.executeRecoveryAttempt(recovery, logger, recoveryStop, attempt)
		if result == recoveryAttemptSuccess || result == recoveryAttemptAborted {
			return
		}
	}

	// All attempts exhausted.
	logIfConfigured(
		logger,
		libLog.LevelError,
		fmt.Sprintf("rabbitmq: auto-recovery failed after %d attempts, publisher is disconnected", recovery.maxAttempts),
	)

	pub.emitHealthState(HealthStateDisconnected)
}

// logChannelClosed logs the channel closure event that triggered recovery.
func (pub *ConfirmablePublisher) logChannelClosed(logger libLog.Logger, amqpErr *amqp.Error, maxAttempts int) {
	if logger == nil {
		return
	}

	errMsg := "unknown"
	if amqpErr != nil {
		errMsg = amqpErr.Error()
	}

	logger.Log(context.Background(), libLog.LevelWarn, fmt.Sprintf("rabbitmq: channel closed (%s), starting auto-recovery (max %d attempts)",
		errMsg, maxAttempts))
}

// executeRecoveryAttempt performs a single recovery iteration: checks for external
// shutdown, waits with backoff, obtains a new channel, and attempts reconnection.
func (pub *ConfirmablePublisher) executeRecoveryAttempt(
	recovery *recoveryConfig,
	logger libLog.Logger,
	recoveryStop <-chan struct{},
	attempt int,
) recoveryAttemptResult {
	// Check if an external Close() was called during recovery.
	select {
	case <-recoveryStop:
		logIfConfigured(logger, libLog.LevelInfo, "rabbitmq: recovery aborted (publisher closed externally)")

		pub.emitHealthState(HealthStateDisconnected)

		return recoveryAttemptAborted
	default:
	}

	if aborted := pub.waitRecoveryBackoff(recovery, logger, recoveryStop, attempt); aborted {
		return recoveryAttemptAborted
	}

	return pub.tryReconnectChannel(recovery, logger, attempt)
}

// waitRecoveryBackoff sleeps for the calculated backoff duration, watching for
// external shutdown via the recoveryStop channel. Returns true if aborted.
func (pub *ConfirmablePublisher) waitRecoveryBackoff(
	recovery *recoveryConfig,
	logger libLog.Logger,
	recoveryStop <-chan struct{},
	attempt int,
) bool {
	// Calculate backoff with jitter, capped at max.
	delay := backoff.ExponentialWithJitter(recovery.backoffInitial, attempt)
	if delay > recovery.backoffMax {
		delay = backoff.FullJitter(recovery.backoffMax)
	}

	logIfConfigured(
		logger,
		libLog.LevelInfo,
		fmt.Sprintf("rabbitmq: recovery attempt %d/%d, backoff %v", attempt+1, recovery.maxAttempts, delay),
	)

	// Sleep with context cancellation tied to recoveryStop.
	sleepCtx, sleepCancel := context.WithCancel(context.Background())

	sleepDone := make(chan struct{})

	runtime.SafeGo(nil, "confirmable-publisher-recovery-sleep-watcher", runtime.KeepRunning, func() {
		select {
		case <-recoveryStop:
			sleepCancel()
		case <-sleepDone:
		}
	})

	sleepErr := backoff.WaitContext(sleepCtx, delay)

	close(sleepDone)
	sleepCancel()

	if sleepErr != nil {
		logIfConfigured(logger, libLog.LevelInfo, "rabbitmq: recovery aborted during backoff (publisher closed)")

		pub.emitHealthState(HealthStateDisconnected)

		return true
	}

	return false
}

// tryReconnectChannel attempts to obtain a new channel from the provider and
// reconnect the publisher. Returns the outcome of the attempt.
func (pub *ConfirmablePublisher) tryReconnectChannel(
	recovery *recoveryConfig,
	logger libLog.Logger,
	attempt int,
) recoveryAttemptResult {
	// Attempt to get a new channel from the provider.
	newCh, err := recovery.provider()
	if err != nil {
		logIfConfigured(
			logger,
			libLog.LevelWarn,
			fmt.Sprintf("rabbitmq: recovery attempt %d/%d failed: %v", attempt+1, recovery.maxAttempts, err),
		)

		return recoveryAttemptRetry
	}

	// Try to reconnect with the new channel.
	if err := pub.Reconnect(newCh); err != nil {
		logIfConfigured(
			logger,
			libLog.LevelWarn,
			fmt.Sprintf("rabbitmq: recovery attempt %d/%d reconnect failed: %v", attempt+1, recovery.maxAttempts, err),
		)

		// Best-effort close of the channel we just obtained.
		if !isNilInterface(newCh) {
			_ = newCh.Close()
		}

		return recoveryAttemptRetry
	}

	logIfConfigured(
		logger,
		libLog.LevelInfo,
		fmt.Sprintf("rabbitmq: auto-recovery succeeded on attempt %d/%d", attempt+1, recovery.maxAttempts),
	)

	pub.emitHealthState(HealthStateConnected)

	return recoveryAttemptSuccess
}

// prepareForRecovery transitions the publisher to a closed state suitable for
// Reconnect without closing the done channel (which the recovery loop needs
// to detect external shutdown). It drains pending confirmations and creates a
// fresh done channel for the recovery period.
func (pub *ConfirmablePublisher) prepareForRecovery() {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	if pub.closed {
		return
	}

	pub.closed = true

	// Signal the close-monitor goroutine to exit. This is the old done channel
	// associated with the goroutine that called us.
	close(pub.done)

	// Broadcast closure (likely already done by the close-monitor, but
	// closeOnce makes this idempotent).
	pub.closeOnce.Do(func() { close(pub.closedCh) })

	// Drain pending confirmations to prevent library deadlock.
	confirms := pub.confirms

	runtime.SafeGo(nil, "confirmable-publisher-drain", runtime.KeepRunning, func() {
		grace := time.NewTimer(pub.confirmTimeout)
		defer grace.Stop()

		for {
			select {
			case _, ok := <-confirms:
				if !ok {
					return
				}
			case <-grace.C:
				return
			}
		}
	})

	// Create a fresh done channel for the recovery loop. This channel is NOT
	// closed yet, so the recovery loop can watch it for external Close() calls.
	// If Reconnect succeeds, it will create yet another fresh done channel for
	// the new close-monitor goroutine.
	pub.done = make(chan struct{})
}

// emitHealthState notifies the health callback (if configured) of a state change.
func (pub *ConfirmablePublisher) emitHealthState(state HealthState) {
	pub.mu.RLock()
	recovery := pub.recovery
	pub.mu.RUnlock()

	if recovery == nil || recovery.healthCallback == nil {
		return
	}

	recovery.healthCallback(state)
}

// Publish sends a message and waits for broker confirmation.
// Returns nil on ack, error on nack, timeout, or channel close.
//
// Publish is serialized: only one goroutine may be in the publish+confirm
// sequence at a time. This prevents confirm cross-talk where goroutine A
// could consume goroutine B's confirmation from the shared channel.
func (pub *ConfirmablePublisher) Publish(
	ctx context.Context,
	exchange, routingKey string,
	mandatory, immediate bool,
	msg amqp.Publishing,
) error {
	// Serialize the entire publish+waitForConfirm sequence so each caller
	// receives exactly its own broker confirmation.
	pub.publishMu.Lock()
	defer pub.publishMu.Unlock()

	pub.mu.RLock()

	if pub.closed {
		pub.mu.RUnlock()
		return ErrPublisherClosed
	}

	if isNilInterface(pub.ch) {
		pub.mu.RUnlock()
		return ErrPublisherNotReady
	}

	publishChannel := pub.ch
	confirms := pub.confirms
	closedCh := pub.closedCh
	confirmTimeout := pub.confirmTimeout
	pub.mu.RUnlock()

	if err := publishChannel.PublishWithContext(ctx, exchange, routingKey, mandatory, immediate, msg); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return waitForConfirm(ctx, confirms, closedCh, confirmTimeout)
}

// waitForConfirm waits for broker confirmation of the last published message.
func waitForConfirm(
	ctx context.Context,
	confirms <-chan amqp.Confirmation,
	closedCh <-chan struct{},
	confirmTimeout time.Duration,
) error {
	timeout := time.NewTimer(confirmTimeout)
	defer timeout.Stop()

	select {
	case confirmed, ok := <-confirms:
		if !ok {
			return ErrPublisherClosed
		}

		if !confirmed.Ack {
			return fmt.Errorf("%w: delivery_tag=%d", ErrPublishNacked, confirmed.DeliveryTag)
		}

		return nil

	case <-closedCh:
		return ErrPublisherClosed

	case <-timeout.C:
		return ErrConfirmTimeout

	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	}
}

// Close drains pending confirmations and closes the publisher.
// The underlying channel is NOT closed - that should be managed by the connection owner.
func (pub *ConfirmablePublisher) Close() error {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	if pub.closed {
		return nil
	}

	pub.closed = true

	// Signal the close-monitor goroutine (spawned in constructor) to exit.
	close(pub.done)

	// Broadcast closure to any goroutine blocked in waitForConfirm.
	pub.closeOnce.Do(func() { close(pub.closedCh) })

	// Drain pending confirmations to prevent library deadlock.
	// See: https://github.com/rabbitmq/amqp091-go/issues/21
	//
	// Since pub.closed is true, no new Publish calls will proceed, so the
	// number of pending confirms is bounded. We drain the buffered channel
	// and then wait briefly for any in-flight confirms from the broker.
	// The goroutine exits when the confirms channel is closed by AMQP or
	// after a short grace period — preventing a permanent goroutine leak.
	confirms := pub.confirms

	runtime.SafeGo(nil, "confirmable-publisher-drain", runtime.KeepRunning, func() {
		// Give the broker a short grace period to deliver in-flight confirms.
		grace := time.NewTimer(pub.confirmTimeout)
		defer grace.Stop()

		for {
			select {
			case _, ok := <-confirms:
				if !ok {
					return
				}
			case <-grace.C:
				return
			}
		}
	})

	return nil
}

// Reconnect replaces the underlying AMQP channel with a fresh one after the
// publisher has been closed. The publisher must be in a closed state (call
// Close first). This resets all internal state -- confirms channel, close
// monitor, and the closed flag -- so the publisher is fully operational again.
//
// The caller is responsible for:
//  1. Detecting channel closure (Publish returns ErrPublisherClosed)
//  2. Obtaining a new channel (e.g., via RabbitMQConnection.GetNewConnect())
//  3. Calling Close() on this publisher
//  4. Calling Reconnect(newChannel)
//  5. Re-declaring exchanges/queues on the new channel if needed
//
// When auto-recovery is enabled (via WithAutoRecovery), this method is called
// automatically by the recovery goroutine. Manual callers should not call
// Reconnect concurrently with auto-recovery.
//
// Example:
//
//	if errors.Is(err, rabbitmq.ErrPublisherClosed) {
//	    newCh, _ := conn.GetNewConnect()
//	    _ = publisher.Close()
//	    _ = publisher.Reconnect(newCh)
//	}
func (pub *ConfirmablePublisher) Reconnect(ch ConfirmableChannel) error {
	if isNilInterface(ch) {
		return ErrChannelRequired
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()

	if !pub.closed {
		return ErrReconnectWhileOpen
	}

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("%w: %w", ErrConfirmModeUnavailable, err)
	}

	confirms := make(chan amqp.Confirmation, confirmChannelBuffer)
	ch.NotifyPublish(confirms)

	closeNotify := ch.NotifyClose(make(chan *amqp.Error, 1))

	pub.ch = ch
	pub.confirms = confirms
	pub.closedCh = make(chan struct{})
	pub.closeOnce = &sync.Once{}
	pub.done = make(chan struct{})
	pub.closed = false

	pub.startCloseMonitorLocked(closeNotify)

	return nil
}

// startCloseMonitorLocked is like startCloseMonitor but assumes pub.mu is held.
// It captures local references under the lock to avoid races.
func (pub *ConfirmablePublisher) startCloseMonitorLocked(closeNotify chan *amqp.Error) {
	monitorDone := pub.done
	monitorCloseOnce := pub.closeOnce
	monitorClosedCh := pub.closedCh

	runtime.SafeGo(nil, "confirmable-publisher-close-monitor", runtime.KeepRunning, func() {
		select {
		case amqpErr := <-closeNotify:
			monitorCloseOnce.Do(func() { close(monitorClosedCh) })

			pub.mu.RLock()
			hasRecovery := pub.recovery != nil && pub.recovery.provider != nil
			pub.mu.RUnlock()

			if hasRecovery {
				pub.attemptAutoRecovery(amqpErr)
			}
		case <-monitorDone:
			return
		}
	})
}

// Channel returns the underlying channel for operations that need direct access
// (e.g., exchange/queue declarations). Use with caution.
func (pub *ConfirmablePublisher) Channel() ConfirmableChannel {
	pub.mu.RLock()
	defer pub.mu.RUnlock()

	return pub.ch
}

func logIfConfigured(logger libLog.Logger, level libLog.Level, message string) {
	if isNilInterface(logger) {
		return
	}

	logger.Log(context.Background(), level, message)
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}

	v := reflect.ValueOf(value)

	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
