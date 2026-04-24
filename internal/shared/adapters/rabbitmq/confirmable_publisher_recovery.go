// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/LerianStudio/lib-commons/v5/commons/backoff"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
		if !sharedPorts.IsNilValue(newCh) {
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
