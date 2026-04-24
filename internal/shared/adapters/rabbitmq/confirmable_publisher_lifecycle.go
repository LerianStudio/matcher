// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package rabbitmq

import (
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	sharedPorts "github.com/LerianStudio/matcher/internal/shared/ports"
)

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
	if sharedPorts.IsNilValue(ch) {
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
	if sharedPorts.IsNilValue(ch) {
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
