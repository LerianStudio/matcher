// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package rabbitmq

import (
	"fmt"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

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
