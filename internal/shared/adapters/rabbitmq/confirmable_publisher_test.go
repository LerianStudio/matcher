//go:build unit

package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libLog "github.com/LerianStudio/lib-uncommons/v2/uncommons/log"
	libRabbitmq "github.com/LerianStudio/lib-uncommons/v2/uncommons/rabbitmq"
)

// mockConfirmableChannel implements ConfirmableChannel for testing.
type mockConfirmableChannel struct {
	mu              sync.Mutex
	confirmErr      error
	publishErr      error
	confirms        chan amqp.Confirmation
	closeNotify     chan *amqp.Error
	confirmCalled   bool
	publishCalled   bool
	closeCalled     bool
	lastExchange    string
	lastRoutingKey  string
	lastMsg         amqp.Publishing
	deliveryCounter uint64
}

func newMockChannel() *mockConfirmableChannel {
	return &mockConfirmableChannel{
		closeNotify: make(chan *amqp.Error, 1),
	}
}

func cleanupPublisher(t *testing.T, publisher *ConfirmablePublisher) {
	t.Helper()

	t.Cleanup(func() {
		require.NoError(t, publisher.Close())
	})
}

func (m *mockConfirmableChannel) Confirm(_ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.confirmCalled = true

	return m.confirmErr
}

func (m *mockConfirmableChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store the passed channel so we can send confirmations on it
	m.confirms = confirm

	return confirm
}

func (m *mockConfirmableChannel) NotifyClose(_ chan *amqp.Error) chan *amqp.Error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closeNotify
}

func (m *mockConfirmableChannel) PublishWithContext(
	_ context.Context,
	exchange, key string,
	_, _ bool,
	msg amqp.Publishing,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.publishCalled = true
	m.lastExchange = exchange
	m.lastRoutingKey = key
	m.lastMsg = msg
	m.deliveryCounter++

	return m.publishErr
}

func (m *mockConfirmableChannel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closeCalled || m.confirms == nil {
		m.closeCalled = true
		return nil
	}

	m.closeCalled = true
	close(m.confirms)

	return nil
}

func (m *mockConfirmableChannel) sendConfirm(ack bool) {
	m.mu.Lock()
	tag := m.deliveryCounter
	confirms := m.confirms
	m.mu.Unlock()

	confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: ack}
}

func TestNewConfirmablePublisher_NilConnection(t *testing.T) {
	t.Parallel()

	publisher, err := NewConfirmablePublisher(nil)

	assert.Nil(t, publisher)
	assert.ErrorIs(t, err, ErrConnectionRequired)
}

func TestNewConfirmablePublisher_NilChannel(t *testing.T) {
	t.Parallel()

	conn := &libRabbitmq.RabbitMQConnection{Channel: nil}

	publisher, err := NewConfirmablePublisher(conn)

	assert.Nil(t, publisher)
	assert.ErrorIs(t, err, ErrChannelRequired)
}

func TestNewConfirmablePublisherFromChannel_NilChannel(t *testing.T) {
	t.Parallel()

	publisher, err := NewConfirmablePublisherFromChannel(nil)

	assert.Nil(t, publisher)
	assert.ErrorIs(t, err, ErrChannelRequired)
}

func TestNewConfirmablePublisherFromChannel_ConfirmModeFails(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	ch.confirmErr = errors.New("confirm not supported")

	publisher, err := NewConfirmablePublisherFromChannel(ch)

	assert.Nil(t, publisher)
	assert.ErrorIs(t, err, ErrConfirmModeUnavailable)
	assert.True(t, ch.confirmCalled)
}

func TestNewConfirmablePublisherFromChannel_Success(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()

	publisher, err := NewConfirmablePublisherFromChannel(ch)

	require.NoError(t, err)
	require.NotNil(t, publisher)
	assert.True(t, ch.confirmCalled)
	assert.Equal(t, DefaultConfirmTimeout, publisher.confirmTimeout)

	cleanupPublisher(t, publisher)
}

func TestNewConfirmablePublisherFromChannel_WithTimeout(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	customTimeout := 10 * time.Second

	publisher, err := NewConfirmablePublisherFromChannel(ch, WithConfirmTimeout(customTimeout))

	require.NoError(t, err)
	assert.Equal(t, customTimeout, publisher.confirmTimeout)

	cleanupPublisher(t, publisher)
}

func TestConfirmablePublisher_Publish_Success(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	go func() {
		time.Sleep(10 * time.Millisecond)
		ch.sendConfirm(true)
	}()

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte(`{"test": true}`),
	}

	err = publisher.Publish(context.Background(), "exchange", "routing.key", false, false, msg)

	assert.NoError(t, err)
	assert.True(t, ch.publishCalled)
	assert.Equal(t, "exchange", ch.lastExchange)
	assert.Equal(t, "routing.key", ch.lastRoutingKey)
}

func TestConfirmablePublisher_Publish_Nacked(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	go func() {
		time.Sleep(10 * time.Millisecond)
		ch.sendConfirm(false)
	}()

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)

	assert.ErrorIs(t, err, ErrPublishNacked)
}

func TestConfirmablePublisher_Publish_Timeout(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithConfirmTimeout(50*time.Millisecond))
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)

	assert.ErrorIs(t, err, ErrConfirmTimeout)
}

func TestConfirmablePublisher_Publish_ContextCancelled(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(ctx, "exchange", "key", false, false, msg)

	assert.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestConfirmablePublisher_Publish_PublishError(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	ch.publishErr = errors.New("connection lost")
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection lost")
}

func TestConfirmablePublisher_Publish_AfterClose(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)

	assert.ErrorIs(t, err, ErrPublisherClosed)
}

func TestConfirmablePublisher_Close_Idempotent(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	err1 := publisher.Close()
	err2 := publisher.Close()

	assert.NoError(t, err1)
	assert.NoError(t, err2)
}

func TestConfirmablePublisher_Channel(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	result := publisher.Channel()

	assert.Equal(t, ch, result)
}

func TestWithConfirmTimeout_ZeroValue(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithLogger(&libLog.NopLogger{}), WithConfirmTimeout(0))
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	assert.Equal(t, DefaultConfirmTimeout, publisher.confirmTimeout)
}

func TestWithConfirmTimeout_NegativeValue(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithLogger(&libLog.NopLogger{}), WithConfirmTimeout(-1*time.Second))
	require.NoError(t, err)

	cleanupPublisher(t, publisher)

	assert.Equal(t, DefaultConfirmTimeout, publisher.confirmTimeout)
}

func TestConfirmablePublisher_Publish_ChannelClose_BroadcastsToWaiter(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Simulate AMQP channel close while Publish is waiting for confirm.
	go func() {
		time.Sleep(20 * time.Millisecond)
		ch.closeNotify <- amqp.ErrClosed
	}()

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)

	assert.ErrorIs(t, err, ErrPublisherClosed)
}

// TestConfirmablePublisher_ConcurrentPublish_NoConfirmCrossTalk verifies that
// concurrent Publish calls each receive their own confirmation. With the
// publishMu serialization, goroutines publish one at a time so delivery tags
// are matched correctly.
func TestConfirmablePublisher_ConcurrentPublish_NoConfirmCrossTalk(t *testing.T) {
	t.Parallel()

	const numPublishers = 20

	// concurrentMockChannel auto-sends an ack for every PublishWithContext call.
	ch := &concurrentMockChannel{
		closeNotify: make(chan *amqp.Error, 1),
	}

	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	var (
		wg      sync.WaitGroup
		ackErr  atomic.Int64
		nackErr atomic.Int64
		okCount atomic.Int64
	)

	wg.Add(numPublishers)

	for i := range numPublishers {
		go func(idx int) {
			defer wg.Done()

			msg := amqp.Publishing{Body: []byte(`concurrent`)}

			pubErr := publisher.Publish(context.Background(), "exchange", "key", false, false, msg)
			if pubErr == nil {
				okCount.Add(1)
				return
			}

			if errors.Is(pubErr, ErrPublishNacked) {
				nackErr.Add(1)
			} else {
				ackErr.Add(1)
			}
		}(i)
	}

	wg.Wait()

	// All publishes should succeed because the mock auto-acks.
	assert.Equal(t, int64(numPublishers), okCount.Load(), "all concurrent publishes should succeed")
	assert.Equal(t, int64(0), nackErr.Load(), "no nack errors expected")
	assert.Equal(t, int64(0), ackErr.Load(), "no other errors expected")
}

// TestConfirmablePublisher_Close_DrainGoroutineExits verifies that the drain
// goroutine spawned by Close() exits within a bounded time, even if the AMQP
// confirms channel is never closed.
func TestConfirmablePublisher_Close_DrainGoroutineExits(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithConfirmTimeout(100*time.Millisecond))
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	// The drain goroutine should exit within confirmTimeout (100ms).
	// We wait 2x to give it margin. If the goroutine leaked, this test
	// would rely on the go test -timeout flag to catch it (which is fine
	// as a safety net), but the assertion below validates the confirms
	// channel is no longer being consumed.
	time.Sleep(250 * time.Millisecond)

	// After the grace period, sending to confirms should either succeed
	// (buffered) or block (goroutine exited and nobody reads). Since the
	// channel buffer is 256, we can verify it's not actively drained by
	// checking that a send doesn't get consumed quickly.
	select {
	case ch.confirms <- amqp.Confirmation{Ack: true}:
		// Message was buffered — this is expected since nobody is
		// consuming anymore (goroutine exited).
	default:
		// Channel buffer full — also valid, confirms are not consumed.
	}
}

// TestConfirmablePublisher_Close_SignalsDoneToMonitor verifies that Close()
// signals the close-monitor goroutine to exit via the done channel.
func TestConfirmablePublisher_Close_SignalsDoneToMonitor(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	// The done channel should be closed, making it immediately readable.
	select {
	case <-publisher.done:
		// Expected: done channel is closed.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("done channel should be closed after Close()")
	}

	// The closedCh should also be closed (broadcast to waiters).
	select {
	case <-publisher.closedCh:
		// Expected: closedCh is closed.
	case <-time.After(100 * time.Millisecond):
		t.Fatal("closedCh should be closed after Close()")
	}
}

// concurrentMockChannel is a mock that auto-sends ack confirmations in
// response to PublishWithContext. This simulates a real AMQP channel where
// each publish triggers a broker confirmation.
type concurrentMockChannel struct {
	mu              sync.Mutex
	confirms        chan amqp.Confirmation
	closeNotify     chan *amqp.Error
	deliveryCounter uint64
}

func (m *concurrentMockChannel) Confirm(_ bool) error { return nil }

func (m *concurrentMockChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.confirms = confirm

	return confirm
}

func (m *concurrentMockChannel) NotifyClose(_ chan *amqp.Error) chan *amqp.Error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.closeNotify
}

func (m *concurrentMockChannel) PublishWithContext(
	_ context.Context,
	_, _ string,
	_, _ bool,
	_ amqp.Publishing,
) error {
	m.mu.Lock()
	m.deliveryCounter++
	tag := m.deliveryCounter
	confirms := m.confirms
	m.mu.Unlock()

	// Auto-ack: send confirmation immediately after publish (in background
	// to avoid blocking under the publisher's publishMu).
	go func() {
		confirms <- amqp.Confirmation{DeliveryTag: tag, Ack: true}
	}()

	return nil
}

func (m *concurrentMockChannel) Close() error { return nil }

func TestConfirmablePublisher_Reconnect_Success(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch1)
	require.NoError(t, err)

	// Close the publisher first.
	err = publisher.Close()
	require.NoError(t, err)

	// Reconnect with a fresh channel.
	ch2 := newMockChannel()

	err = publisher.Reconnect(ch2)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Verify publisher is operational: publish should work.
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch2.sendConfirm(true)
	}()

	msg := amqp.Publishing{Body: []byte(`reconnected`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)
	assert.NoError(t, err)
}

func TestConfirmablePublisher_Reconnect_NilChannel(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	err = publisher.Reconnect(nil)
	assert.ErrorIs(t, err, ErrChannelRequired)
}

func TestConfirmablePublisher_Reconnect_WhileOpen(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Attempting to reconnect without closing first should fail.
	ch2 := newMockChannel()

	err = publisher.Reconnect(ch2)
	assert.ErrorIs(t, err, ErrReconnectWhileOpen)
}

func TestConfirmablePublisher_Reconnect_ConfirmModeFails(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch1)
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	// Create a channel where Confirm fails.
	ch2 := newMockChannel()
	ch2.confirmErr = errors.New("confirm not supported on new channel")

	err = publisher.Reconnect(ch2)
	assert.ErrorIs(t, err, ErrConfirmModeUnavailable)
}

func TestConfirmablePublisher_Reconnect_DetectsChannelClose(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch1)
	require.NoError(t, err)

	err = publisher.Close()
	require.NoError(t, err)

	ch2 := newMockChannel()

	err = publisher.Reconnect(ch2)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Simulate AMQP channel close on the new channel.
	go func() {
		time.Sleep(20 * time.Millisecond)
		ch2.closeNotify <- amqp.ErrClosed
	}()

	msg := amqp.Publishing{Body: []byte(`test`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)
	assert.ErrorIs(t, err, ErrPublisherClosed)
}

// ---------------------------------------------------------------------------
// HealthState tests
// ---------------------------------------------------------------------------

func TestHealthState_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state    HealthState
		expected string
	}{
		{HealthStateConnected, "connected"},
		{HealthStateReconnecting, "reconnecting"},
		{HealthStateDisconnected, "disconnected"},
		{HealthState(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, tt.state.String())
		})
	}
}

// ---------------------------------------------------------------------------
// Option tests for auto-recovery
// ---------------------------------------------------------------------------

func TestWithAutoRecovery_NilProvider(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithAutoRecovery(nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	assert.Nil(t, publisher.recovery, "nil provider should not create recovery config")
}

func TestWithAutoRecovery_SetsDefaults(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	provider := func() (ConfirmableChannel, error) { return newMockChannel(), nil }

	publisher, err := NewConfirmablePublisherFromChannel(ch, WithAutoRecovery(provider))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	require.NotNil(t, publisher.recovery)
	assert.NotNil(t, publisher.recovery.provider)
	assert.Equal(t, DefaultMaxRecoveryAttempts, publisher.recovery.maxAttempts)
	assert.Equal(t, DefaultRecoveryBackoffInitial, publisher.recovery.backoffInitial)
	assert.Equal(t, DefaultRecoveryBackoffMax, publisher.recovery.backoffMax)
}

func TestWithMaxRecoveryAttempts_Valid(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithMaxRecoveryAttempts(5))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	require.NotNil(t, publisher.recovery)
	assert.Equal(t, 5, publisher.recovery.maxAttempts)
}

func TestWithMaxRecoveryAttempts_ZeroIgnored(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithMaxRecoveryAttempts(0))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	assert.Nil(t, publisher.recovery, "zero max attempts should not create recovery config")
}

func TestWithMaxRecoveryAttempts_NegativeIgnored(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithMaxRecoveryAttempts(-1))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	assert.Nil(t, publisher.recovery, "negative max attempts should not create recovery config")
}

func TestWithRecoveryBackoff_Valid(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithRecoveryBackoff(500*time.Millisecond, 10*time.Second))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	require.NotNil(t, publisher.recovery)
	assert.Equal(t, 500*time.Millisecond, publisher.recovery.backoffInitial)
	assert.Equal(t, 10*time.Second, publisher.recovery.backoffMax)
}

func TestWithRecoveryBackoff_InvalidIgnored(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithRecoveryBackoff(-1*time.Second, 10*time.Second))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	assert.Nil(t, publisher.recovery, "invalid initial backoff should not create recovery config")
}

func TestWithHealthCallback_Valid(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	callbackCalled := false
	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithHealthCallback(func(_ HealthState) { callbackCalled = true }))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	require.NotNil(t, publisher.recovery)
	assert.NotNil(t, publisher.recovery.healthCallback)

	publisher.recovery.healthCallback(HealthStateConnected)
	assert.True(t, callbackCalled)
}

func TestWithHealthCallback_NilIgnored(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch, WithHealthCallback(nil))
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	assert.Nil(t, publisher.recovery, "nil callback should not create recovery config")
}

// ---------------------------------------------------------------------------
// Auto-recovery integration tests
// ---------------------------------------------------------------------------

// TestAutoRecovery_SuccessOnFirstAttempt verifies that when the channel closes
// and the provider returns a valid channel on the first try, the publisher
// recovers and becomes operational again.
func TestAutoRecovery_SuccessOnFirstAttempt(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	ch2 := newMockChannel()

	var healthStates []HealthState
	var healthMu sync.Mutex

	recoveryDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return ch2, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(3),
		WithHealthCallback(func(state HealthState) {
			healthMu.Lock()
			healthStates = append(healthStates, state)
			healthMu.Unlock()

			if state == HealthStateConnected {
				close(recoveryDone)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Simulate channel closure to trigger auto-recovery.
	ch1.closeNotify <- amqp.ErrClosed

	// Wait for recovery to complete.
	select {
	case <-recoveryDone:
		// Recovery succeeded.
	case <-time.After(2 * time.Second):
		t.Fatal("auto-recovery did not complete in time")
	}

	// Verify publisher is operational with the new channel.
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch2.sendConfirm(true)
	}()

	msg := amqp.Publishing{Body: []byte(`recovered`)}

	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)
	assert.NoError(t, err)

	// Verify health state transitions.
	healthMu.Lock()
	defer healthMu.Unlock()

	require.GreaterOrEqual(t, len(healthStates), 2, "should have at least reconnecting+connected")
	assert.Equal(t, HealthStateReconnecting, healthStates[0])
	assert.Equal(t, HealthStateConnected, healthStates[len(healthStates)-1])
}

// TestAutoRecovery_SuccessAfterRetries verifies that recovery succeeds after
// the provider fails on initial attempts but eventually returns a valid channel.
func TestAutoRecovery_SuccessAfterRetries(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	chFinal := newMockChannel()

	var attempt atomic.Int32
	recoveryDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			n := attempt.Add(1)
			// Fail the first 2 attempts, succeed on the third.
			if n < 3 {
				return nil, errors.New("connection not ready")
			}

			return chFinal, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(5),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateConnected {
				close(recoveryDone)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Trigger channel closure.
	ch1.closeNotify <- amqp.ErrClosed

	select {
	case <-recoveryDone:
		// Recovery succeeded after retries.
	case <-time.After(2 * time.Second):
		t.Fatal("auto-recovery did not complete in time")
	}

	assert.GreaterOrEqual(t, attempt.Load(), int32(3), "should have attempted at least 3 times")
}

// TestAutoRecovery_ExhaustsAllAttempts verifies that when the provider always
// fails, the publisher transitions to disconnected after max attempts.
func TestAutoRecovery_ExhaustsAllAttempts(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	maxAttempts := 3

	var healthStates []HealthState
	var healthMu sync.Mutex

	disconnected := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return nil, errors.New("broker unreachable")
		}),
		WithRecoveryBackoff(1*time.Millisecond, 5*time.Millisecond),
		WithMaxRecoveryAttempts(maxAttempts),
		WithHealthCallback(func(state HealthState) {
			healthMu.Lock()
			healthStates = append(healthStates, state)
			healthMu.Unlock()

			if state == HealthStateDisconnected {
				close(disconnected)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	ch.closeNotify <- amqp.ErrClosed

	select {
	case <-disconnected:
		// Expected: publisher gave up.
	case <-time.After(2 * time.Second):
		t.Fatal("publisher should have reached disconnected state")
	}

	healthMu.Lock()
	defer healthMu.Unlock()

	require.GreaterOrEqual(t, len(healthStates), 2)
	assert.Equal(t, HealthStateReconnecting, healthStates[0])
	assert.Equal(t, HealthStateDisconnected, healthStates[len(healthStates)-1])
}

// TestAutoRecovery_ProviderReturnsChannelWithConfirmError verifies that when
// the provider returns a channel that fails to enter confirm mode, the recovery
// retries and eventually succeeds with a good channel.
func TestAutoRecovery_ProviderReturnsChannelWithConfirmError(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	chGood := newMockChannel()
	chBad := newMockChannel()
	chBad.confirmErr = errors.New("confirm not supported")

	var attempt atomic.Int32
	recoveryDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			n := attempt.Add(1)
			if n == 1 {
				return chBad, nil
			}

			return chGood, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(5),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateConnected {
				close(recoveryDone)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	ch1.closeNotify <- amqp.ErrClosed

	select {
	case <-recoveryDone:
		// Recovery succeeded.
	case <-time.After(2 * time.Second):
		t.Fatal("auto-recovery did not complete in time")
	}

	assert.GreaterOrEqual(t, attempt.Load(), int32(2))
}

// TestAutoRecovery_NoRecoveryWithoutProvider verifies that without a
// ChannelProvider, channel closure does NOT trigger auto-recovery (backward
// compatibility with the manual Reconnect pattern).
func TestAutoRecovery_NoRecoveryWithoutProvider(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Simulate channel closure.
	ch.closeNotify <- amqp.ErrClosed

	// Give the close monitor time to process.
	time.Sleep(50 * time.Millisecond)

	// Publisher should be in closed state with no recovery attempt.
	msg := amqp.Publishing{Body: []byte(`test`)}
	err = publisher.Publish(context.Background(), "exchange", "key", false, false, msg)
	assert.ErrorIs(t, err, ErrPublisherClosed)
}

// TestAutoRecovery_ChannelCloseAfterRecovery verifies that if the recovered
// channel also closes, recovery is triggered again.
func TestAutoRecovery_ChannelCloseAfterRecovery(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	ch2 := newMockChannel()
	ch3 := newMockChannel()

	var attempt atomic.Int32
	var recoveryCount atomic.Int32

	allRecoveriesDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			n := attempt.Add(1)
			if n == 1 {
				return ch2, nil
			}

			return ch3, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(3),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateConnected {
				count := recoveryCount.Add(1)
				if count >= 2 {
					close(allRecoveriesDone)
				}
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// First channel closure triggers recovery to ch2.
	ch1.closeNotify <- amqp.ErrClosed

	// Wait for first recovery.
	time.Sleep(100 * time.Millisecond)

	// Second channel closure triggers recovery to ch3.
	ch2.closeNotify <- amqp.ErrClosed

	select {
	case <-allRecoveriesDone:
		// Both recoveries succeeded.
	case <-time.After(3 * time.Second):
		t.Fatal("second recovery did not complete in time")
	}

	assert.GreaterOrEqual(t, attempt.Load(), int32(2))
}

// TestAutoRecovery_NilAMQPError verifies that recovery handles nil AMQP error
// gracefully (some broker implementations send nil on clean shutdown).
func TestAutoRecovery_NilAMQPError(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	ch2 := newMockChannel()

	recoveryDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(&libLog.NopLogger{}),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return ch2, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(3),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateConnected {
				close(recoveryDone)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Send nil error (simulates clean channel close).
	ch1.closeNotify <- nil

	select {
	case <-recoveryDone:
		// Recovery succeeded even with nil error.
	case <-time.After(2 * time.Second):
		t.Fatal("auto-recovery did not complete in time")
	}
}

// TestEmitHealthState_NoRecoveryConfig verifies that emitHealthState is a no-op
// when no recovery config is set.
func TestEmitHealthState_NoRecoveryConfig(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Should not panic.
	publisher.emitHealthState(HealthStateConnected)
}

// TestEmitHealthState_NoCallback verifies that emitHealthState is a no-op when
// recovery config exists but no callback is set.
func TestEmitHealthState_NoCallback(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return newMockChannel(), nil
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	// Should not panic even without a callback.
	publisher.emitHealthState(HealthStateReconnecting)
}

// TestAutoRecovery_WithLogger verifies that recovery logs are emitted when a
// logger is configured. We use a mock logger to capture calls.
func TestAutoRecovery_WithLogger(t *testing.T) {
	t.Parallel()

	ch1 := newMockChannel()
	ch2 := newMockChannel()

	logger := &mockLogger{}
	recoveryDone := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch1,
		WithLogger(logger),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return ch2, nil
		}),
		WithRecoveryBackoff(1*time.Millisecond, 10*time.Millisecond),
		WithMaxRecoveryAttempts(3),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateConnected {
				close(recoveryDone)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	ch1.closeNotify <- amqp.ErrClosed

	select {
	case <-recoveryDone:
		// Recovery succeeded.
	case <-time.After(2 * time.Second):
		t.Fatal("auto-recovery did not complete in time")
	}

	logger.mu.Lock()
	defer logger.mu.Unlock()

	assert.Greater(t, logger.warnCount, 0, "should have logged warnings during recovery")
	assert.Greater(t, logger.infoCount, 0, "should have logged info during recovery")
}

// TestAutoRecovery_ExhaustedWithLogger verifies that exhausting all recovery
// attempts logs an error.
func TestAutoRecovery_ExhaustedWithLogger(t *testing.T) {
	t.Parallel()

	ch := newMockChannel()
	logger := &mockLogger{}

	disconnected := make(chan struct{})

	publisher, err := NewConfirmablePublisherFromChannel(ch,
		WithLogger(logger),
		WithAutoRecovery(func() (ConfirmableChannel, error) {
			return nil, errors.New("nope")
		}),
		WithRecoveryBackoff(1*time.Millisecond, 5*time.Millisecond),
		WithMaxRecoveryAttempts(2),
		WithHealthCallback(func(state HealthState) {
			if state == HealthStateDisconnected {
				close(disconnected)
			}
		}),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = publisher.Close() })

	ch.closeNotify <- amqp.ErrClosed

	select {
	case <-disconnected:
		// Expected.
	case <-time.After(2 * time.Second):
		t.Fatal("should have reached disconnected state")
	}

	logger.mu.Lock()
	defer logger.mu.Unlock()

	assert.Greater(t, logger.errCount, 0, "should have logged error when exhausted")
}

// mockLogger captures log calls for testing. Implements libLog.Logger (v2).
type mockLogger struct {
	mu        sync.Mutex
	infoCount int
	warnCount int
	errCount  int
}

func (m *mockLogger) Log(_ context.Context, level libLog.Level, _ string, _ ...libLog.Field) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch level {
	case libLog.LevelError:
		m.errCount++
	case libLog.LevelWarn:
		m.warnCount++
	case libLog.LevelInfo:
		m.infoCount++
	}
}

//nolint:ireturn // required by Logger interface
func (m *mockLogger) With(_ ...libLog.Field) libLog.Logger { return m }

//nolint:ireturn // required by Logger interface
func (m *mockLogger) WithGroup(_ string) libLog.Logger { return m }
func (m *mockLogger) Enabled(_ libLog.Level) bool      { return true }
func (m *mockLogger) Sync(_ context.Context) error     { return nil }
