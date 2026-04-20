//go:build unit

package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"

	"github.com/LerianStudio/matcher/internal/auth"
	matchingEntities "github.com/LerianStudio/matcher/internal/matching/domain/entities"
	sharedRabbitmq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
)

type fakeChannel struct {
	exchangeDeclared bool
	published        bool
	publishExchange  string
	publishKey       string
	publishMsg       amqp.Publishing
}

func (f *fakeChannel) ExchangeDeclare(_, _ string, _, _, _, _ bool, _ amqp.Table) error {
	f.exchangeDeclared = true
	return nil
}

func (f *fakeChannel) PublishWithContext(
	_ context.Context,
	exchange, key string,
	_, _ bool,
	msg amqp.Publishing,
) error {
	f.published = true
	f.publishExchange = exchange
	f.publishKey = key
	f.publishMsg = msg

	return nil
}

// mockConfirmableChannel implements sharedRabbitmq.ConfirmableChannel for testing.
type mockConfirmableChannel struct {
	mu              sync.Mutex
	confirms        chan amqp.Confirmation
	closeNotify     chan *amqp.Error
	published       bool
	publishExchange string
	publishKey      string
	publishMsg      amqp.Publishing
	publishErr      error
}

func newMockConfirmableChannel() *mockConfirmableChannel {
	return &mockConfirmableChannel{
		closeNotify: make(chan *amqp.Error, 1),
	}
}

func (m *mockConfirmableChannel) Confirm(_ bool) error {
	return nil
}

func (m *mockConfirmableChannel) NotifyPublish(confirm chan amqp.Confirmation) chan amqp.Confirmation {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.confirms = confirm

	return confirm
}

func (m *mockConfirmableChannel) NotifyClose(_ chan *amqp.Error) chan *amqp.Error {
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

	m.published = true
	m.publishExchange = exchange
	m.publishKey = key
	m.publishMsg = msg

	if m.publishErr != nil {
		return m.publishErr
	}

	// Send confirmation immediately
	if m.confirms != nil {
		go func() {
			m.confirms <- amqp.Confirmation{DeliveryTag: 1, Ack: true}
		}()
	}

	return nil
}

func (m *mockConfirmableChannel) Close() error {
	return nil
}

func createTestConfirmablePublisher(t *testing.T) (*sharedRabbitmq.ConfirmablePublisher, *mockConfirmableChannel) {
	t.Helper()

	ch := newMockConfirmableChannel()
	pub, err := sharedRabbitmq.NewConfirmablePublisherFromChannel(ch)
	require.NoError(t, err)

	return pub, ch
}

func TestNewEventPublisherFromChannel_NilChannel(t *testing.T) {
	t.Parallel()

	publisher, err := NewEventPublisherFromChannel(nil)

	require.Nil(t, publisher)
	require.ErrorIs(t, err, errRabbitMQChannelRequired)
}

func TestEventPublisher_Close_NilSafe(t *testing.T) {
	t.Parallel()

	var nilPublisher *EventPublisher
	require.NoError(t, nilPublisher.Close())

	publisher := &EventPublisher{}
	require.NoError(t, publisher.Close())
}

func TestEventPublisher_Close_ClosesConfirmablePublisher(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, _ := createTestConfirmablePublisher(t)
	publisher, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, nil, confirmPub)
	require.NoError(t, err)

	require.NoError(t, publisher.Close())

	event := &matchingEntities.MatchConfirmedEvent{MatchID: uuid.New()}
	err = publisher.PublishMatchConfirmed(context.Background(), event)
	require.ErrorIs(t, err, sharedRabbitmq.ErrPublisherClosed)
}

func TestEventPublisher_PublishMatchConfirmed_NilEvent(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, _ := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, nil, confirmPub)
	require.NoError(t, err)

	err = pub.PublishMatchConfirmed(context.Background(), nil)
	require.ErrorIs(t, err, errEventRequired)
}

func TestEventPublisher_PublishMatchConfirmed_NilIdempotencyKey(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, _ := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, nil, confirmPub)
	require.NoError(t, err)

	event := &matchingEntities.MatchConfirmedEvent{
		MatchID: uuid.Nil,
	}

	err = pub.PublishMatchConfirmed(context.Background(), event)
	require.ErrorIs(t, err, errIdempotencyKeyRequired)
}

func TestEventPublisher_PublishMatchConfirmed_SetsIdempotencyHeadersAndTrace(t *testing.T) {
	t.Parallel()

	originalPropagator := otel.GetTextMapPropagator()

	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTextMapPropagator(originalPropagator)
	})

	ch := &fakeChannel{}
	confirmPub, mockCh := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(
		&libRabbitmq.RabbitMQConnection{},
		ch,
		otel.GetTextMapPropagator(),
		confirmPub,
	)
	require.NoError(t, err)
	require.True(t, ch.exchangeDeclared)

	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	event := &matchingEntities.MatchConfirmedEvent{
		EventType:      matchingEntities.EventTypeMatchConfirmed,
		TenantID:       uuid.New(),
		TenantSlug:     "default",
		ContextID:      uuid.New(),
		RunID:          uuid.New(),
		MatchID:        uuid.New(),
		RuleID:         uuid.New(),
		Confidence:     90,
		ConfirmedAt:    now,
		Timestamp:      now,
		TransactionIDs: []uuid.UUID{uuid.New()},
	}

	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	spanID := trace.SpanID{9, 9, 9, 9, 9, 9, 9, 9}
	sc := trace.NewSpanContext(
		trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled},
	)
	ctx := trace.ContextWithSpanContext(context.Background(), sc)
	ctx = context.WithValue(ctx, auth.TenantIDKey, "tenant-a")

	require.NoError(t, pub.PublishMatchConfirmed(ctx, event))
	require.True(t, mockCh.published)

	require.Equal(t, "matcher.events", mockCh.publishExchange)
	require.Equal(t, routingKeyMatchConfirmed, mockCh.publishKey)

	require.Equal(t, event.MatchID.String(), mockCh.publishMsg.MessageId)
	idempotencyKey, ok := mockCh.publishMsg.Headers["idempotency_key"].(string)
	require.True(t, ok, "idempotency_key header should be a string")
	require.Equal(t, event.MatchID.String(), idempotencyKey)
	require.Equal(t, "tenant-a", mockCh.publishMsg.Headers["X-Tenant-ID"])

	rawTraceparent, ok := mockCh.publishMsg.Headers["traceparent"].(string)
	require.True(t, ok)
	require.NotEmpty(t, rawTraceparent)
}

func TestEventPublisher_PublishMatchConfirmed_DoesNotSetTenantHeaderWithoutExplicitTenant(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, mockCh := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, otel.GetTextMapPropagator(), confirmPub)
	require.NoError(t, err)

	event := &matchingEntities.MatchConfirmedEvent{MatchID: uuid.New()}

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled}))
	require.NoError(t, pub.PublishMatchConfirmed(ctx, event))
	_, exists := mockCh.publishMsg.Headers["X-Tenant-ID"]
	assert.False(t, exists)
}

func TestEventPublisher_PublishMatchUnmatched_SetsTenantHeaderWhenExplicitTenantPresent(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, mockCh := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, otel.GetTextMapPropagator(), confirmPub)
	require.NoError(t, err)

	event := &matchingEntities.MatchUnmatchedEvent{RunID: uuid.New(), ContextID: uuid.New(), MatchID: uuid.New(), Timestamp: time.Now().UTC()}
	ctx := context.WithValue(context.Background(), auth.TenantIDKey, "tenant-a")

	require.NoError(t, pub.PublishMatchUnmatched(ctx, event))
	require.Equal(t, "tenant-a", mockCh.publishMsg.Headers["X-Tenant-ID"])
}

func TestEventPublisher_PublishMatchUnmatched_DoesNotSetTenantHeaderWithoutExplicitTenant(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}
	confirmPub, mockCh := createTestConfirmablePublisher(t)
	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, otel.GetTextMapPropagator(), confirmPub)
	require.NoError(t, err)

	event := &matchingEntities.MatchUnmatchedEvent{RunID: uuid.New(), ContextID: uuid.New(), MatchID: uuid.New(), Timestamp: time.Now().UTC()}

	require.NoError(t, pub.PublishMatchUnmatched(context.Background(), event))
	_, exists := mockCh.publishMsg.Headers["X-Tenant-ID"]
	assert.False(t, exists)
}

type fakeChannelWithPublishError struct {
	fakeChannel
	publishErr error
}

func (f *fakeChannelWithPublishError) PublishWithContext(
	_ context.Context,
	_, _ string,
	_, _ bool,
	_ amqp.Publishing,
) error {
	return f.publishErr
}

var errConnectionClosed = errors.New("connection closed")

type fakeChannelWithExchangeDeclareError struct {
	exchangeDeclareErr error
}

func (f *fakeChannelWithExchangeDeclareError) ExchangeDeclare(
	_, _ string,
	_, _, _, _ bool,
	_ amqp.Table,
) error {
	return f.exchangeDeclareErr
}

func (f *fakeChannelWithExchangeDeclareError) PublishWithContext(
	_ context.Context,
	_, _ string,
	_, _ bool,
	_ amqp.Publishing,
) error {
	return nil
}

var errExchangeDeclareFailed = errors.New("exchange declare failed")

func TestNewEventPublisher_ExchangeDeclareFailure(t *testing.T) {
	t.Parallel()

	ch := &fakeChannelWithExchangeDeclareError{exchangeDeclareErr: errExchangeDeclareFailed}
	confirmPub, _ := createTestConfirmablePublisher(t)
	_, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, nil, confirmPub)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to declare exchange")
}

func TestEventPublisher_PublishMatchConfirmed_PublishFailure(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}

	// Create a confirmable publisher with a channel that returns an error on publish
	mockCh := newMockConfirmableChannel()
	mockCh.publishErr = errConnectionClosed
	confirmPub, err := sharedRabbitmq.NewConfirmablePublisherFromChannel(mockCh)
	require.NoError(t, err)

	pub, err := newEventPublisher(&libRabbitmq.RabbitMQConnection{}, ch, nil, confirmPub)
	require.NoError(t, err)

	event := &matchingEntities.MatchConfirmedEvent{
		EventType:      matchingEntities.EventTypeMatchConfirmed,
		TenantID:       uuid.New(),
		MatchID:        uuid.New(),
		TransactionIDs: []uuid.UUID{uuid.New()},
	}

	err = pub.PublishMatchConfirmed(context.Background(), event)
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to publish event")
}

func TestEventPublisher_PublishMatchConfirmed_NilPublisher(t *testing.T) {
	t.Parallel()

	var pub *EventPublisher

	err := pub.PublishMatchConfirmed(context.Background(), &matchingEntities.MatchConfirmedEvent{})
	require.ErrorIs(t, err, errPublisherNotInit)
}
