//go:build unit

package rabbitmq

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeChannel struct {
	exchangeDeclareCount int
	queueDeclareCount    int
	queueBindCount       int

	lastExchangeName string
	lastExchangeType string
	lastQueueName    string
	lastBindQueue    string
	lastBindKey      string
	lastBindExchange string
}

func (f *fakeChannel) ExchangeDeclare(name, kind string, _, _, _, _ bool, _ amqp.Table) error {
	f.exchangeDeclareCount++
	f.lastExchangeName = name
	f.lastExchangeType = kind

	return nil
}

func (f *fakeChannel) QueueDeclare(name string, _, _, _, _ bool, _ amqp.Table) (amqp.Queue, error) {
	f.queueDeclareCount++
	f.lastQueueName = name

	return amqp.Queue{Name: name}, nil
}

func (f *fakeChannel) QueueBind(name, key, exchange string, _ bool, _ amqp.Table) error {
	f.queueBindCount++
	f.lastBindQueue = name
	f.lastBindKey = key
	f.lastBindExchange = exchange

	return nil
}

func TestDeclareDLQTopology_Success(t *testing.T) {
	t.Parallel()

	ch := &fakeChannel{}

	err := DeclareDLQTopology(ch)

	require.NoError(t, err)
	assert.Equal(t, 1, ch.exchangeDeclareCount)
	assert.Equal(t, 1, ch.queueDeclareCount)
	assert.Equal(t, 1, ch.queueBindCount)

	assert.Equal(t, DLXExchangeName, ch.lastExchangeName)
	assert.Equal(t, ExchangeType, ch.lastExchangeType)
	assert.Equal(t, DLQName, ch.lastQueueName)
	assert.Equal(t, DLQName, ch.lastBindQueue)
	assert.Equal(t, "#", ch.lastBindKey)
	assert.Equal(t, DLXExchangeName, ch.lastBindExchange)
}

func TestDeclareDLQTopology_NilChannel(t *testing.T) {
	t.Parallel()

	err := DeclareDLQTopology(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrChannelRequired)
}

var errExchangeFailed = errors.New("exchange declare failed")

type fakeChannelExchangeError struct {
	fakeChannel
}

func (f *fakeChannelExchangeError) ExchangeDeclare(
	_, _ string,
	_, _, _, _ bool,
	_ amqp.Table,
) error {
	return errExchangeFailed
}

func TestDeclareDLQTopology_ExchangeDeclareError(t *testing.T) {
	t.Parallel()

	ch := &fakeChannelExchangeError{}

	err := DeclareDLQTopology(ch)

	require.Error(t, err)
	require.ErrorIs(t, err, errExchangeFailed)
	assert.Contains(t, err.Error(), "declare dlx exchange")
}

var errQueueFailed = errors.New("queue declare failed")

type fakeChannelQueueError struct {
	fakeChannel
}

func (f *fakeChannelQueueError) QueueDeclare(
	_ string,
	_, _, _, _ bool,
	_ amqp.Table,
) (amqp.Queue, error) {
	return amqp.Queue{}, errQueueFailed
}

func TestDeclareDLQTopology_QueueDeclareError(t *testing.T) {
	t.Parallel()

	ch := &fakeChannelQueueError{}

	err := DeclareDLQTopology(ch)

	require.Error(t, err)
	require.ErrorIs(t, err, errQueueFailed)
	assert.Contains(t, err.Error(), "declare dlq queue")
}

var errBindFailed = errors.New("queue bind failed")

type fakeChannelBindError struct {
	fakeChannel
}

func (f *fakeChannelBindError) QueueBind(_, _, _ string, _ bool, _ amqp.Table) error {
	return errBindFailed
}

func TestDeclareDLQTopology_QueueBindError(t *testing.T) {
	t.Parallel()

	ch := &fakeChannelBindError{}

	err := DeclareDLQTopology(ch)

	require.Error(t, err)
	require.ErrorIs(t, err, errBindFailed)
	assert.Contains(t, err.Error(), "bind dlq to dlx")
}

func TestGetDLXArgs(t *testing.T) {
	t.Parallel()

	args := GetDLXArgs()

	require.NotNil(t, args)
	assert.Equal(t, DLXExchangeName, args["x-dead-letter-exchange"])
}
