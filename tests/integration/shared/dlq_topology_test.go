//go:build integration

package shared

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dlq "github.com/LerianStudio/matcher/internal/shared/adapters/rabbitmq"
	"github.com/LerianStudio/matcher/tests/integration"
)

// dialRabbitMQ opens an AMQP connection to the harness broker and returns
// the connection + channel. Both are registered for cleanup via t.Cleanup.
func dialRabbitMQ(t *testing.T, h *integration.TestHarness) (*amqp.Connection, *amqp.Channel) {
	t.Helper()

	uri := "amqp://guest:guest@" + h.RabbitMQHost + ":" + h.RabbitMQPort + "/"

	conn, err := amqp.Dial(uri)
	require.NoError(t, err, "amqp.Dial should connect to RabbitMQ")

	t.Cleanup(func() {
		_ = conn.Close()
	})

	ch, err := conn.Channel()
	require.NoError(t, err, "opening AMQP channel should succeed")

	t.Cleanup(func() {
		_ = ch.Close()
	})

	return conn, ch
}

// cleanupDLQTopology removes the DLQ queue and DLX exchange so each test
// starts with a clean slate.
func cleanupDLQTopology(t *testing.T, ch *amqp.Channel) {
	t.Helper()

	// Order matters: unbind implicitly by deleting queue first, then exchange.
	_, err := ch.QueueDelete(dlq.DLQName, false, false, false)
	if err != nil {
		t.Logf("cleanup: queue delete %s: %v", dlq.DLQName, err)
	}

	err = ch.ExchangeDelete(dlq.DLXExchangeName, false, false)
	if err != nil {
		t.Logf("cleanup: exchange delete %s: %v", dlq.DLXExchangeName, err)
	}
}

func TestIntegration_Shared_DeclareDLQTopology_Success(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		_, ch := dialRabbitMQ(t, h)

		// Pre-clean in case a previous test run left artifacts.
		cleanupDLQTopology(t, ch)

		// Re-open a fresh channel because delete operations can invalidate
		// passive-declare expectations on certain broker versions.
		_, ch = dialRabbitMQ(t, h)

		err := dlq.DeclareDLQTopology(ch)
		require.NoError(t, err, "DeclareDLQTopology should succeed")

		// Passive-declare verifies the exchange exists with the expected attributes.
		// ExchangeDeclarePassive returns an error if the exchange does not exist.
		err = ch.ExchangeDeclarePassive(
			dlq.DLXExchangeName,
			dlq.ExchangeType,
			true,  // durable
			false, // autoDelete
			false, // internal
			false, // noWait
			nil,
		)
		require.NoError(t, err, "DLX exchange should exist after topology declaration")

		// Passive-declare verifies the queue exists with the expected attributes.
		q, err := ch.QueueDeclarePassive(
			dlq.DLQName,
			true,  // durable
			false, // autoDelete
			false, // exclusive
			false, // noWait
			nil,
		)
		require.NoError(t, err, "DLQ queue should exist after topology declaration")
		assert.Equal(t, dlq.DLQName, q.Name, "queue name should match DLQName constant")
	})
}

func TestIntegration_Shared_DeclareDLQTopology_Idempotent(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		_, ch := dialRabbitMQ(t, h)
		cleanupDLQTopology(t, ch)
		_, ch = dialRabbitMQ(t, h)

		err := dlq.DeclareDLQTopology(ch)
		require.NoError(t, err, "first DeclareDLQTopology call should succeed")

		err = dlq.DeclareDLQTopology(ch)
		require.NoError(t, err, "second DeclareDLQTopology call should succeed (idempotent)")

		// Verify topology still valid after double-declaration.
		err = ch.ExchangeDeclarePassive(
			dlq.DLXExchangeName,
			dlq.ExchangeType,
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err, "DLX exchange should still exist after idempotent declaration")

		q, err := ch.QueueDeclarePassive(
			dlq.DLQName,
			true,
			false,
			false,
			false,
			nil,
		)
		require.NoError(t, err, "DLQ queue should still exist after idempotent declaration")
		assert.Equal(t, dlq.DLQName, q.Name)
	})
}

func TestIntegration_Shared_GetDLXArgs_ReturnsCorrectTable(t *testing.T) {
	t.Parallel()

	args := dlq.GetDLXArgs()

	require.NotNil(t, args, "GetDLXArgs should return a non-nil table")

	val, ok := args["x-dead-letter-exchange"]
	require.True(t, ok, "table should contain 'x-dead-letter-exchange' key")
	assert.Equal(t, dlq.DLXExchangeName, val,
		"x-dead-letter-exchange should reference the DLX exchange name constant")
}

func TestIntegration_Shared_DeclareDLQTopology_QueueIsDurable(t *testing.T) {
	integration.RunWithHarness(t, func(t *testing.T, h *integration.TestHarness) {
		_, ch := dialRabbitMQ(t, h)
		cleanupDLQTopology(t, ch)
		_, ch = dialRabbitMQ(t, h)

		err := dlq.DeclareDLQTopology(ch)
		require.NoError(t, err, "DeclareDLQTopology should succeed")

		// QueueDeclarePassive with durable=true succeeds only if the existing
		// queue was also declared as durable. A mismatch triggers a channel
		// error (PRECONDITION_FAILED), which surfaces as a non-nil error.
		q, err := ch.QueueDeclarePassive(
			dlq.DLQName,
			true,  // durable — must match the original declaration
			false, // autoDelete
			false, // exclusive
			false, // noWait
			nil,
		)
		require.NoError(t, err,
			"passive declare with durable=true should succeed, confirming queue durability")
		assert.Equal(t, dlq.DLQName, q.Name)
	})
}
