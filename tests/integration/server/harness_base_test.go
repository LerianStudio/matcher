//go:build integration

package server

import (
	"testing"
)

// Compile-time checks to verify harness base types exist.
var (
	_ = serverHarnessBase{}
)

func TestIntegration_Server_HarnessBase_TypesExist(t *testing.T) {
	t.Parallel()

	t.Run("serverHarnessBase has Fiber App field", func(t *testing.T) {
		t.Parallel()
		var base serverHarnessBase
		_ = base.App
	})

	t.Run("serverHarnessBase has OutboxDispatcher field", func(t *testing.T) {
		t.Parallel()
		var base serverHarnessBase
		_ = base.OutboxDispatcher
	})

	t.Run("serverHarnessBase has test context field", func(t *testing.T) {
		t.Parallel()
		var base serverHarnessBase
		_ = base.t
	})

	t.Run("serverHarnessBase has infrastructure fields", func(t *testing.T) {
		t.Parallel()
		var base serverHarnessBase
		_ = base.PostgresDSN
		_ = base.RedisAddr
		_ = base.RabbitMQHost
		_ = base.RabbitMQPort
		_ = base.RabbitMQHealthURL
		_ = base.Seed
	})
}

func TestIntegration_Server_HarnessBase_Methods(t *testing.T) {
	t.Parallel()

	t.Run("Do method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.Do
	})

	t.Run("DoJSON method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.DoJSON
	})

	t.Run("DoMultipart method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.DoMultipart
	})

	t.Run("DispatchOutboxOnce method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.DispatchOutboxOnce
	})

	t.Run("DispatchOutboxUntilEmpty method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.DispatchOutboxUntilEmpty
	})

	t.Run("WaitForEvent method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.WaitForEvent
	})

	t.Run("WaitForEventWithTimeout method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.WaitForEventWithTimeout
	})

	t.Run("ServerCtx method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.ServerCtx
	})

	t.Run("RabbitMQConnection method signature exists", func(t *testing.T) {
		t.Parallel()
		var base *serverHarnessBase
		_ = base.RabbitMQConnection
	})
}
