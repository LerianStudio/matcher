//go:build integration

package server

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/integration"
)

// Compile-time checks to verify server harness types exist.
var (
	_ = (*ServerHarness)(nil)
)

func TestServerHarness_TypesExist(t *testing.T) {
	t.Parallel()

	t.Run("ServerHarness embeds TestHarness", func(t *testing.T) {
		t.Parallel()
		var harness ServerHarness
		_ = harness.TestHarness
		_ = harness.Service
	})

	t.Run("ServerHarness embeds serverHarnessBase", func(t *testing.T) {
		t.Parallel()
		var harness ServerHarness
		_ = harness.serverHarnessBase
	})
}

func TestServerHarness_Constants(t *testing.T) {
	t.Parallel()

	t.Run("Exchange and routing key constants are defined", func(t *testing.T) {
		t.Parallel()
		_ = ExchangeName
		_ = RoutingKeyIngestionCompleted
		_ = RoutingKeyIngestionFailed
		_ = RoutingKeyMatchConfirmed
	})
}

func TestServerHarness_CompatibilityWithTestHarness(t *testing.T) {
	t.Parallel()

	t.Run("TestHarness fields accessible through ServerHarness", func(t *testing.T) {
		t.Parallel()
		var harness ServerHarness
		harness.TestHarness = &integration.TestHarness{}
		_ = harness.TestHarness.PostgresDSN
		_ = harness.TestHarness.RedisAddr
		_ = harness.TestHarness.Connection
		_ = harness.TestHarness.Seed
	})
}
