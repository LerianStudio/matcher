//go:build integration

package integration

import (
	"testing"
)

// Compile-time checks to verify integration harness types exist.
var (
	_ = (*SharedInfra)(nil)
	_ = (*SharedTestHarness)(nil)
	_ = (*TestHarness)(nil)
	_ = (*SeedData)(nil)
)

func TestIntegration_Flow_SharedHarness_TypesExist(t *testing.T) {
	t.Parallel()

	t.Run("SharedInfra has expected fields", func(t *testing.T) {
		t.Parallel()
		var infra SharedInfra
		_ = infra.PostgresContainer
		_ = infra.RedisContainer
		_ = infra.RabbitMQContainer
		_ = infra.PostgresDSN
		_ = infra.RedisAddr
		_ = infra.RabbitMQHost
		_ = infra.RabbitMQPort
		_ = infra.RabbitMQHealthURL
	})

	t.Run("SharedTestHarness embeds SharedInfra", func(t *testing.T) {
		t.Parallel()
		var harness SharedTestHarness
		_ = harness.SharedInfra
		_ = harness.Connection
		_ = harness.Seed
	})

	t.Run("SeedData has expected fields", func(t *testing.T) {
		t.Parallel()
		var seed SeedData
		_ = seed.TenantID
		_ = seed.ContextID
		_ = seed.SourceID
	})
}
