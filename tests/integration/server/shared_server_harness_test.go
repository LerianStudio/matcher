//go:build integration

package server

import (
	"testing"

	"github.com/LerianStudio/matcher/tests/integration"
)

// Compile-time checks to verify shared server harness types exist.
var (
	_ = (*SharedServerHarness)(nil)
)

func TestIntegration_Server_SharedServerHarness_TypesExist(t *testing.T) {
	t.Parallel()

	t.Run("SharedServerHarness embeds SharedTestHarness", func(t *testing.T) {
		t.Parallel()
		var harness SharedServerHarness
		_ = harness.SharedTestHarness
		_ = harness.Service
	})

	t.Run("SharedServerHarness embeds serverHarnessBase", func(t *testing.T) {
		t.Parallel()
		var harness SharedServerHarness
		_ = harness.serverHarnessBase
	})
}

func TestIntegration_Server_SharedServerHarness_CompatibilityMethods(t *testing.T) {
	t.Parallel()

	t.Run("ToLegacyServerHarness method exists", func(t *testing.T) {
		t.Parallel()
		var harness *SharedServerHarness
		var _ func() *ServerHarness = harness.ToLegacyServerHarness
	})

	t.Run("ToLegacyHarness from embedded SharedTestHarness exists", func(t *testing.T) {
		t.Parallel()
		var harness *integration.SharedTestHarness
		var _ func() *integration.TestHarness = harness.ToLegacyHarness
	})
}
