//go:build integration

package exception

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	_, err := integration.InitSharedInfra(ctx)
	if err != nil {
		cancel()
		println("FATAL: failed to initialize shared infrastructure:", err.Error())
		os.Exit(1)
	}

	cancel()

	code := m.Run()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	if err := integration.CleanupSharedInfra(cleanupCtx); err != nil {
		println("WARNING: failed to cleanup shared infrastructure:", err.Error())
	}

	cleanupCancel()

	os.Exit(code)
}

func TestIntegration_Exception_HarnessSetup(t *testing.T) {
	// Validates that the integration test harness initializes correctly.
	// Additional integration tests will be added as features are implemented.
	infra := integration.GetSharedInfra()
	if infra == nil {
		t.Fatal("shared infrastructure was not initialized by TestMain")
	}
}
