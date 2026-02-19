//go:build integration

package governance

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/LerianStudio/matcher/tests/integration"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel() // safety net in case of unexpected return

	_, err := integration.InitSharedInfra(ctx)
	if err != nil {
		cancel() // explicit: os.Exit does not run defers
		println("FATAL: failed to initialize shared infrastructure:", err.Error())
		os.Exit(1)
	}

	cancel()

	code := m.Run()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel() // safety net in case of unexpected return

	if err := integration.CleanupSharedInfra(cleanupCtx); err != nil {
		println("WARNING: failed to cleanup shared infrastructure:", err.Error())
	}

	cleanupCancel() // explicit: os.Exit does not run defers

	os.Exit(code)
}
