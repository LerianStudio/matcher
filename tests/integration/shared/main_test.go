//go:build integration

package shared

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
		println("FATAL: failed to initialize shared infrastructure:", err.Error())
		os.Exit(1)
	}

	code := m.Run()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cleanupCancel()

	if err := integration.CleanupSharedInfra(cleanupCtx); err != nil {
		println("WARNING: failed to cleanup shared infrastructure:", err.Error())
	}

	os.Exit(code)
}
