//go:build chaos

package chaos

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 7*time.Minute)
	defer cancel()

	_, err := InitSharedChaos(ctx)
	if err != nil {
		log.Fatalf("failed to initialize chaos infrastructure: %v", err)
	}

	code := m.Run()

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cleanupCancel()

	if err := CleanupSharedChaos(cleanupCtx); err != nil {
		log.Printf("WARNING: failed to cleanup chaos infrastructure: %v", err)
	}

	os.Exit(code)
}
