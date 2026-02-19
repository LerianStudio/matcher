//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/LerianStudio/matcher/tests/e2e"
)

// TestMain runs before all journey tests to verify the local stack is running.
func TestMain(m *testing.M) {
	cfg := e2e.LoadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.StackCheckTimeout)
	defer cancel()

	checker := e2e.NewStackChecker(cfg)
	results, err := checker.CheckAll(ctx)

	fmt.Println(e2e.FormatResults(results))

	if err != nil {
		fmt.Printf("\n❌ E2E tests require the local stack to be running.\n")
		fmt.Printf("   Start it with: docker-compose up -d\n")
		fmt.Printf("   Then wait for all services to be healthy.\n\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	// Flush rate limit keys before tests to ensure clean state
	if flushErr := checker.FlushRateLimitKeys(ctx); flushErr != nil {
		fmt.Printf("Warning: Failed to flush rate limit keys: %v\n", flushErr)
	} else {
		fmt.Printf("✓ Rate limit keys flushed\n")
	}

	if deleted, cleanErr := checker.CleanStaleOutboxEvents(ctx); cleanErr != nil {
		fmt.Printf("Warning: Failed to clean stale outbox events: %v\n", cleanErr)
	} else if deleted > 0 {
		fmt.Printf("✓ Cleaned %d stale outbox events\n", deleted)
	}

	if cleanErr := checker.CleanStaleAuditState(ctx); cleanErr != nil {
		fmt.Printf("Warning: Failed to clean stale audit state: %v\n", cleanErr)
	} else {
		fmt.Printf("✓ Cleaned stale audit state\n")
	}

	client, clientErr := e2e.NewClient(cfg)
	if clientErr != nil {
		fmt.Printf("Failed to create API client: %v\n", clientErr)
		os.Exit(1)
	}

	e2e.SetGlobals(cfg, client)

	fmt.Printf("\n✓ Stack is healthy. Running journey e2e tests...\n\n")
	os.Exit(m.Run())
}
