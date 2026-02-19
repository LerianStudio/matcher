//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestMain runs before all tests to verify the local stack is running.
func TestMain(m *testing.M) {
	cfg := LoadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.StackCheckTimeout)
	defer cancel()

	checker := NewStackChecker(cfg)
	results, err := checker.CheckAll(ctx)

	fmt.Println(FormatResults(results))

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

	client, clientErr := NewClient(cfg)
	if clientErr != nil {
		fmt.Printf("Failed to create API client: %v\n", clientErr)
		os.Exit(1)
	}

	SetGlobals(cfg, client)

	fmt.Printf("\n✓ Stack is healthy. Running e2e tests...\n\n")
	os.Exit(m.Run())
}
