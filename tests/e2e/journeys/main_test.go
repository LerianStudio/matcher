//go:build e2e

package journeys

import (
	"context"
	"fmt"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/LerianStudio/matcher/tests/e2e"
	"github.com/LerianStudio/matcher/tests/e2e/mock"
)

// mockFetcherPort is the fixed port the mock Fetcher server listens on.
// This must match the FETCHER_URL default in docker-compose.yml
// (http://host.docker.internal:14006).
const mockFetcherPort = 14006

const requireFetcherMockEnv = "E2E_REQUIRE_FETCHER_MOCK"

// TestMain runs before all journey tests to verify the local stack is running.
func TestMain(m *testing.M) {
	requireFetcherMock := os.Getenv(requireFetcherMockEnv) != ""

	// --- Start mock Fetcher server ---
	// The mock starts before stack checks so it is already listening when the
	// Matcher app (in Docker) makes its first Fetcher health check. Discovery
	// tests manipulate mock state via getMockFetcher(); non-discovery tests are
	// unaffected because Fetcher is disabled by default (FETCHER_ENABLED=false).
	mockFetcher = mock.NewMockFetcherServer()
	mockFetcher.SetHealthy(true)

	fetcherURL, fetcherErr := mockFetcher.StartOnPort(mockFetcherPort)
	if fetcherErr != nil {
		fmt.Printf("Warning: Failed to start mock Fetcher on port %d: %v\n", mockFetcherPort, fetcherErr)
		if requireFetcherMock {
			fmt.Printf("   %s is set; failing instead of skipping discovery coverage.\n", requireFetcherMockEnv)
			os.Exit(1)
		}
		fmt.Printf("   Discovery E2E tests will be skipped.\n")

		_ = mockFetcher.Stop()
		mockFetcher = nil
	} else {
		fmt.Printf("✓ Mock Fetcher server listening at %s\n", fetcherURL)
	}

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

	var restoreFetcherConfig func() error

	// --- Configure systemplane for Discovery mock ---
	// The systemplane owns runtime config. Env vars are bootstrap-only and don't
	// propagate to the dynamic Fetcher client. We PATCH the systemplane to point
	// the Fetcher at our mock server so Discovery tests can exercise the full path.
	if mockFetcher != nil {
		restore, patchErr := patchSystemplaneFetcherConfig(cfg.AppBaseURL, mockFetcherPort)
		if patchErr != nil {
			fmt.Printf("Warning: Failed to configure systemplane for Fetcher mock: %v\n", patchErr)
			if requireFetcherMock {
				fmt.Printf("   %s is set; failing instead of continuing with partial discovery coverage.\n", requireFetcherMockEnv)
				os.Exit(1)
			}
			fmt.Printf("   Discovery E2E tests may fail.\n")

			_ = mockFetcher.Stop()
			mockFetcher = nil
		} else {
			restoreFetcherConfig = restore
			fmt.Printf("✓ Systemplane configured: fetcher → mock at port %d\n", mockFetcherPort)
		}
	}

	fmt.Printf("\n✓ Stack is healthy. Running journey e2e tests...\n\n")
	code := m.Run()

	if restoreFetcherConfig != nil {
		if restoreErr := restoreFetcherConfig(); restoreErr != nil {
			fmt.Printf("Warning: Failed to restore Fetcher systemplane config: %v\n", restoreErr)
			if code == 0 {
				code = 1
			}
		} else {
			fmt.Printf("✓ Restored Fetcher systemplane config\n")
		}
	}

	if mockFetcher != nil {
		if stopErr := mockFetcher.Stop(); stopErr != nil {
			fmt.Printf("Warning: Failed to stop mock Fetcher: %v\n", stopErr)
			if code == 0 {
				code = 1
			}
		}
	}

	os.Exit(code)
}
