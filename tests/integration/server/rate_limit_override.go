//go:build integration

package server

import (
	"context"
	"fmt"

	"github.com/LerianStudio/matcher/internal/bootstrap"
)

// overrideRateLimitsForTests writes test-friendly rate limit values to the
// systemplane client so that sequential integration tests from the same
// client IP do not hit 429 Too Many Requests responses.
//
// Why this is needed: `RegisterMatcherKeys` registers rate_limit.* keys with
// compile-time defaults (rate_limit.max=100 per minute). The systemplane
// `client.Get` returns these registered defaults with ok=true even when no
// runtime override exists, which masks env-var-based overrides like
// `RATE_LIMIT_MAX=10000` that the harness sets. Without this override, the
// first ~100 requests from any integration test run consume the global
// budget, and subsequent tests fail with 429.
//
// The systemplane namespace and key names must match those registered in
// `internal/bootstrap/systemplane_keys.go`.
func overrideRateLimitsForTests(ctx context.Context, svc *bootstrap.Service) error {
	if svc == nil {
		return nil
	}

	client := svc.GetSystemplaneClient()
	if client == nil {
		// Graceful degradation: systemplane failed to initialize, static config
		// from env vars is in effect, no override needed.
		return nil
	}

	// Values chosen to be effectively unlimited for test workloads while
	// preserving the rate limiter's code path (so tests still exercise the
	// middleware). Using 0 or disabled would skip the middleware entirely.
	overrides := []struct {
		key   string
		value any
	}{
		{"rate_limit.max", 100000},
		{"rate_limit.expiry_sec", 60},
		{"rate_limit.export_max", 10000},
		{"rate_limit.export_expiry_sec", 60},
		{"rate_limit.dispatch_max", 10000},
		{"rate_limit.dispatch_expiry_sec", 60},
	}

	for _, override := range overrides {
		if err := client.Set(ctx, "matcher", override.key, override.value, "integration-test-harness"); err != nil {
			return fmt.Errorf("systemplane set %s: %w", override.key, err)
		}
	}

	return nil
}
