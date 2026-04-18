//go:build integration

package integration

import (
	"context"
	"fmt"

	"github.com/LerianStudio/matcher/internal/bootstrap"
)

// OverrideRateLimitsForTests writes test-friendly rate limit values to the
// systemplane client so that sequential integration tests from the same
// client IP do not hit 429 Too Many Requests responses.
//
// Why this is needed: RegisterMatcherKeys registers rate_limit.* keys with
// compile-time defaults (rate_limit.max=100 per minute). The systemplane
// client.Get returns these registered defaults with ok=true even when no
// runtime override exists, which masks env-var-based overrides like
// RATE_LIMIT_MAX=1000 that tests set. Without this override, a single test
// run with more than ~100 requests will fail with 429.
//
// The systemplane namespace and key names must match those registered in
// internal/bootstrap/systemplane_keys.go. A matching helper exists in
// tests/integration/server/rate_limit_override.go for the server subtests.
func OverrideRateLimitsForTests(ctx context.Context, svc *bootstrap.Service) error {
	if svc == nil {
		return nil
	}

	client := svc.GetSystemplaneClient()
	if client == nil {
		// Graceful degradation: systemplane failed to initialize, static config
		// from env vars is in effect, no override needed.
		return nil
	}

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
