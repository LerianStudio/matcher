//go:build integration

package ratelimit

import (
	"context"

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
// Delegates the Set loop to applyRateLimitOverrides in override.go so the
// override policy (namespace, key list, values, actor) is reachable from a
// unit test without the integration build tag.
func OverrideRateLimitsForTests(ctx context.Context, svc *bootstrap.Service) error {
	if svc == nil {
		return nil
	}

	return applyRateLimitOverrides(ctx, svc.GetSystemplaneClient())
}
