// Package ratelimit hosts integration-test helpers that need to touch the
// bootstrap Service directly. It lives in its own subpackage so that
// tests/integration can be imported by test files inside internal/* without
// dragging bootstrap (and therefore the full module graph) with it, which
// would otherwise create an import cycle through packages like
// internal/discovery/services/worker.
package ratelimit

import (
	"context"
	"fmt"
)

// systemplaneSetter abstracts the single method of the systemplane client
// that this package uses. Production wires *systemplane.Client directly;
// unit tests supply a recording fake without depending on the full client.
type systemplaneSetter interface {
	Set(ctx context.Context, namespace, key string, value any, actor string) error
}

// rateLimitOverrideNamespace is the systemplane namespace the override
// writes into. Matches the constant in internal/bootstrap/systemplane_keys.go.
const rateLimitOverrideNamespace = "matcher"

// rateLimitOverrideActor is the audit actor recorded on every override
// write so drift between bootstrap-registered keys and test-time Sets is
// easy to trace in systemplane_history.
const rateLimitOverrideActor = "integration-test-harness"

// rateLimitOverrideEntry is a single (key, value) pair written to
// systemplane by applyRateLimitOverrides.
type rateLimitOverrideEntry struct {
	key   string
	value any
}

// rateLimitOverrides is the canonical list of (key, value) pairs written
// by OverrideRateLimitsForTests. Exposing it as a package var gives the
// unit test drift-guard teeth: if a key here disappears from
// internal/bootstrap/systemplane_keys.go the test notices.
var rateLimitOverrides = []rateLimitOverrideEntry{
	{"rate_limit.max", 100000},
	{"rate_limit.expiry_sec", 60},
	{"rate_limit.export_max", 10000},
	{"rate_limit.export_expiry_sec", 60},
	{"rate_limit.dispatch_max", 10000},
	{"rate_limit.dispatch_expiry_sec", 60},
}

// applyRateLimitOverrides writes the canonical override values to the given
// systemplane client. Extracted from OverrideRateLimitsForTests so it is
// reachable from a unit test without a full bootstrap.Service and the
// integration build tag it transitively requires.
func applyRateLimitOverrides(ctx context.Context, client systemplaneSetter) error {
	if client == nil {
		// Graceful degradation: systemplane failed to initialize, static config
		// from env vars is in effect, no override needed.
		return nil
	}

	for _, override := range rateLimitOverrides {
		if err := client.Set(
			ctx,
			rateLimitOverrideNamespace,
			override.key,
			override.value,
			rateLimitOverrideActor,
		); err != nil {
			return fmt.Errorf("systemplane set %s: %w", override.key, err)
		}
	}

	return nil
}
