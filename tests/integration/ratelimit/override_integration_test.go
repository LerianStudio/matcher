//go:build integration

package ratelimit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegration_Ratelimit_OverrideRateLimitsForTests_NilService exercises the nil-service guard so
// the helper compiles and is wired up. It does not require infrastructure.
func TestIntegration_Ratelimit_OverrideRateLimitsForTests_NilService(t *testing.T) {
	require.NoError(t, OverrideRateLimitsForTests(context.Background(), nil))
}
