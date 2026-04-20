//go:build integration

package ratelimit

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOverrideRateLimitsForTests_NilService exercises the nil-service guard so
// the helper compiles and is wired up. It does not require infrastructure.
func TestOverrideRateLimitsForTests_NilService(t *testing.T) {
	require.NoError(t, OverrideRateLimitsForTests(context.Background(), nil))
}
