//go:build integration

package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestOverrideRateLimitsForTests_NilServiceNoOp confirms the helper handles a
// nil service gracefully. Integration tests that skip or fail early in setup
// may pass a nil service, so this guard must not panic or return an error.
//
// Full coverage of the happy path (real systemplane client, real overrides)
// comes from integration tests that invoke the helper during their harness
// bootstrap. This unit-style assertion exists to satisfy make check-tests and
// protect the nil-safety branch in the helper.
func TestOverrideRateLimitsForTests_NilServiceNoOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	err := overrideRateLimitsForTests(ctx, nil)
	assert.NoError(t, err, "nil service must be a no-op")
}
