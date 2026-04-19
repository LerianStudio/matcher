//go:build unit

package ratelimit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedSet captures a single Set call so drift between the override list
// and systemplane_keys.go is visible in assertion output.
type recordedSet struct {
	ns    string
	key   string
	value any
	actor string
}

// recordingSetter is a minimal systemplaneSetter fake that records every
// Set call in order. Production uses *systemplane.Client which writes
// through to Postgres; the unit test only needs the Set contract.
type recordingSetter struct {
	calls []recordedSet
	// setErr, when set, is returned from the indexed Set call (0-based)
	// so tests can exercise the early-return error path without wiring
	// the full client.
	setErr      error
	setErrIndex int
}

func (r *recordingSetter) Set(_ context.Context, namespace, key string, value any, actor string) error {
	r.calls = append(r.calls, recordedSet{ns: namespace, key: key, value: value, actor: actor})

	if r.setErr != nil && len(r.calls)-1 == r.setErrIndex {
		return r.setErr
	}

	return nil
}

// TestApplyRateLimitOverrides_NilClient asserts the graceful-degradation
// path: when systemplane is not initialized the override is a silent no-op
// rather than an error that would fail every integration-test startup.
func TestApplyRateLimitOverrides_NilClient(t *testing.T) {
	t.Parallel()

	require.NoError(t, applyRateLimitOverrides(context.Background(), nil))
}

// TestApplyRateLimitOverrides_WritesAllKeys asserts every canonical key
// reaches the systemplane client with the expected namespace, value, and
// actor. This is the drift guard: if a key is dropped from
// rateLimitOverrides, or if systemplane_keys.go renames one of these keys,
// a paired test (below) or a production boot will fail — and this test
// pins the intended override surface.
func TestApplyRateLimitOverrides_WritesAllKeys(t *testing.T) {
	t.Parallel()

	setter := &recordingSetter{}

	require.NoError(t, applyRateLimitOverrides(context.Background(), setter))
	require.Len(t, setter.calls, len(rateLimitOverrides),
		"every override entry must produce exactly one Set call")

	// Assert order-for-order matches the canonical list, so the table is
	// the single source of truth for test-time rate-limit relaxations.
	for i, expected := range rateLimitOverrides {
		assert.Equal(t, rateLimitOverrideNamespace, setter.calls[i].ns,
			"call %d must target the matcher namespace", i)
		assert.Equal(t, expected.key, setter.calls[i].key,
			"call %d key mismatch", i)
		assert.Equal(t, expected.value, setter.calls[i].value,
			"call %d value mismatch for key %q", i, expected.key)
		assert.Equal(t, rateLimitOverrideActor, setter.calls[i].actor,
			"call %d actor must be the test-harness constant", i)
	}
}

// TestApplyRateLimitOverrides_CanonicalKeySet pins the exact set of keys
// that the override helper writes. This is intentionally a hard-coded
// duplicate of rateLimitOverrides — if a reviewer adds or removes an
// entry there, this test is the second place it must be changed, keeping
// the override surface loud on diff.
func TestApplyRateLimitOverrides_CanonicalKeySet(t *testing.T) {
	t.Parallel()

	expected := []string{
		"rate_limit.max",
		"rate_limit.expiry_sec",
		"rate_limit.export_max",
		"rate_limit.export_expiry_sec",
		"rate_limit.dispatch_max",
		"rate_limit.dispatch_expiry_sec",
	}

	got := make([]string, 0, len(rateLimitOverrides))
	for _, entry := range rateLimitOverrides {
		got = append(got, entry.key)
	}

	assert.Equal(t, expected, got,
		"rate-limit override key list changed; update both sites intentionally")
}

// TestApplyRateLimitOverrides_PropagatesSetError asserts a failure from
// systemplane.Set short-circuits the loop with a wrapped error so the
// bootstrap failure is diagnosable (e.g. a typo'd key name rejected by
// Register).
func TestApplyRateLimitOverrides_PropagatesSetError(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("boom")
	setter := &recordingSetter{
		setErr:      sentinel,
		setErrIndex: 2, // fail on the third entry (rate_limit.export_max)
	}

	err := applyRateLimitOverrides(context.Background(), setter)

	require.Error(t, err)
	assert.ErrorIs(t, err, sentinel)
	assert.Contains(t, err.Error(), "rate_limit.export_max",
		"error must identify which key failed for diagnosability")
	// Subsequent keys must not be attempted after the first error.
	assert.Len(t, setter.calls, 3,
		"loop must short-circuit on the first failing Set")
}
