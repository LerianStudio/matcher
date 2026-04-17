// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"errors"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedMemoryLimitReader returns a canned (bytes, source, err) triple so
// tests can drive the enforcement branches deterministically.
func fixedMemoryLimitReader(bytes int64, source string, err error) memoryLimitReader {
	return func() (int64, string, error) {
		return bytes, source, err
	}
}

// TestEnsureBridgeMemoryBudget_FetcherDisabled_ReturnsNil asserts the
// guard is a no-op when Fetcher is not enabled. This is the main
// constraint: non-Fetcher paths must never feel this check.
func TestEnsureBridgeMemoryBudget_FetcherDisabled_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = false

	require.NoError(t, EnsureBridgeMemoryBudget(cfg))
}

// TestEnsureBridgeMemoryBudget_NilConfig_ReturnsNil covers the defensive
// guard: callers with a nil config get a silent no-op (mis-wiring
// elsewhere, not the memory-budget layer's problem to diagnose).
func TestEnsureBridgeMemoryBudget_NilConfig_ReturnsNil(t *testing.T) {
	t.Parallel()

	require.NoError(t, EnsureBridgeMemoryBudget(nil))
}

// TestEnforceMemoryBudget_NoCgroup_ReturnsNil covers the dev/macOS path:
// when the reader returns an error (e.g., cgroup files missing), the
// guard must NOT block boot. This is the "can't check, don't block"
// policy documented on the helper.
func TestEnforceMemoryBudget_NoCgroup_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(0, "", errors.New("no cgroup"))

	require.NoError(t, enforceMemoryBudget(cfg, reader))
}

// TestEnforceMemoryBudget_ReaderReportsNoLimit_ReturnsNil covers the
// case where cgroup says "max" / unlimited. Zero bytes with no error is
// the documented "no explicit limit" signal; we must not synthesize a
// violation out of nothing.
func TestEnforceMemoryBudget_ReaderReportsNoLimit_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(0, "/sys/fs/cgroup/memory.max", nil)

	require.NoError(t, enforceMemoryBudget(cfg, reader))
}

// TestEnforceMemoryBudget_BelowMinimum_ReturnsError verifies the HIGH
// finding is closed: 1 GiB cgroup with FETCHER_ENABLED=true must be a
// boot-time rejection, not a silent OOMKill later. The error must wrap
// ErrFetcherBridgeNotOperational so the existing error-handling chain
// surfaces it correctly.
func TestEnforceMemoryBudget_BelowMinimum_ReturnsError(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	const oneGiB = int64(1) << 30

	reader := fixedMemoryLimitReader(oneGiB, "/sys/fs/cgroup/memory.max", nil)

	err := enforceMemoryBudget(cfg, reader)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrFetcherBridgeNotOperational)
	assert.Contains(t, err.Error(), "pod memory limit")
	assert.Contains(t, err.Error(), "/sys/fs/cgroup/memory.max",
		"error must name the source file so operators know where the number came from")
}

// TestEnforceMemoryBudget_AtMinimum_ReturnsNil verifies the threshold
// boundary: 2 GiB is the minimum, not an exclusive floor. A pod sized
// exactly at the floor must boot.
func TestEnforceMemoryBudget_AtMinimum_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(bridgeMinMemoryBytes, "/sys/fs/cgroup/memory.max", nil)

	require.NoError(t, enforceMemoryBudget(cfg, reader))
}

// TestEnforceMemoryBudget_AboveMinimum_ReturnsNil verifies the happy
// path: a pod with more than the minimum budget boots normally.
func TestEnforceMemoryBudget_AboveMinimum_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	const fourGiB = int64(4) << 30

	reader := fixedMemoryLimitReader(fourGiB, "/sys/fs/cgroup/memory.max", nil)

	require.NoError(t, enforceMemoryBudget(cfg, reader))
}

// TestEnforceMemoryBudget_FetcherDisabled_IgnoresTinyBudget confirms the
// enforcement is scoped: even a 128 MiB cgroup must not trigger the
// guard when Fetcher is not enabled. This is the isolation test for
// non-Fetcher workloads.
func TestEnforceMemoryBudget_FetcherDisabled_IgnoresTinyBudget(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = false

	reader := fixedMemoryLimitReader(128<<20, "/sys/fs/cgroup/memory.max", nil)

	require.NoError(t, enforceMemoryBudget(cfg, reader))
}

// TestEnforceMemoryBudget_NilReader_ReturnsNil guards against
// mis-wiring: the production call always passes a real reader, but a
// nil reader must not panic.
func TestEnforceMemoryBudget_NilReader_ReturnsNil(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	require.NoError(t, enforceMemoryBudget(cfg, nil))
}

// TestApplyGOMEMLIMIT_FetcherDisabled_NoOp asserts the companion setter
// stays completely quiet when Fetcher is off — no GOMEMLIMIT mutation,
// no log spam for unrelated deployments.
func TestApplyGOMEMLIMIT_FetcherDisabled_NoOp(t *testing.T) {
	t.Parallel()

	cfg := &Config{}
	cfg.Fetcher.Enabled = false

	reader := fixedMemoryLimitReader(4<<30, "/sys/fs/cgroup/memory.max", nil)

	got := applyGOMEMLIMIT(t.Context(), cfg, nil, reader)
	assert.Equal(t, int64(0), got, "must not set GOMEMLIMIT when Fetcher disabled")
}

// TestApplyGOMEMLIMIT_OperatorOverride_Respected asserts the operator's
// explicit GOMEMLIMIT is never overwritten. This is the "respect the
// human" rule: if they set it, they own the value.
func TestApplyGOMEMLIMIT_OperatorOverride_Respected(t *testing.T) {
	// Cannot use t.Parallel() because we mutate a process-wide env var.
	t.Setenv("GOMEMLIMIT", "1GiB")

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(4<<30, "/sys/fs/cgroup/memory.max", nil)

	got := applyGOMEMLIMIT(t.Context(), cfg, nil, reader)
	assert.Equal(t, int64(0), got, "must not overwrite an operator-set GOMEMLIMIT")
}

// TestApplyGOMEMLIMIT_ReaderError_NoOp covers the dev/macOS path: no
// cgroup, nothing to compute against, nothing set.
func TestApplyGOMEMLIMIT_ReaderError_NoOp(t *testing.T) {
	// Clear any ambient GOMEMLIMIT so the operator-override branch does
	// not shadow the reader-error branch.
	t.Setenv("GOMEMLIMIT", "")

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(0, "", errors.New("no cgroup"))

	got := applyGOMEMLIMIT(t.Context(), cfg, nil, reader)
	assert.Equal(t, int64(0), got)
}

// TestApplyGOMEMLIMIT_UnlimitedCgroup_NoOp verifies "no explicit limit"
// (e.g., cgroup "max") is a no-op rather than a zero-value trap.
func TestApplyGOMEMLIMIT_UnlimitedCgroup_NoOp(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	reader := fixedMemoryLimitReader(0, "/sys/fs/cgroup/memory.max", nil)

	got := applyGOMEMLIMIT(t.Context(), cfg, nil, reader)
	assert.Equal(t, int64(0), got)
}

// TestApplyGOMEMLIMIT_SetsHeadroom verifies the 85% headroom math and
// that a concrete value is applied when conditions are met.
//
// Note: debug.SetMemoryLimit is process-global. We restore the previous
// value at teardown so this test does not leak state into parallel
// siblings (also why this test does not call t.Parallel()).
func TestApplyGOMEMLIMIT_SetsHeadroom(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "")

	cfg := &Config{}
	cfg.Fetcher.Enabled = true

	const fourGiB = int64(4) << 30

	reader := fixedMemoryLimitReader(fourGiB, "/sys/fs/cgroup/memory.max", nil)

	got := applyGOMEMLIMIT(t.Context(), cfg, nil, reader)

	// Mirror the production calculation. Forcing the float multiplication
	// through an intermediate variable avoids the constant-expression
	// float->int64 conversion rule.
	headroom := float64(gomemlimitHeadroomPct)
	expected := int64(headroom * float64(fourGiB))
	assert.Equal(t, expected, got, "soft limit must be 85%% of the detected cgroup limit")

	// Restore so we do not affect other tests that inspect runtime
	// memory limits. math.MaxInt64 resets the runtime to "no soft
	// limit", which is the package default; using the int64 max
	// constant directly avoids importing math just for this one value.
	t.Cleanup(func() {
		const noSoftLimit = int64(1<<63 - 1)
		_ = debug.SetMemoryLimit(noSoftLimit)
	})
}

// TestParseCgroupMemoryLimit_Max returns zero bytes for the cgroup v2
// "max" sentinel.
func TestParseCgroupMemoryLimit_Max(t *testing.T) {
	t.Parallel()

	bytes, src, err := parseCgroupMemoryLimit([]byte("max\n"), "memory.max")
	require.NoError(t, err)
	assert.Equal(t, int64(0), bytes)
	assert.Equal(t, "memory.max", src)
}

// TestParseCgroupMemoryLimit_Numeric returns the numeric value intact
// when below the unlimited threshold.
func TestParseCgroupMemoryLimit_Numeric(t *testing.T) {
	t.Parallel()

	bytes, src, err := parseCgroupMemoryLimit([]byte("2147483648"), "memory.max")
	require.NoError(t, err)
	assert.Equal(t, int64(2147483648), bytes)
	assert.Equal(t, "memory.max", src)
}

// TestParseCgroupMemoryLimit_V1Unlimited maps the very large cgroup v1
// sentinel to "no limit" (bytes=0) so callers do not try to enforce
// exabyte-scale budgets.
func TestParseCgroupMemoryLimit_V1Unlimited(t *testing.T) {
	t.Parallel()

	// Classic cgroup v1 "unlimited" value.
	bytes, _, err := parseCgroupMemoryLimit([]byte("9223372036854771712"), "memory.limit_in_bytes")
	require.NoError(t, err)
	assert.Equal(t, int64(0), bytes)
}

// TestParseCgroupMemoryLimit_Garbage returns a wrapped parse error.
func TestParseCgroupMemoryLimit_Garbage(t *testing.T) {
	t.Parallel()

	_, _, err := parseCgroupMemoryLimit([]byte("not-a-number"), "memory.max")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "memory.max")
}

// TestParseCgroupMemoryLimit_EmptyString treats empty input as "max"
// (zero bytes, no error). Defensive behaviour for malformed cgroup files
// on exotic kernels.
func TestParseCgroupMemoryLimit_EmptyString(t *testing.T) {
	t.Parallel()

	bytes, _, err := parseCgroupMemoryLimit([]byte(""), "memory.max")
	require.NoError(t, err)
	assert.Equal(t, int64(0), bytes)
}

