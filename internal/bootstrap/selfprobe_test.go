// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

//go:build unit

package bootstrap

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
)

// Startup self-probe contract (dev-readyz skill, "Self-Probe Gating"):
//
//   - Package-level atomic flag selfProbeOK starts false.
//   - RunSelfProbe iterates HealthDependencies, measures each dep, emits
//     selfprobe_result metric, and aggregates a healthy/unhealthy verdict
//     where "healthy" = required deps up (optional deps down are ignored
//     for aggregation but still surface).
//   - selfProbeOK flips to true ONLY when RunSelfProbe returns nil.
//   - Returning an error does NOT abort startup — the caller logs and lets
//     K8s liveness probe decide to restart. The test mirrors that contract:
//     RunSelfProbe returns non-nil, selfProbeOK remains false.

// withFreshSelfProbeState resets the package-level flag so tests are not
// order-dependent. The flag is intentionally package-private — tests mutate
// it via the SelfProbeOK()/RunSelfProbe() contract.
func withFreshSelfProbeState(t *testing.T) {
	t.Helper()

	resetSelfProbeStateForTest()

	t.Cleanup(resetSelfProbeStateForTest)
}

//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestSelfProbeOK_StartsFalse(t *testing.T) {
	withFreshSelfProbeState(t)

	assert.False(t, SelfProbeOK(), "self-probe OK must be false before RunSelfProbe completes")
}

//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestRunSelfProbe_NilDeps_Errors(t *testing.T) {
	withFreshSelfProbeState(t)

	err := RunSelfProbe(context.Background(), nil, &libLog.NopLogger{})
	require.Error(t, err)
	assert.False(t, SelfProbeOK(), "nil deps must not flip the ok flag")
}

//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestRunSelfProbe_AllUp_FlipsOKTrue(t *testing.T) {
	withFreshSelfProbeState(t)

	deps := &HealthDependencies{
		PostgresCheck:        func(context.Context) error { return nil },
		PostgresReplicaCheck: func(context.Context) error { return nil },
		RedisCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:        func(context.Context) error { return nil },
		ObjectStorageCheck:   func(context.Context) error { return nil },

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	require.NoError(t, err)
	assert.True(t, SelfProbeOK(), "all required deps up must flip flag true")
}

//nolint:paralleltest,err113 // mutates selfProbeOK; dynamic error is test-local
func TestRunSelfProbe_RequiredDown_FlagStaysFalse(t *testing.T) {
	withFreshSelfProbeState(t)

	depErr := errors.New("pg down")

	deps := &HealthDependencies{
		// Postgres is required (default-non-optional per NewHealthDependencies).
		PostgresCheck:      func(context.Context) error { return depErr },
		RedisCheck:         func(context.Context) error { return nil },
		RabbitMQCheck:      func(context.Context) error { return nil },
		ObjectStorageCheck: func(context.Context) error { return nil },

		RedisOptional:         true,
		ObjectStorageOptional: true,
	}

	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	require.Error(t, err)
	assert.False(t, SelfProbeOK(), "a required-dep failure must keep flag false")
}

//nolint:paralleltest,err113 // mutates selfProbeOK; dynamic error is test-local
func TestRunSelfProbe_OptionalDown_FlagStillTrue(t *testing.T) {
	withFreshSelfProbeState(t)

	optErr := errors.New("replica down")

	deps := &HealthDependencies{
		PostgresCheck:        func(context.Context) error { return nil },
		PostgresReplicaCheck: func(context.Context) error { return optErr },
		RedisCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:        func(context.Context) error { return nil },
		ObjectStorageCheck:   func(context.Context) error { return nil },

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	require.NoError(t, err, "optional dep down must not produce an error")
	assert.True(t, SelfProbeOK(), "optional dep failures must not flip flag false")
}

// TestRunSelfProbe_ProbesRunInParallel asserts that RunSelfProbe fans out
// per-dep probes concurrently. With 5 deps each sleeping 100ms, sequential
// execution would exceed 500ms; parallel execution completes well under 250ms.
// This guards startup latency from scaling linearly with dep count.
//
//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestRunSelfProbe_ProbesRunInParallel(t *testing.T) {
	withFreshSelfProbeState(t)

	const (
		perCheckSleep   = 100 * time.Millisecond
		parallelCeiling = 400 * time.Millisecond
	)

	sleepyCheck := func(ctx context.Context) error {
		select {
		case <-time.After(perCheckSleep):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	deps := &HealthDependencies{
		PostgresCheck:        sleepyCheck,
		PostgresReplicaCheck: sleepyCheck,
		RedisCheck:           sleepyCheck,
		RabbitMQCheck:        sleepyCheck,
		ObjectStorageCheck:   sleepyCheck,

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	started := time.Now()
	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	elapsed := time.Since(started)

	require.NoError(t, err)
	assert.True(t, SelfProbeOK())
	assert.Less(t, elapsed, parallelCeiling,
		"RunSelfProbe must fan out: expected <%s, got %s (sequential would be ~%s)",
		parallelCeiling, elapsed, 5*perCheckSleep)
}

//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestRunSelfProbe_DepWithoutCheckFunc_Skipped(t *testing.T) {
	withFreshSelfProbeState(t)

	deps := &HealthDependencies{
		PostgresCheck: func(context.Context) error { return nil },
		// Redis/RabbitMQ/ObjectStorage/Replica left nil. resolver returns not-available.
		RedisOptional:           true,
		RabbitMQOptional:        true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}

	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	require.NoError(t, err)
	assert.True(t, SelfProbeOK())
}

//nolint:paralleltest // mutates package-level selfProbeOK flag; MUST run serially
func TestRunSelfProbe_RequiredDepWithoutCheckFunc_Fails(t *testing.T) {
	withFreshSelfProbeState(t)

	deps := &HealthDependencies{
		// Required Postgres dependency left unresolved.
		RedisOptional:           true,
		RabbitMQOptional:        true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}

	err := RunSelfProbe(context.Background(), deps, &libLog.NopLogger{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrSelfProbeRequiredDepDown)
	assert.False(t, SelfProbeOK())
}

// TestRunSelfProbe_EmitsMetricForEachDep verifies that RunSelfProbe emits a
// selfprobe_result data point for every resolvable dep. Wires a ManualReader
// against the global OTel provider, runs a successful self-probe with all 5
// deps registered, collects, and asserts that the gauge carries a dep-labelled
// point for each.
//
//nolint:paralleltest // mutates package-level selfProbeOK flag + global OTel meter provider; MUST run serially
func TestRunSelfProbe_EmitsMetricForEachDep(t *testing.T) {
	withFreshSelfProbeState(t)

	reader := setupManualReader(t)

	deps := &HealthDependencies{
		PostgresCheck:        func(context.Context) error { return nil },
		PostgresReplicaCheck: func(context.Context) error { return nil },
		RedisCheck:           func(context.Context) error { return nil },
		RabbitMQCheck:        func(context.Context) error { return nil },
		ObjectStorageCheck:   func(context.Context) error { return nil },

		PostgresReplicaOptional: true,
		RedisOptional:           true,
		ObjectStorageOptional:   true,
	}

	require.NoError(t, RunSelfProbe(context.Background(), deps, &libLog.NopLogger{}))

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := findMetric(rm, "selfprobe_result")
	require.NotNil(t, found, "selfprobe_result must be registered and collected")

	seen := map[string]bool{}

	switch d := found.Data.(type) {
	case metricdata.Gauge[int64]:
		for _, p := range d.DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				if string(kv.Key) == "dep" {
					seen[kv.Value.AsString()] = true
				}
			}
		}
	case metricdata.Sum[int64]:
		for _, p := range d.DataPoints {
			for _, kv := range p.Attributes.ToSlice() {
				if string(kv.Key) == "dep" {
					seen[kv.Value.AsString()] = true
				}
			}
		}
	default:
		t.Fatalf("selfprobe_result must be gauge-shaped; got %T", found.Data)
	}

	for _, dep := range []string{"postgres", "postgres_replica", "redis", "rabbitmq", "object_storage"} {
		assert.True(t, seen[dep], "selfprobe_result must carry a datapoint for dep=%s", dep)
	}
}

//nolint:paralleltest,err113 // mutates selfProbeOK; dynamic error is test-local
func TestRunSelfProbe_ResetsSelfProbeFlagBeforeEachRun(t *testing.T) {
	withFreshSelfProbeState(t)

	depsHealthy := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return nil },
		RedisCheck:              func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	require.NoError(t, RunSelfProbe(context.Background(), depsHealthy, &libLog.NopLogger{}))
	assert.True(t, SelfProbeOK())

	depsUnhealthy := &HealthDependencies{
		PostgresCheck:           func(context.Context) error { return errors.New("postgres down") },
		RedisCheck:              func(context.Context) error { return nil },
		RabbitMQCheck:           func(context.Context) error { return nil },
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
	require.Error(t, RunSelfProbe(context.Background(), depsUnhealthy, &libLog.NopLogger{}))
	assert.False(t, SelfProbeOK(), "failed probe must reset stale success flag")
}
