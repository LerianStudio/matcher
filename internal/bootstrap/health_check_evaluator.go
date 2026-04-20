// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// readyzDepCount is the fixed number of dependencies probed by /readyz:
// postgres, postgres_replica, redis, rabbitmq, object_storage. Used as the
// initial capacity hint for the checks map.
const readyzDepCount = 5

// depSpec binds a dep name to its resolver, optional-check, and TLS posture
// helpers. Hoisted to package level so evaluateReadinessChecks does not
// rebuild the slice per /readyz hit.
type depSpec struct {
	name     string
	resolve  func(*HealthDependencies) (HealthCheckFunc, bool)
	optional func(*HealthDependencies) bool
	tlsPost  func(*Config) (*bool, string)
}

// readyzSpecs is the canonical dep-order for /readyz. Package-level so
// evaluateReadinessChecks does not allocate per request. Entries capture no
// state that varies per call; deps/cfg flow in via arguments.
var readyzSpecs = []depSpec{ //nolint:gochecknoglobals // immutable package-local constant
	{
		name:     "postgres",
		resolve:  resolvePostgresCheck,
		optional: func(d *HealthDependencies) bool { return d != nil && d.PostgresOptional },
		tlsPost:  postgresTLSPosture,
	},
	{
		name:     "postgres_replica",
		resolve:  resolvePostgresReplicaCheck,
		optional: func(d *HealthDependencies) bool { return d != nil && d.PostgresReplicaOptional },
		tlsPost:  postgresReplicaTLSPosture,
	},
	{
		name:     "redis",
		resolve:  resolveRedisCheck,
		optional: func(d *HealthDependencies) bool { return d != nil && d.RedisOptional },
		tlsPost:  redisTLSPosture,
	},
	{
		name:     "rabbitmq",
		resolve:  resolveRabbitMQCheck,
		optional: func(d *HealthDependencies) bool { return d != nil && d.RabbitMQOptional },
		tlsPost:  rabbitMQTLSPosture,
	},
	{
		name:     "object_storage",
		resolve:  resolveObjectStorageCheck,
		optional: func(d *HealthDependencies) bool { return d != nil && d.ObjectStorageOptional },
		tlsPost:  objectStorageTLSPosture,
	},
}

// evaluateReadinessChecks probes every dependency once, building a
// map[string]CheckResult. Returns the HTTP status the handler should send
// (200 or 503), the populated checks map, and the boolean "healthy" aggregate.
//
// Per-dep probes run in parallel via goroutines with a sync.WaitGroup join.
// Worst-case handler latency is max(per-check timeout) — NOT n_deps × timeout.
// This matters because K8s readinessProbe.periodSeconds=5 would back up
// under the sequential 5 × 5s = 25s worst case on degraded deps.
//
// Panic recovery is provided by SafeGoWithContextAndComponent (via its
// internal RecoverWithPolicyAndContext defer). A panic in a probe is caught
// before wg.Done() releases, so wg.Wait() completes cleanly.
func evaluateReadinessChecks(
	ctx context.Context,
	cfg *Config,
	deps *HealthDependencies,
	logger libLog.Logger,
	timeout time.Duration,
) (int, map[string]CheckResult, bool) {
	results := make([]CheckResult, len(readyzSpecs))
	aggOK := make([]bool, len(readyzSpecs))

	var wg sync.WaitGroup

	for idx, spec := range readyzSpecs {
		wg.Add(1)

		runtime.SafeGoWithContextAndComponent(
			ctx,
			logger,
			constants.ApplicationName,
			"readyz_probe",
			runtime.KeepRunning,
			func(_ context.Context) {
				defer wg.Done()

				check, available := spec.resolve(deps)
				results[idx], aggOK[idx] = applyReadinessCheckResult(
					ctx,
					spec.name,
					check,
					available,
					spec.optional(deps),
					cfg,
					spec.tlsPost,
					logger,
					timeout,
				)
			},
		)
	}

	wg.Wait()

	checks := make(map[string]CheckResult, readyzDepCount)
	healthy := true

	for idx, spec := range readyzSpecs {
		checks[spec.name] = results[idx]
		healthy = healthy && aggOK[idx]
	}

	httpStatus := fiber.StatusOK
	if !healthy {
		httpStatus = fiber.StatusServiceUnavailable
	}

	return httpStatus, checks, healthy
}

// applyReadinessCheckResult runs one probe and builds the CheckResult. The
// returned bool is the aggregation contribution — true when the outcome
// should NOT flip the top-level to unhealthy.
//
// Outcomes:
//
//	optional dep unresolved → CheckResult{Status: skipped, Reason: "...not configured"}, agg=true
//	required dep unresolved → CheckResult{Status: down, Error: "check not configured"}, agg=false
//	check returns nil       → CheckResult{Status: up, LatencyMs, TLS}, agg=true
//	check returns error     → CheckResult{Status: down, LatencyMs, TLS, Error},
//	                          agg=optional (true if the dep is optional, else false)
//
// The response is honest about optional-dep failures: status stays "down" so
// operators see the truth, but the aggregation bit lets K8s keep routing
// traffic. This is the contract's "optional-dep carve-out" — see the
// discussion on the evaluateReadinessChecks doc.
func applyReadinessCheckResult(
	ctx context.Context,
	name string,
	checkFunc HealthCheckFunc,
	available, optional bool,
	cfg *Config,
	tlsPost func(*Config) (*bool, string),
	logger libLog.Logger,
	timeout time.Duration,
) (CheckResult, bool) {
	if !available || checkFunc == nil {
		if optional {
			result := CheckResult{
				Status: checkStatusSkipped,
				Reason: name + " not configured",
			}

			emitCheckStatus(ctx, name, checkStatusSkipped)

			return result, true
		}

		result := CheckResult{
			Status: checkStatusDown,
			Error:  "check not configured",
		}

		emitCheckStatus(ctx, name, checkStatusDown)

		return result, false
	}

	// perCheckTimeoutDefault is the fallback per-check probe timeout when the
	// caller does not pass one. Bounded below the default
	// readyzHandlerWallClockCap so the wall-clock cap does not pre-empt the
	// per-check probe under normal conditions.
	const perCheckTimeoutDefault = 800 * time.Millisecond

	effectiveTimeout := perCheckTimeoutDefault
	if timeout > 0 {
		effectiveTimeout = timeout
	}

	checkCtx, cancel := context.WithTimeout(ctx, effectiveTimeout)
	defer cancel()

	start := time.Now()
	err := checkFunc(checkCtx)
	elapsed := time.Since(start)

	latencyMs := elapsed.Milliseconds()

	tls, tlsReason := tlsPost(cfg)

	result := CheckResult{
		Status:    checkStatusUp,
		LatencyMs: latencyMs,
		TLS:       tls,
	}

	// When TLS posture cannot be determined (nil tls with a non-empty reason),
	// surface that to operators via a bounded, non-sensitive reason string.
	if tls == nil && tlsReason != "" {
		result.Reason = "TLS posture unknown: " + tlsReason
	}

	if err != nil {
		result.Status = checkStatusDown
		result.Error = categoriseProbeError(err)

		if logger != nil {
			logger.Log(ctx, libLog.LevelWarn,
				fmt.Sprintf("readiness check failed: dep=%s latency_ms=%d optional=%t err=%v",
					name, latencyMs, optional, err))
		}

		emitCheckDuration(ctx, name, checkStatusDown, elapsed)
		emitCheckStatus(ctx, name, checkStatusDown)

		return result, optional
	}

	emitCheckDuration(ctx, name, checkStatusUp, elapsed)
	emitCheckStatus(ctx, name, checkStatusUp)

	return result, true
}
