// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

import (
	"context"
	"fmt"
	"time"

	"github.com/gofiber/fiber/v2"
	"golang.org/x/sync/errgroup"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// readyzProbeConcurrency caps how many readyz probes run in parallel inside
// evaluateReadinessChecks. With seven dependencies (postgres, postgres_replica,
// redis, rabbitmq, fetcher, object_storage, streaming) the previous unbounded
// fan-out spawned all seven goroutines at once. Under K8s probe amplification
// (5 probes/sec × N pods) that means 7×N goroutines per probe cycle plus the
// matching number of context.WithTimeout allocations. SetLimit(4) keeps probe
// parallelism bounded and predictable while still completing well inside the
// readyzHandlerWallClockCap budget — even if all seven deps hit the per-check
// timeout, the worst-case wall-clock is ⌈7/4⌉ × per-check-timeout.
const readyzProbeConcurrency = 4

// readyzDepCount is the fixed number of dependencies probed by /readyz:
// postgres, postgres_replica, redis, rabbitmq, fetcher, object_storage,
// streaming. Used
// as the initial capacity hint for the checks map.
const readyzDepCount = 7

// depSpec binds a dep name to its resolver, optional-check, and TLS posture
// helpers. Hoisted to package level so evaluateReadinessChecks does not
// rebuild the slice per /readyz hit.
type depSpec struct {
	name     string
	resolve  func(*Config, *HealthDependencies) (HealthCheckFunc, bool)
	optional func(*Config, *HealthDependencies) bool
	tlsPost  func(*Config) (*bool, string)
}

type readinessProbeOutcome struct {
	index     int
	result    CheckResult
	aggregate bool
}

// readyzSpecs is the canonical dep-order for /readyz. Package-level so
// evaluateReadinessChecks does not allocate per request. Entries capture no
// state that varies per call; deps/cfg flow in via arguments.
var readyzSpecs = []depSpec{ //nolint:gochecknoglobals // immutable package-local constant
	{
		name:     "postgres",
		resolve:  func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolvePostgresCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool { return d != nil && d.PostgresOptional },
		tlsPost:  postgresTLSPosture,
	},
	{
		name:     "postgres_replica",
		resolve:  func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolvePostgresReplicaCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool { return d != nil && d.PostgresReplicaOptional },
		tlsPost:  postgresReplicaTLSPosture,
	},
	{
		name:     "redis",
		resolve:  func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolveRedisCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool { return d != nil && d.RedisOptional },
		tlsPost:  redisTLSPosture,
	},
	{
		name:     "rabbitmq",
		resolve:  func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolveRabbitMQCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool { return d != nil && d.RabbitMQOptional },
		tlsPost:  rabbitMQTLSPosture,
	},
	{
		name:    "fetcher",
		resolve: resolveFetcherCheck,
		optional: func(cfg *Config, d *HealthDependencies) bool {
			if d != nil && d.FetcherOptional {
				return true
			}

			if cfg == nil {
				return false
			}

			return !cfg.Fetcher.Enabled
		},
		tlsPost: fetcherTLSPosture,
	},
	{
		name:     "object_storage",
		resolve:  func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolveObjectStorageCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool { return d != nil && d.ObjectStorageOptional },
		tlsPost:  objectStorageTLSPosture,
	},
	{
		name:    "streaming",
		resolve: func(_ *Config, d *HealthDependencies) (HealthCheckFunc, bool) { return resolveStreamingCheck(d) },
		optional: func(_ *Config, d *HealthDependencies) bool {
			return d == nil || d.StreamingOptional || !d.StreamingEnabled
		},
		tlsPost: func(*Config) (*bool, string) { return nil, "streaming TLS posture is owned by lib-streaming" },
	},
}

// evaluateReadinessChecks probes every dependency once, building a
// map[string]CheckResult. Returns the HTTP status the handler should send
// (200 or 503), the populated checks map, and the boolean "healthy" aggregate.
//
// Concurrency model — bounded errgroup over channel-based fan-out:
//
// Probes run on a golang.org/x/sync/errgroup with SetLimit(readyzProbeConcurrency)
// so at most N (currently 4) goroutines execute simultaneously. The previous
// implementation spawned ALL deps in parallel via runtime.SafeGoWithContextAndComponent
// and consumed results through a buffered channel + select on ctx.Done(). That
// gave us the canceled-context surface we need (so /readyz can return 503
// immediately when the wall-clock cap fires, rather than blocking on a hung
// dep), but unbounded fan-out is wasteful under K8s probe amplification.
// errgroup with SetLimit caps parallelism while still racing every probe to
// completion in the common case — wall-clock is bounded by
// ⌈len(readyzSpecs) / readyzProbeConcurrency⌉ × per-check-timeout.
//
// We keep the buffered-channel + select pattern (rather than a plain
// errgroup.Wait) because Wait would block until every probe finishes, including
// any that never observe their checkCtx cancellation. The select lets the
// handler return partial results immediately on ctx.Done() and synthesise
// "down" entries for any spec we have not yet received an outcome for. This
// preserves the fail-fast contract: kubelet's 1s probe budget is never blocked
// on a hung dependency.
//
// Panic safety: errgroup propagates a recovered panic as ErrPanicRecovered
// from Wait. We don't call Wait — we drain via the channel — so each goroutine
// body is wrapped with runtime.RecoverWithPolicyAndContext (KeepRunning) so a
// panicking probe still emits the matched outcome instead of leaking the
// goroutine and starving the receive loop.
func evaluateReadinessChecks(
	ctx context.Context,
	cfg *Config,
	deps *HealthDependencies,
	logger libLog.Logger,
	timeout time.Duration,
) (int, map[string]CheckResult, bool) {
	results := make([]CheckResult, len(readyzSpecs))
	aggOK := make([]bool, len(readyzSpecs))

	outcomes := make(chan readinessProbeOutcome, len(readyzSpecs))

	group := new(errgroup.Group)
	group.SetLimit(readyzProbeConcurrency)

	// Dispatch probes from a dedicated goroutine so the receive loop below can
	// observe ctx.Done() promptly even when errgroup.SetLimit blocks group.Go.
	// Without this, a cluster of hung probes that ignore checkCtx would stall
	// the spawn loop and prevent the fan-in select from synthesising "down"
	// outcomes within the readyz wall-clock budget. The dispatch goroutine
	// performs a non-blocking ctx check before every group.Go so it exits
	// cleanly on cancellation; remaining specs are then materialised as "down"
	// by the ctx.Done branch in the receive loop.
	go func() {
		defer runtime.RecoverWithPolicyAndContext(
			ctx,
			logger,
			constants.ApplicationName,
			"readyz_dispatch",
			runtime.KeepRunning,
		)

		for idx := range readyzSpecs {
			select {
			case <-ctx.Done():
				return
			default:
			}

			group.Go(func() error {
				defer runtime.RecoverWithPolicyAndContext(
					ctx,
					logger,
					constants.ApplicationName,
					"readyz_probe",
					runtime.KeepRunning,
				)

				check, available := readyzSpecs[idx].resolve(cfg, deps)
				result, aggregate := applyReadinessCheckResult(
					ctx,
					readyzSpecs[idx].name,
					check,
					available,
					readyzSpecs[idx].optional(cfg, deps),
					cfg,
					readyzSpecs[idx].tlsPost,
					logger,
					timeout,
				)

				outcomes <- readinessProbeOutcome{index: idx, result: result, aggregate: aggregate}

				return nil
			})
		}
	}()

	received := make([]bool, len(readyzSpecs))
	pending := len(readyzSpecs)

	for pending > 0 {
		select {
		case outcome := <-outcomes:
			if received[outcome.index] {
				continue
			}

			results[outcome.index] = outcome.result
			aggOK[outcome.index] = outcome.aggregate
			received[outcome.index] = true
			pending--
		case <-ctx.Done():
			for idx, spec := range readyzSpecs {
				if received[idx] {
					continue
				}

				optional := spec.optional(cfg, deps)
				results[idx] = CheckResult{Status: checkStatusDown, Error: categoriseProbeError(ctx.Err())}
				aggOK[idx] = optional

				emitCheckStatus(ctx, spec.name, checkStatusDown)
			}

			pending = 0
		}
	}

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
