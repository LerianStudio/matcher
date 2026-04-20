// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Startup self-probe. The dev-readyz skill specifies:
//
//   - A package-level atomic boolean selfProbeOK starts false and is set only
//     after a successful probe of every required dependency at startup.
//   - /health (liveness) returns 503 until selfProbeOK is true — this gates
//     K8s traffic immediately rather than relying on /readyz, which is a
//     per-request probe.
//   - A probe failure does NOT abort startup. The caller logs the error and
//     lets K8s livenessProbe restart the pod on sustained /health 503.
//
// The probe reuses the same resolvePostgresCheck / resolveRedisCheck helpers
// as /readyz so both endpoints reflect the same truth.

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	"github.com/LerianStudio/lib-commons/v5/commons/runtime"

	"github.com/LerianStudio/matcher/internal/shared/constants"
)

// selfProbeOK is true once RunSelfProbe has confirmed every required
// dependency. Consumed by livenessHandler to gate /health.
var selfProbeOK atomic.Bool

// selfProbePerCheckTimeout caps each per-dependency probe to bound startup
// latency. Mirrors applyReadinessCheckResult's default of 5s — the standards
// file says "within a timeout window (5 seconds recommended)" for readiness.
const selfProbePerCheckTimeout = 5 * time.Second

// ErrSelfProbeNilDeps indicates RunSelfProbe was called with nil HealthDependencies.
// Bootstrap wires this up — nil indicates a wiring bug, not a runtime condition.
var ErrSelfProbeNilDeps = errors.New("self-probe: nil health dependencies")

// ErrSelfProbeRequiredDepDown indicates at least one required dep failed its
// startup probe. The caller logs and returns; startup continues. K8s will
// restart the pod via /health 503 if the condition persists.
var ErrSelfProbeRequiredDepDown = errors.New("self-probe: required dependency down")

// SelfProbeOK reports whether the startup self-probe has succeeded. Used by
// livenessHandler to gate /health responses. Zero value is false — until
// RunSelfProbe explicitly flips it, the service reports unhealthy.
func SelfProbeOK() bool {
	return selfProbeOK.Load()
}

// selfprobeSpec binds a dep name to its resolver, mirroring depSpec but
// without the TLS posture helper /readyz uses. Hoisted below as a package-
// level slice so RunSelfProbe does not rebuild per startup.
type selfprobeSpec struct {
	name     string
	resolve  func(*HealthDependencies) (HealthCheckFunc, bool)
	optional func(*HealthDependencies) bool
}

// selfprobeSpecs is the canonical dep-order for the startup self-probe.
// Package-level so RunSelfProbe does not allocate per call.
var selfprobeSpecs = []selfprobeSpec{ //nolint:gochecknoglobals // immutable package-local constant
	{name: "postgres", resolve: resolvePostgresCheck, optional: func(d *HealthDependencies) bool { return d.PostgresOptional }},
	{name: "postgres_replica", resolve: resolvePostgresReplicaCheck, optional: func(d *HealthDependencies) bool { return d.PostgresReplicaOptional }},
	{name: "redis", resolve: resolveRedisCheck, optional: func(d *HealthDependencies) bool { return d.RedisOptional }},
	{name: "rabbitmq", resolve: resolveRabbitMQCheck, optional: func(d *HealthDependencies) bool { return d.RabbitMQOptional }},
	{name: "object_storage", resolve: resolveObjectStorageCheck, optional: func(d *HealthDependencies) bool { return d.ObjectStorageOptional }},
}

// RunSelfProbe probes every resolvable dependency exactly once and sets
// selfProbeOK accordingly. Returns nil when every required dep is up. Optional
// deps that are unresolvable are treated as "not configured" (skipped), while
// required deps that are unresolvable are treated as down and fail the probe.
// Returns an error naming offending deps when at least one required dep fails.
//
// The probe ALWAYS runs to completion — one failing dep does not short-circuit
// the others — so the emitted selfprobe_result gauge covers every dep on
// every startup, which is what dashboards expect.
//
// Per-dep probes fan out into goroutines joined by a sync.WaitGroup. Worst-
// case startup probe latency becomes max(selfProbePerCheckTimeout) = 5s, not
// n_deps × 5s. Panic recovery is provided by SafeGoWithContextAndComponent
// (via its internal RecoverWithPolicyAndContext defer). A panic in a probe is
// caught before wg.Done() releases, so wg.Wait() completes cleanly. Results
// aggregate via indexed slices; after Wait() the deterministic spec order is
// walked to build the downRequired list and emit log lines.
//
// Metric emission (selfprobe_result) and per-dep log lines still fire for
// every dep on every startup — ordering between deps is no longer guaranteed
// since goroutines race, but ordering across deps was never an observable
// contract (tests assert on final state, not on log interleaving).
func RunSelfProbe(ctx context.Context, deps *HealthDependencies, logger libLog.Logger) error {
	selfProbeOK.Store(false)

	if logger == nil {
		logger = &libLog.NopLogger{}
	}

	if deps == nil {
		return ErrSelfProbeNilDeps
	}

	specs := selfprobeSpecs

	// Per-spec outcomes, indexed by spec position for deterministic join.
	// probed[i] is false when the dep has no check registered — skipped,
	// not a failure, and excluded from aggregation.
	probed := make([]bool, len(specs))
	upResults := make([]bool, len(specs))

	var wg sync.WaitGroup

	for idx, spec := range specs {
		check, available := spec.resolve(deps)
		optional := spec.optional(deps)

		if !available || check == nil {
			if optional {
				// Optional dep isn't configured (e.g., no replica host). Not a
				// failure — leave unreported for this dep, which dashboards render
				// as "not present".
				continue
			}

			probed[idx] = true
			upResults[idx] = false

			emitSelfProbeResult(ctx, spec.name, false)
			logger.Log(ctx, libLog.LevelWarn,
				fmt.Sprintf("self-probe: %s=down optional=false err=check not configured", spec.name))

			continue
		}

		probed[idx] = true

		wg.Add(1)

		runtime.SafeGoWithContextAndComponent(
			ctx,
			logger,
			constants.ApplicationName,
			"selfprobe",
			runtime.KeepRunning,
			func(_ context.Context) {
				defer wg.Done()

				checkCtx, cancel := context.WithTimeout(ctx, selfProbePerCheckTimeout)
				defer cancel()

				err := check(checkCtx)
				up := err == nil

				upResults[idx] = up
				emitSelfProbeResult(ctx, spec.name, up)

				if up {
					logger.Log(ctx, libLog.LevelInfo, fmt.Sprintf("self-probe: %s=up", spec.name))

					return
				}

				logger.Log(ctx, libLog.LevelWarn,
					fmt.Sprintf("self-probe: %s=down optional=%t err=%v", spec.name, optional, err))
			},
		)
	}

	wg.Wait()

	var downRequired []string

	for idx, spec := range specs {
		if !probed[idx] || upResults[idx] {
			continue
		}

		if !spec.optional(deps) {
			downRequired = append(downRequired, spec.name)
		}
	}

	if len(downRequired) > 0 {
		return fmt.Errorf("%w: %v", ErrSelfProbeRequiredDepDown, downRequired)
	}

	selfProbeOK.Store(true)

	return nil
}
