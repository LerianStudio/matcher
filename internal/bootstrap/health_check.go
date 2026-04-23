// Copyright 2025 Lerian Studio. All rights reserved.
// Use of this source code is governed by an Elastic License 2.0
// that can be found in the LICENSE.md file.

package bootstrap

// Readiness and liveness handlers for Kubernetes probes. Conforms to the
// canonical /readyz contract defined in the dev-readyz skill:
//
//	GET /readyz  → JSON {status, checks, version, deployment_mode}
//	GET /health  → plain "healthy" (gated by startup self-probe)
//	GET /version → JSON {version, requestDate} (lib-commons Version handler)
//
// The five-value per-check status vocabulary — "up", "down", "degraded",
// "skipped", "n/a" — is part of the contract, as is the aggregation rule
// (top-level "healthy" iff every check is in {up, skipped, n/a}). Optional
// deps are a documented carve-out: their failures remain visible in the
// response as "down" with an error field so operators see the truth, but
// do not flip the top-level aggregation (K8s still routes traffic).
//
// Design notes called out by the dev-readyz skill:
//
//   - No breaker_state field. Matcher's infra deps (PG/Redis/RabbitMQ/S3) are
//     NOT wrapped in circuit breakers. The only breaker is lib-commons
//     tenant-manager's HTTP client (Tenant Manager API), which isn't probed
//     by /readyz. Gate 6 is a soft-skip.
//
//   - No "n/a" status / no /readyz/tenant/:id. Matcher uses shared-cluster
//     multi-tenancy; all infra is one cluster with per-tenant schema/vhost/
//     key-prefix isolation. Global /readyz is sufficient.
//
//   - No response caching. K8s must see transient failures immediately.
//
//   - No /ready alias. The previous /ready path was renamed to /readyz to
//     match the canonical contract. /health/live and /health/ready splits
//     are also forbidden — keep single /health + /readyz.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"

	libCommons "github.com/LerianStudio/lib-commons/v5/commons"
	libLog "github.com/LerianStudio/lib-commons/v5/commons/log"
	sharedhttp "github.com/LerianStudio/lib-commons/v5/commons/net/http"
	libPostgres "github.com/LerianStudio/lib-commons/v5/commons/postgres"
	libRabbitmq "github.com/LerianStudio/lib-commons/v5/commons/rabbitmq"
	libRedis "github.com/LerianStudio/lib-commons/v5/commons/redis"
)

// HealthCheckFunc is a function type for performing health checks on dependencies.
type HealthCheckFunc func(ctx context.Context) error

// ReadinessResponse is the canonical /readyz response envelope.
//
// @Description Kubernetes readiness probe response in the canonical contract
// @Description shape (dev-readyz skill). Top-level "status" is "healthy" iff
// @Description every check is in {up, skipped, n/a}; any "down" or "degraded"
// @Description in a required dep yields "unhealthy" and HTTP 503.
type ReadinessResponse struct {
	Status         string                 `json:"status" example:"healthy" enums:"healthy,unhealthy"`
	Checks         map[string]CheckResult `json:"checks"`
	Version        string                 `json:"version" example:"1.3.0"`
	DeploymentMode string                 `json:"deployment_mode" example:"local" enums:"saas,byoc,local"`
}

// CheckResult describes the outcome of one /readyz per-dependency probe.
// Field presence follows the dev-readyz contract:
//
//	status      — always
//	latency_ms  — on up/degraded/down when a probe ran (ms)
//	tls         — when dep supports TLS (posture, NOT cert validity)
//	error       — on down/degraded (opaque category; raw detail stays in server logs)
//	reason      — on skipped/n/a, or when TLS posture cannot be determined
//
// Status enum: the full canonical vocabulary is {up, down, degraded, skipped,
// n/a}. Only `up`, `down`, `skipped` are emitted by matcher today — there is
// no latency-threshold degraded logic and no per-tenant "n/a" surface. The
// remaining two values stay in the contract so future work (circuit breakers,
// per-tenant endpoints) can emit them without a Swagger contract break.
type CheckResult struct {
	Status    string `json:"status" example:"up" enums:"up,down,degraded,skipped,n/a"`
	LatencyMs int64  `json:"latency_ms,omitempty" example:"2"`
	// TLS is a pointer so json omitempty drops it for deps that have no TLS
	// concept (e.g., in-process cache). For deps that do have TLS posture,
	// false and true are both meaningful and must serialise.
	TLS    *bool  `json:"tls,omitempty"`
	Error  string `json:"error,omitempty"`
	Reason string `json:"reason,omitempty"`
}

// Canonical top-level status values.
const (
	statusHealthy   = "healthy"
	statusUnhealthy = "unhealthy"
)

// Canonical per-check status vocabulary — dev-readyz skill.
//
// The full contract enum is {up, down, degraded, skipped, n/a} — surfaced in
// the Swagger enum annotation on CheckResult.Status. Matcher only emits the
// three values below today (no degraded-latency logic, no n/a per-tenant
// surface). `degraded` and `n/a` intentionally remain in the JSON schema for
// future use; callers reading /readyz responses MUST handle all five values.
const (
	checkStatusUp      = "up"
	checkStatusDown    = "down"
	checkStatusSkipped = "skipped"
)

// HealthDependencies holds references to infrastructure components for health checks.
type HealthDependencies struct {
	Postgres        *libPostgres.Client
	PostgresReplica *libPostgres.Client
	Redis           *libRedis.Client
	RabbitMQ        *libRabbitmq.RabbitMQConnection
	ObjectStorage   ObjectStorageHealthChecker

	PostgresCheck        HealthCheckFunc
	PostgresReplicaCheck HealthCheckFunc
	RedisCheck           HealthCheckFunc
	RabbitMQCheck        HealthCheckFunc
	ObjectStorageCheck   HealthCheckFunc

	// Optional dependencies do not impact overall readiness status when unavailable
	// or failing their readiness checks (the "optional-dep down" carve-out).
	PostgresOptional        bool
	PostgresReplicaOptional bool
	RedisOptional           bool
	RabbitMQOptional        bool
	ObjectStorageOptional   bool
}

// ObjectStorageHealthChecker is an interface for checking object storage health.
type ObjectStorageHealthChecker interface {
	// Exists checks if an object exists at the given key (used for health check).
	Exists(ctx context.Context, key string) (bool, error)
}

// NewHealthDependencies creates a new HealthDependencies with default settings.
func NewHealthDependencies(
	postgres *libPostgres.Client,
	postgresReplica *libPostgres.Client,
	redis *libRedis.Client,
	rabbitmq *libRabbitmq.RabbitMQConnection,
	objectStorage ObjectStorageHealthChecker,
) *HealthDependencies {
	return &HealthDependencies{
		Postgres:        postgres,
		PostgresReplica: postgresReplica,
		Redis:           redis,
		RabbitMQ:        rabbitmq,
		ObjectStorage:   objectStorage,

		// Redis, replica, and object storage are treated as optional dependencies
		// by default.
		RedisOptional:           true,
		PostgresReplicaOptional: true,
		ObjectStorageOptional:   true,
	}
}

// livenessHandler is the Kubernetes liveness probe handler. It returns 503
// until the startup self-probe has confirmed every required dependency; once
// the service has bootstrapped, it delegates to the lib-commons Ping handler.
//
// @Summary      Liveness check
// @Description  Returns HTTP 200 with "healthy" once the startup self-probe
// @Description  has confirmed every required dependency. Returns 503 before
// @Description  that gate is cleared so Kubernetes does not route traffic
// @Description  to a partially-initialised pod. Separate from /readyz, which
// @Description  probes on every request.
// @Tags         Health
// @Produce      plain
// @Success      200  {string}  string  "healthy"
// @Failure      503  {string}  string  "self-probe failed"
// @Router       /health [get]
// @ID           getHealth
func livenessHandler(c *fiber.Ctx) error {
	if !SelfProbeOK() {
		return c.Status(fiber.StatusServiceUnavailable).SendString("self-probe failed")
	}

	if err := sharedhttp.Ping(c); err != nil {
		return fmt.Errorf("liveness handler: ping: %w", err)
	}

	return nil
}

// versionHandler returns the service version from the VERSION environment variable.
//
// @Summary      Service version
// @Description  Returns the service version from the VERSION environment
// @Description  variable (defaults to "0.0.0").
// @Tags         Health
// @Produce      json
// @Success      200  {object}  map[string]interface{}  "version and requestDate"
// @Router       /version [get]
// @ID           getVersion
func versionHandler(c *fiber.Ctx) error {
	if err := sharedhttp.Version(c); err != nil {
		return fmt.Errorf("version handler: %w", err)
	}

	return nil
}

// readinessHandler creates a handler that responds to readiness probe requests
// in the canonical dev-readyz contract. MUST be mounted at /readyz (no aliases)
// and BEFORE auth middleware — K8s probes are unauthenticated.
//
// No span/tracer: /readyz is K8s-probed every ~5s and mounted pre-auth; adding
// a span per request would dominate trace volume without value.
//
// Aggregation rule implemented here:
//
//   - every dep is probed on every request; a 250ms-TTL in-memory cache
//     dampens amplification (drain short-circuit bypasses the cache)
//   - each dep produces a CheckResult with structured status
//   - top-level is "healthy" when every required dep is up/skipped/n/a
//   - optional dep failures appear as "down" in the response (honest) but
//     do not flip the top-level (K8s still routes traffic)
//   - when draining (SIGTERM received), short-circuit with 503 + empty
//     checks map to drain in-flight requests before shutdown
//
// Handler wall-clock cap: the entire fan-out is wrapped in a
// readyzHandlerWallClockCap-bounded context so kubelet's default
// timeoutSeconds=1 budget is preserved even if a probe exceeds its per-check
// timeout due to scheduling. On cap hit, partial results still surface.
//
// @Summary      Readiness check
// @Description  Canonical Kubernetes readiness probe. Per-check status
// @Description  vocabulary: up, down, degraded, skipped, n/a. Top-level
// @Description  "healthy" iff every required dep is in {up, skipped, n/a};
// @Description  anything else yields "unhealthy" and HTTP 503.
// @Tags         Health
// @Produce      json
// @Success      200  {object}  ReadinessResponse  "Service is ready"
// @Failure      503  {object}  ReadinessResponse  "Service is not ready"
// @Router       /readyz [get]
// @ID           getReadyz
func readinessHandler(
	initialCfg *Config,
	configGetter func() *Config,
	drainingGetter func() bool,
	deps *HealthDependencies,
	logger libLog.Logger,
) fiber.Handler {
	cache := &readyzResultCache{}

	return func(fiberCtx *fiber.Ctx) error {
		ctx := fiberCtx.UserContext()
		if ctx == nil {
			ctx = context.Background()
		}

		cfg := initialCfg

		if configGetter != nil {
			if runtimeCfg := configGetter(); runtimeCfg != nil {
				cfg = runtimeCfg
			}
		}

		// Drain short-circuit MUST bypass the cache so SIGTERM flips /readyz
		// 503 immediately.
		if drainingGetter != nil && drainingGetter() {
			return sharedhttp.Respond(fiberCtx, fiber.StatusServiceUnavailable, ReadinessResponse{
				Status:         statusUnhealthy,
				Checks:         map[string]CheckResult{},
				Version:        resolveReadinessVersion(),
				DeploymentMode: resolveDeploymentMode(cfg),
			})
		}

		// 250ms TTL cache keeps K8s probe amplification (5 probes × N pods at
		// 5s intervals) from hammering backend pools when nothing is changing.
		// The cache is per-handler so mounting a new handler (e.g. in tests)
		// always starts clean.
		if cached, status, ok := cache.lookup(); ok {
			return sharedhttp.Respond(fiberCtx, status, cached)
		}

		// Derive per-check timeout from the effective runtime config; fall
		// back to applyReadinessCheckResult's default on zero/non-positive.
		var healthCheckTimeout time.Duration
		if cfg != nil {
			healthCheckTimeout = cfg.Infrastructure.HealthCheckTimeout()
		}

		// Handler wall-clock cap: preserve kubelet's default 1s probe budget
		// even if a probe blows past its own deadline due to scheduling. On
		// cap hit, partial results still surface via evaluateReadinessChecks.
		capCtx, capCancel := context.WithTimeout(ctx, readyzHandlerWallClockCap)
		defer capCancel()

		httpStatus, checks, healthy := evaluateReadinessChecks(capCtx, cfg, deps, logger, healthCheckTimeout)

		topStatus := statusHealthy
		if !healthy {
			topStatus = statusUnhealthy
		}

		response := ReadinessResponse{
			Status:         topStatus,
			Checks:         checks,
			Version:        resolveReadinessVersion(),
			DeploymentMode: resolveDeploymentMode(cfg),
		}

		cache.store(response, httpStatus)

		return sharedhttp.Respond(fiberCtx, httpStatus, response)
	}
}

// readyzHandlerWallClockCap is the maximum time readinessHandler spends in
// evaluateReadinessChecks. Bounded below kubelet's default probe
// timeoutSeconds=1 so a readinessProbe with timeoutSeconds=2 (our recommended
// doc value) always has ≥100ms of network headroom.
const readyzHandlerWallClockCap = 900 * time.Millisecond

// readyzCacheTTL bounds how long a /readyz result can be served from the
// per-handler cache. 250ms keeps amplification down without hiding transient
// failures for longer than a single probe cycle.
const readyzCacheTTL = 250 * time.Millisecond

// readyzResultCache is a tiny per-handler 250ms TTL cache around the rendered
// ReadinessResponse + HTTP status. Each readinessHandler closure owns one
// instance — no global state — so unit tests that mount their own handler get
// a clean cache automatically.
type readyzResultCache struct {
	mu         sync.RWMutex
	result     *ReadinessResponse
	httpStatus int
	expiresAt  time.Time
}

func (cache *readyzResultCache) lookup() (ReadinessResponse, int, bool) {
	cache.mu.RLock()
	defer cache.mu.RUnlock()

	if cache.result == nil || !time.Now().Before(cache.expiresAt) {
		return ReadinessResponse{}, 0, false
	}

	return *cache.result, cache.httpStatus, true
}

func (cache *readyzResultCache) store(response ReadinessResponse, httpStatus int) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	cache.result = &response
	cache.httpStatus = httpStatus
	cache.expiresAt = time.Now().Add(readyzCacheTTL)
}

// resolveReadinessVersion mirrors lib-commons sharedhttp.Version behaviour:
// return the VERSION env var, falling back to "0.0.0" when absent.
func resolveReadinessVersion() string {
	v := strings.TrimSpace(libCommons.GetenvOrDefault("VERSION", "0.0.0"))
	if v == "" {
		return "0.0.0"
	}

	return v
}

// resolveDeploymentMode returns the effective deployment mode string for the
// response envelope. Empty cfg → "local".
func resolveDeploymentMode(cfg *Config) string {
	if cfg == nil {
		return deploymentModeLocal
	}

	return cfg.App.DeploymentMode()
}
